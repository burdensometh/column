// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"unsafe"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
	"github.com/kelindar/iostream"
	"github.com/klauspost/compress/s2"
)

var (
	errUnexpectedEOF = errors.New("column: unable to restore, unexpected EOF")
)

// snapshotVersion is the version of the snapshot format. Version 0x2 adds the
// slot generation vector used by versioned row handles.
const snapshotVersion = 0x2

// --------------------------- Commit Replay ---------------------------

// Replay replays a commit on a collection, applying the changes.
func (c *Collection) Replay(change commit.Commit) error {
	return c.Query(func(txn *Txn) error {
		txn.dirty.Set(uint32(change.Chunk))
		for i := range change.Updates {
			if !change.Updates[i].IsEmpty() {
				txn.updates = append(txn.updates, change.Updates[i])
			}
		}
		return nil
	})
}

// --------------------------- Snapshotting ---------------------------

// Restore restores the collection from the underlying snapshot reader. This operation
// should be called before any of transactions, right after initialization.
func (c *Collection) Restore(snapshot io.Reader) error {
	commits, err := c.readState(s2.NewReader(snapshot))
	if err != nil {
		return err
	}

	// Reconcile the pending commit log
	return commit.Open(snapshot).Range(func(commit commit.Commit) error {
		lastCommit := commits[commit.Chunk]
		if commit.ID > lastCommit {
			return c.Replay(commit)
		}
		return nil
	})
}

// Snapshot writes a collection snapshot into the underlying writer.
func (c *Collection) Snapshot(dst io.Writer) error {
	recorder, err := c.recorderOpen()
	if err != nil {
		return err
	}

	// Take a snapshot of the current state
	defer os.Remove(recorder.Name())
	if _, err := c.writeState(s2.NewWriter(dst)); err != nil {
		return err
	}

	// Close the recorder
	c.recorderClose()
	return recorder.Copy(dst)
}

// recorderOpen opens a recorder for commits while the snapshot is in progress
func (c *Collection) recorderOpen() (log *commit.Log, err error) {
	if log, err = commit.OpenTemp(); err == nil {
		dst := (*unsafe.Pointer)(unsafe.Pointer(&c.record))
		ptr := unsafe.Pointer(log)
		if !atomic.CompareAndSwapPointer(dst, nil, ptr) {
			return nil, fmt.Errorf("column: unable to snapshot, another one might be in progress")
		}
	}
	return
}

// recorderClose closes the pending commit recorder and deletes the file
func (c *Collection) recorderClose() {
	if _, ok := c.isSnapshotting(); ok {
		dst := (*unsafe.Pointer)(unsafe.Pointer(&c.record))
		atomic.StorePointer(dst, nil)
	}
}

// isSnapshotting loads a currently used commit log for a pending snapshot
func (c *Collection) isSnapshotting() (*commit.Log, bool) {
	dst := (*unsafe.Pointer)(unsafe.Pointer(&c.record))
	ptr := atomic.LoadPointer(dst)
	if ptr == nil {
		return nil, false
	}

	return (*commit.Log)(ptr), true
}

// --------------------------- Collection Encoding ---------------------------

// writeState writes collection state into the specified writer.
func (c *Collection) writeState(dst io.Writer) (int64, error) {
	writer := iostream.NewWriter(dst)
	buffer := c.txns.acquirePage(rowColumn)
	defer c.txns.releasePage(buffer)

	// Write the schema version
	if err := writer.WriteUvarint(snapshotVersion); err != nil {
		return writer.Offset(), err
	}

	// Load the number of columns and the max index
	chunks := c.snapshotChunks()
	columns := uint64(c.cols.Count()) + 1 // extra 'insert' column

	// Write the number of columns
	if err := writer.WriteUvarint(columns); err != nil {
		return writer.Offset(), err
	}

	// Write each chunk
	if err := writer.WriteRange(chunks, func(i int, w *iostream.Writer) error {
		return c.readChunk(commit.Chunk(i), func(lastCommit uint64, chunk commit.Chunk, fill bitmap.Bitmap) error {
			offset := chunk.Min()

			// Write the last written commit for this chunk
			if err := writer.WriteUvarint(lastCommit); err != nil {
				return err
			}

			// Write the inserts column
			buffer.Reset(rowColumn)
			fill.Range(func(idx uint32) {
				buffer.PutOperation(commit.Insert, offset+idx)
			})
			if err := writer.WriteSelf(buffer); err != nil {
				return err
			}

			// Snapshot each column and write the buffer
			if err := c.cols.RangeUntil(func(column *column) error {
				if !column.Snapshot(chunk, buffer) {
					return nil // Skip indexes
				}
				return writer.WriteSelf(buffer)
			}); err != nil {
				return err
			}

			// Write the slot generations for this chunk so that versioned
			// handles keep matching the same rows after a restore.
			return c.writeGensLocked(chunk, writer)
		})
	}); err != nil {
		return writer.Offset(), err
	}

	return writer.Offset(), writer.Flush()
}

// writeGensLocked writes the generation vector of a single chunk. The caller
// must hold the collection lock (see readChunk). Slots whose generation is
// still the initial one carry no information and are trimmed off, since a
// slot that was never occupied and a slot that was never deleted both
// correctly read as the initial generation.
func (c *Collection) writeGensLocked(chunk commit.Chunk, writer *iostream.Writer) error {
	offset := int(chunk.Min())
	end := offset + chunkSize
	if end > len(c.gens) {
		end = len(c.gens)
	}

	// Trim the trailing slots which are still at the initial generation
	count := 0
	if end > offset {
		if count = end - offset; count > 0 {
			for count > 0 && c.gens[offset+count-1] == initialGeneration {
				count--
			}
		}
	}

	// Write the number of generations followed by the generations themselves
	if err := writer.WriteUvarint(uint64(count)); err != nil {
		return err
	}
	for _, generation := range c.gens[offset : offset+count] {
		if err := writer.WriteUint32(generation); err != nil {
			return err
		}
	}
	return nil
}

// readState reads a collection snapshotted state from the underlying reader. It
// returns the last commit IDs for each chunk.
func (c *Collection) readState(src io.Reader) (map[commit.Chunk]uint64, error) {
	r := iostream.NewReader(src)
	commits := make(map[commit.Chunk]uint64)

	// Read the version and make sure it matches
	version, err := r.ReadUvarint()
	if err != nil || version < 0x1 || version > snapshotVersion {
		return nil, fmt.Errorf("column: unable to restore (version %d) %v", version, err)
	}

	// Read the number of columns
	columns, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}

	// Read each chunk
	return commits, r.ReadRange(func(chunk int, r *iostream.Reader) error {
		return c.Query(func(txn *Txn) error {
			txn.dirty.Set(uint32(chunk))

			// Read the last written commit ID for the chunk
			if commits[commit.Chunk(chunk)], err = r.ReadUvarint(); err != nil {
				return err
			}

			for i := uint64(0); i < columns; i++ {
				buffer := txn.owner.txns.acquirePage("")
				_, err := buffer.ReadFrom(r)
				switch {
				case err == io.EOF && i < columns:
					return errUnexpectedEOF
				case err != nil:
					return err
				default:
					txn.updates = append(txn.updates, buffer)
				}
			}

			// Read the slot generations for the chunk (version 0x2 and above)
			if version >= 0x2 {
				return c.readGens(commit.Chunk(chunk), r)
			}
			return nil
		})
	})
}

// readGens reads the generation vector of a single chunk and installs it into
// the collection, so that restored rows keep matching their versioned handles.
func (c *Collection) readGens(chunk commit.Chunk, r *iostream.Reader) error {
	count, err := r.ReadUvarint()
	if err != nil {
		return err
	}
	if count > chunkSize {
		return fmt.Errorf("column: unable to restore, invalid generation count %d", count)
	}

	// Read the generations of the chunk
	gens := make([]uint32, count)
	for i := range gens {
		if gens[i], err = r.ReadUint32(); err != nil {
			return err
		}
	}

	// Install the generations, growing the vector if necessary
	c.lock.Lock()
	offset := int(chunk.Min())
	for len(c.gens) < offset+len(gens) {
		c.gens = append(c.gens, initialGeneration)
	}
	copy(c.gens[offset:], gens)
	c.lock.Unlock()
	return nil
}

// chunks returns the number of chunks and columns
func (c *Collection) chunks() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	if len(c.fill) == 0 {
		return 0
	}

	max, _ := c.fill.Max()
	return int(commit.ChunkAt(max) + 1)
}

// snapshotChunks returns the number of chunks to write during a snapshot. It
// covers both the fill-list and the generation vector, since generations of
// deleted rows must be retained to keep invalidating their stale handles.
func (c *Collection) snapshotChunks() int {
	c.lock.Lock()
	defer c.lock.Unlock()

	chunks := 0
	if len(c.fill) > 0 {
		if max, ok := c.fill.Max(); ok {
			chunks = int(commit.ChunkAt(max) + 1)
		}
	}

	if tail := (c.gensTailLocked() + chunkSize - 1) / chunkSize; tail > chunks {
		chunks = tail
	}
	return chunks
}

// gensTailLocked returns the length of the meaningful part of the generation
// vector, trimming off the trailing slots that were never occupied. The
// caller must hold the collection lock.
func (c *Collection) gensTailLocked() (tail int) {
	for tail = len(c.gens); tail > 0 && c.gens[tail-1] == initialGeneration; tail-- {
	}
	return tail
}

// readChunk acquires appropriate locks for a chunk and executes a read callback.
// This is used for snapshotting purposes only.
func (c *Collection) readChunk(chunk commit.Chunk, fn func(uint64, commit.Chunk, bitmap.Bitmap) error) error {

	// Lock both the chunk and the fill list
	c.slock.RLock(uint(chunk))
	c.lock.Lock()
	defer c.slock.RUnlock(uint(chunk))
	defer c.lock.Unlock()

	// The generation vector can extend beyond the committed chunks
	var lastCommit uint64
	if int(chunk) < len(c.commits) {
		lastCommit = c.commits[chunk]
	}
	return fn(lastCommit, chunk, chunk.OfBitmap(c.fill))
}
