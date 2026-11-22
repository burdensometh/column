// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
	"github.com/kelindar/iostream"
	"github.com/klauspost/compress/s2"
	"github.com/stretchr/testify/assert"
)

// TestHandleInsertQuery tests that a committed insert publishes a versioned
// handle which can be used to query the row afterwards.
func TestHandleInsertQuery(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("name", ForString())

	// Insert a row and get its handle
	handle, err := c.InsertHandle(func(r Row) error {
		r.SetString("name", "merlin")
		return nil
	})
	assert.NoError(t, err)
	assert.NotZero(t, handle)
	assert.Equal(t, uint32(0), handle.Index())
	assert.Equal(t, uint32(1), handle.Generation())

	// The handle should resolve to the row
	assert.NoError(t, c.QueryHandle(handle, func(r Row) error {
		name, ok := r.String("name")
		assert.True(t, ok)
		assert.Equal(t, "merlin", name)
		return nil
	}))

	// The numeric index interface should still work on the same row
	assert.NoError(t, c.QueryAt(handle.Index(), func(r Row) error {
		name, _ := r.String("name")
		assert.Equal(t, "merlin", name)
		return nil
	}))
}

// TestHandleUpdate tests that a row can be corrected through its handle.
func TestHandleUpdate(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("name", ForString())

	handle, err := c.InsertHandle(func(r Row) error {
		r.SetString("name", "merlin")
		return nil
	})
	assert.NoError(t, err)

	// Update the row through its handle
	assert.NoError(t, c.QueryHandle(handle, func(r Row) error {
		r.SetString("name", "gandalf")
		return nil
	}))

	// The update should be visible through both the handle and the index
	assert.NoError(t, c.QueryHandle(handle, func(r Row) error {
		name, _ := r.String("name")
		assert.Equal(t, "gandalf", name)
		return nil
	}))
	assert.NoError(t, c.QueryAt(handle.Index(), func(r Row) error {
		name, _ := r.String("name")
		assert.Equal(t, "gandalf", name)
		return nil
	}))
}

// TestHandleDeleteInvalidates tests that deleting a row invalidates its handle,
// and that the handle never matches a different row re-using the same slot.
func TestHandleDeleteInvalidates(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("name", ForString())

	// Insert and delete a row
	handle1, err := c.InsertHandle(func(r Row) error {
		r.SetString("name", "merlin")
		return nil
	})
	assert.NoError(t, err)
	assert.True(t, c.DeleteAt(handle1.Index()))

	// The old handle must be invalid now
	assert.Equal(t, ErrInvalidHandle, c.QueryHandle(handle1, func(r Row) error {
		return fmt.Errorf("should not be called")
	}))
	assert.False(t, c.DeleteHandle(handle1))

	// Insert a new row, it should re-use the slot but with a new generation
	handle2, err := c.InsertHandle(func(r Row) error {
		r.SetString("name", "gandalf")
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, handle1.Index(), handle2.Index())
	assert.NotEqual(t, handle1, handle2)
	assert.Equal(t, handle1.Generation()+1, handle2.Generation())

	// The stale handle must not resolve to the new row
	assert.Equal(t, ErrInvalidHandle, c.QueryHandle(handle1, func(r Row) error {
		return fmt.Errorf("should not be called")
	}))
	assert.False(t, c.DeleteHandle(handle1))

	// The new handle resolves to the new row
	assert.NoError(t, c.QueryHandle(handle2, func(r Row) error {
		name, _ := r.String("name")
		assert.Equal(t, "gandalf", name)
		return nil
	}))
}

// TestHandleDeleteHandle tests deletion through a versioned handle.
func TestHandleDeleteHandle(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("name", ForString())

	handle, err := c.InsertHandle(func(r Row) error {
		r.SetString("name", "merlin")
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, c.Count())

	// Delete through the handle, it should only work once
	assert.True(t, c.DeleteHandle(handle))
	assert.Equal(t, 0, c.Count())
	assert.False(t, c.DeleteHandle(handle))
	assert.Equal(t, ErrInvalidHandle, c.QueryHandle(handle, func(r Row) error {
		return nil
	}))
}

// TestHandleRollback tests that a rolled-back transaction does not publish
// handles and that the reserved slot is reclaimed.
func TestHandleRollback(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("name", ForString())

	// Commit one row first
	committed, err := c.InsertHandle(func(r Row) error {
		r.SetString("name", "merlin")
		return nil
	})
	assert.NoError(t, err)

	// Insert inside a transaction that gets rolled back
	var rolled Handle
	assert.Error(t, c.Query(func(txn *Txn) error {
		var err error
		rolled, err = txn.InsertHandle(func(r Row) error {
			r.SetString("name", "gandalf")
			return nil
		})
		assert.NoError(t, err)
		assert.NotZero(t, rolled)
		return fmt.Errorf("trigger rollback")
	}))

	// The slot must have been reclaimed and no handle published
	assert.Equal(t, 1, c.Count())
	assert.Equal(t, ErrInvalidHandle, c.QueryHandle(rolled, func(r Row) error {
		return fmt.Errorf("should not be called")
	}))
	assert.False(t, c.DeleteHandle(rolled))

	// The next insert should re-use the rolled-back slot with a new generation
	next, err := c.InsertHandle(func(r Row) error {
		r.SetString("name", "radagast")
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, rolled.Index(), next.Index())
	assert.NotEqual(t, rolled, next)
	assert.Equal(t, 2, c.Count())

	// The rolled-back handle must not resolve to the new row
	assert.Equal(t, ErrInvalidHandle, c.QueryHandle(rolled, func(r Row) error {
		return fmt.Errorf("should not be called")
	}))

	// The previously committed handle is unaffected
	assert.NoError(t, c.QueryHandle(committed, func(r Row) error {
		name, _ := r.String("name")
		assert.Equal(t, "merlin", name)
		return nil
	}))
}

// TestHandleInsertError tests that a failed insert does not publish a handle.
func TestHandleInsertError(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("name", ForString())

	handle, err := c.InsertHandle(func(r Row) error {
		return fmt.Errorf("insert failure")
	})
	assert.Error(t, err)
	assert.Zero(t, handle)
	assert.Equal(t, 0, c.Count())

	// The slot should be immediately re-usable
	handle, err = c.InsertHandle(func(r Row) error {
		r.SetString("name", "merlin")
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), handle.Index())
}

// TestHandleAt tests minting a handle for an existing numeric index.
func TestHandleAt(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("name", ForString())

	idx, err := c.Insert(func(r Row) error {
		r.SetString("name", "merlin")
		return nil
	})
	assert.NoError(t, err)

	// Mint a handle for the numeric index and use it
	handle, ok := c.HandleAt(idx)
	assert.True(t, ok)
	assert.Equal(t, idx, handle.Index())
	assert.NoError(t, c.QueryHandle(handle, func(r Row) error {
		name, _ := r.String("name")
		assert.Equal(t, "merlin", name)
		return nil
	}))

	// No row at the index, no handle
	_, ok = c.HandleAt(99)
	assert.False(t, ok)
}

// TestHandleZero tests that the zero handle is never valid.
func TestHandleZero(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("name", ForString())

	_, err := c.InsertHandle(func(r Row) error {
		r.SetString("name", "merlin")
		return nil
	})
	assert.NoError(t, err)

	assert.Equal(t, ErrInvalidHandle, c.QueryHandle(0, func(r Row) error {
		return fmt.Errorf("should not be called")
	}))
	assert.False(t, c.DeleteHandle(0))
}

// TestHandleRow tests that a row can publish its own handle during iteration.
func TestHandleRow(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("name", ForString())

	idx, err := c.Insert(func(r Row) error {
		r.SetString("name", "merlin")
		return nil
	})
	assert.NoError(t, err)

	// Read the handle off the row itself and use it afterwards
	var handle Handle
	assert.NoError(t, c.QueryAt(idx, func(r Row) error {
		handle = r.Handle()
		return nil
	}))
	assert.NotZero(t, handle)
	assert.Equal(t, idx, handle.Index())
	assert.NoError(t, c.QueryHandle(handle, func(r Row) error {
		name, _ := r.String("name")
		assert.Equal(t, "merlin", name)
		return nil
	}))
}

// TestHandleBatch tests handles minted for a batch of rows loaded in a single
// transaction, simulating a batch load followed by slot re-use.
func TestHandleBatch(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("name", ForString())

	// Batch load 100 rows and collect their handles
	handles := make([]Handle, 0, 100)
	assert.NoError(t, c.Query(func(txn *Txn) error {
		for i := 0; i < 100; i++ {
			handle, err := txn.InsertHandle(func(r Row) error {
				r.SetString("name", fmt.Sprintf("name-%d", r.Index()))
				return nil
			})
			assert.NoError(t, err)
			handles = append(handles, handle)
		}
		return nil
	}))

	// All handles should resolve to their rows
	for i, handle := range handles {
		assert.NoError(t, c.QueryHandle(handle, func(r Row) error {
			name, _ := r.String("name")
			assert.Equal(t, fmt.Sprintf("name-%d", i), name)
			return nil
		}))
	}

	// Delete every second row (by numeric index) and re-load the batch
	for i := 0; i < 100; i += 2 {
		assert.True(t, c.DeleteAt(uint32(i)))
	}
	reloaded := make([]Handle, 0, 50)
	assert.NoError(t, c.Query(func(txn *Txn) error {
		for i := 0; i < 50; i++ {
			handle, err := txn.InsertHandle(func(r Row) error {
				r.SetString("name", "reloaded")
				return nil
			})
			assert.NoError(t, err)
			reloaded = append(reloaded, handle)
		}
		return nil
	}))

	// The old handles of deleted rows must stay invalid and must not resolve
	// to the rows of the new batch that re-used their slots.
	for i := 0; i < 100; i += 2 {
		assert.Equal(t, ErrInvalidHandle, c.QueryHandle(handles[i], func(r Row) error {
			return fmt.Errorf("should not be called")
		}))
	}
	for _, handle := range reloaded {
		assert.NoError(t, c.QueryHandle(handle, func(r Row) error {
			name, _ := r.String("name")
			assert.Equal(t, "reloaded", name)
			return nil
		}))
	}

	// The handles of rows that were not deleted must still be valid
	for i := 1; i < 100; i += 2 {
		assert.NoError(t, c.QueryHandle(handles[i], func(r Row) error {
			name, _ := r.String("name")
			assert.Equal(t, fmt.Sprintf("name-%d", i), name)
			return nil
		}))
	}
}

// TestHandleSnapshotRestore tests that handles and their generations survive a
// snapshot/restore round-trip and keep matching the same rows.
func TestHandleSnapshotRestore(t *testing.T) {
	input := NewCollection()
	input.CreateColumn("name", ForString())

	// Insert a batch and delete some rows to advance generations
	handles := make([]Handle, 0, 10)
	for i := 0; i < 10; i++ {
		handle, err := input.InsertHandle(func(r Row) error {
			r.SetString("name", fmt.Sprintf("name-%d", r.Index()))
			return nil
		})
		assert.NoError(t, err)
		handles = append(handles, handle)
	}
	assert.True(t, input.DeleteHandle(handles[3]))
	assert.True(t, input.DeleteHandle(handles[7]))

	// Re-insert into the re-used slots
	reused, err := input.InsertHandle(func(r Row) error {
		r.SetString("name", "reused")
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(3), reused.Index())

	// Snapshot and restore into a new collection
	buffer := bytes.NewBuffer(nil)
	assert.NoError(t, input.Snapshot(buffer))
	output := NewCollection()
	output.CreateColumn("name", ForString())
	assert.NoError(t, output.Restore(buffer))
	assert.Equal(t, input.Count(), output.Count())

	// Handles of live rows must still match the same rows after the restore
	for i, handle := range handles {
		if i == 3 || i == 7 {
			continue
		}
		assert.NoError(t, output.QueryHandle(handle, func(r Row) error {
			name, _ := r.String("name")
			assert.Equal(t, fmt.Sprintf("name-%d", i), name)
			return nil
		}), "handle %v should be valid", handle)
	}

	// Handles of rows deleted before the snapshot must stay invalid
	assert.Equal(t, ErrInvalidHandle, output.QueryHandle(handles[7], func(r Row) error {
		return fmt.Errorf("should not be called")
	}))

	// The re-used slot keeps its post-delete generation after the restore
	assert.NoError(t, output.QueryHandle(reused, func(r Row) error {
		name, _ := r.String("name")
		assert.Equal(t, "reused", name)
		return nil
	}))
	assert.Equal(t, ErrInvalidHandle, output.QueryHandle(handles[3], func(r Row) error {
		return fmt.Errorf("should not be called")
	}))

	// Deleting through the restored collection invalidates the handle everywhere
	assert.True(t, output.DeleteHandle(reused))
	assert.Equal(t, ErrInvalidHandle, output.QueryHandle(reused, func(r Row) error {
		return fmt.Errorf("should not be called")
	}))
}

// TestHandleSnapshotDeletedTail tests that generations of rows deleted before
// a snapshot are retained even when no rows are left, so stale handles cannot
// match rows inserted after the restore.
func TestHandleSnapshotDeletedTail(t *testing.T) {
	input := NewCollection()
	input.CreateColumn("name", ForString())

	// Insert and delete everything, leaving only advanced generations behind
	stale := make([]Handle, 0, 10)
	for i := 0; i < 10; i++ {
		handle, err := input.InsertHandle(func(r Row) error {
			r.SetString("name", "merlin")
			return nil
		})
		assert.NoError(t, err)
		stale = append(stale, handle)
	}
	for _, handle := range stale {
		assert.True(t, input.DeleteHandle(handle))
	}
	assert.Equal(t, 0, input.Count())

	// Snapshot the empty collection and restore it
	buffer := bytes.NewBuffer(nil)
	assert.NoError(t, input.Snapshot(buffer))
	output := NewCollection()
	output.CreateColumn("name", ForString())
	assert.NoError(t, output.Restore(buffer))
	assert.Equal(t, 0, output.Count())

	// Insert new rows which re-use the slots
	for i := 0; i < 10; i++ {
		_, err := output.InsertHandle(func(r Row) error {
			r.SetString("name", "gandalf")
			return nil
		})
		assert.NoError(t, err)
	}

	// The stale handles must not resolve to the new rows
	for _, handle := range stale {
		assert.Equal(t, ErrInvalidHandle, output.QueryHandle(handle, func(r Row) error {
			return fmt.Errorf("should not be called")
		}))
		assert.False(t, output.DeleteHandle(handle))
	}
}

// TestHandleSnapshotConcurrentDelete tests that a delete committed while a
// snapshot is in progress is replayed on top of the restored generations, so
// the deleted row's handle stays invalid after the restore.
func TestHandleSnapshotConcurrentDelete(t *testing.T) {
	input := NewCollection()
	input.CreateColumn("name", ForString())

	// Insert a batch of rows and keep their handles
	handles := make([]Handle, 0, 5)
	for i := 0; i < 5; i++ {
		handle, err := input.InsertHandle(func(r Row) error {
			r.SetString("name", fmt.Sprintf("name-%d", r.Index()))
			return nil
		})
		assert.NoError(t, err)
		handles = append(handles, handle)
	}

	// Start a snapshot and delete a row while the recorder is active, so the
	// delete is only present in the snapshot's commit log.
	buffer := bytes.NewBuffer(nil)
	recorder, err := input.recorderOpen()
	assert.NoError(t, err)
	_, err = input.writeState(s2.NewWriter(buffer))
	assert.NoError(t, err)
	assert.True(t, input.DeleteHandle(handles[2]))
	input.recorderClose()
	assert.NoError(t, recorder.Copy(buffer))

	// Restore and replay the pending delete on top of the snapshot
	output := NewCollection()
	output.CreateColumn("name", ForString())
	assert.NoError(t, output.Restore(buffer))
	assert.Equal(t, 4, output.Count())

	// The deleted row's handle must be invalid, the others must still match
	assert.Equal(t, ErrInvalidHandle, output.QueryHandle(handles[2], func(r Row) error {
		return fmt.Errorf("should not be called")
	}))
	for i, handle := range handles {
		if i == 2 {
			continue
		}
		assert.NoError(t, output.QueryHandle(handle, func(r Row) error {
			name, _ := r.String("name")
			assert.Equal(t, fmt.Sprintf("name-%d", handle.Index()), name)
			return nil
		}), "handle %v should be valid", handle)
	}
}

// TestHandleRestoreV1 tests that snapshots written in the version 0x1 format
// (without generations) can still be restored.
func TestHandleRestoreV1(t *testing.T) {	input := NewCollection()
	input.CreateColumn("name", ForString())
	for i := 0; i < 100; i++ {
		_, err := input.Insert(func(r Row) error {
			r.SetString("name", "merlin")
			return nil
		})
		assert.NoError(t, err)
	}

	// Write a snapshot in the legacy version 0x1 format, which does not carry
	// the generation vector.
	buffer := bytes.NewBuffer(nil)
	writer := iostream.NewWriter(buffer)
	page := commit.NewBuffer(chunkSize)
	assert.NoError(t, writer.WriteUvarint(0x1))
	assert.NoError(t, writer.WriteUvarint(3)) // inserts + 'expire' + 'name' columns
	assert.NoError(t, writer.WriteRange(input.chunks(), func(i int, w *iostream.Writer) error {
		return input.readChunk(commit.Chunk(i), func(lastCommit uint64, chunk commit.Chunk, fill bitmap.Bitmap) error {
			offset := chunk.Min()
			assert.NoError(t, writer.WriteUvarint(lastCommit))
			page.Reset(rowColumn)
			fill.Range(func(idx uint32) {
				page.PutOperation(commit.Insert, offset+idx)
			})
			if err := writer.WriteSelf(page); err != nil {
				return err
			}
			return input.cols.RangeUntil(func(column *column) error {
				if !column.Snapshot(chunk, page) {
					return nil
				}
				return writer.WriteSelf(page)
			})
		})
	}))
	assert.NoError(t, writer.Flush())

	// Restore the legacy snapshot
	output := NewCollection()
	output.CreateColumn("name", ForString())
	_, err := output.readState(bytes.NewReader(buffer.Bytes()))
	assert.NoError(t, err)
	assert.Equal(t, 100, output.Count())

	// Handles minted after the restore must work as usual
	handle, ok := output.HandleAt(5)
	assert.True(t, ok)
	assert.NoError(t, output.QueryHandle(handle, func(r Row) error {
		name, _ := r.String("name")
		assert.Equal(t, "merlin", name)
		return nil
	}))
}
