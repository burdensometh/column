// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"sort"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)

const (
	bitmapShift = chunkShift - 6
	bitmapSize  = 1 << bitmapShift
	chunkShift  = 14 // 16K
	chunkSize   = 1 << chunkShift
)

// initialize ensures that the transaction is pre-initialized with the snapshot
// of the owner's fill list.
func (txn *Txn) initialize() {
	if txn.setup {
		return
	}

	txn.owner.lock.RLock()
	txn.index.Grow(uint32(txn.owner.opts.Capacity))
	txn.owner.fill.Clone(&txn.index)
	txn.owner.lock.RUnlock()
	txn.setup = true
}

// --------------------------- Locked Seek ---------------------------

// QueryAt jumps at a particular offset in the collection, sets the cursor to the
// provided position and executes given callback fn.
func (txn *Txn) QueryAt(index uint32, f func(Row) error) (err error) {
	lock := txn.owner.slock
	txn.cursor = index

	chunk := commit.ChunkAt(index)
	lock.RLock(uint(chunk))
	err = f(Row{txn})
	lock.RUnlock(uint(chunk))
	return err
}

// --------------------------- Locked Range ---------------------------

// rangeRead iterates over index, chunk by chunk and ensures that each
// chunk is protected by an appropriate read lock.
func (txn *Txn) rangeRead(f func(chunk commit.Chunk, index bitmap.Bitmap)) {
	limit := commit.Chunk(len(txn.index) >> bitmapShift)
	lock := txn.owner.slock

	for chunk := commit.Chunk(0); chunk <= limit; chunk++ {
		lock.RLock(uint(chunk))
		f(chunk, chunk.OfBitmap(txn.index))
		lock.RUnlock(uint(chunk))
	}
}

// rangeReadPair iterates over the index and another bitmap, chunk by chunk and
// ensures that each chunk is protected by an appropriate read lock.
func (txn *Txn) rangeReadPair(column *column, f func(a, b bitmap.Bitmap)) {
	limit := commit.Chunk(len(txn.index) >> bitmapShift)
	lock := txn.owner.slock

	// Iterate through all of the chunks and acquire appropriate shard locks.
	for chunk := commit.Chunk(0); chunk <= limit; chunk++ {
		lock.RLock(uint(chunk))
		f(chunk.OfBitmap(txn.index), column.Index(chunk))
		lock.RUnlock(uint(chunk))
	}
}

// rangeWrite ranges over the dirty chunks and acquires exclusive latches along
// the way. This is used to commit a transaction.
func (txn *Txn) rangeWrite(fn func(commitID uint64, chunk commit.Chunk, fill bitmap.Bitmap)) {
	lock := txn.owner.slock
	txn.dirty.Range(func(x uint32) {
		chunk := commit.Chunk(x)
		commitID := commit.Next()
		lock.Lock(uint(chunk))

		// Compute the fill and set the last commit ID
		txn.owner.lock.RLock()
		fill := chunk.OfBitmap(txn.owner.fill)
		txn.owner.commits[chunk] = commitID // OK, since we have a shard lock
		txn.owner.lock.RUnlock()

		// Call the delegate
		fn(commitID, chunk, fill)
		lock.Unlock(uint(chunk))
	})
}

// shardCount is the number of shards in the collection's sharded latch (see
// smutex.SMutex128). A chunk is protected by shard "chunk % shardCount".
const shardCount = 128

// rangeWriteBatch commits a transaction atomically with respect to readers.
// Unlike rangeWrite, every latch protecting a dirty chunk is acquired before
// any change is applied and all of the latches are held until the entire
// transaction, including the computed indexes, has been applied. A reader
// traversing chunks through rangeRead therefore cannot observe a mixture of
// the previous and the committed version, regardless of traversal order. All
// affected chunks share a single commit ID which acts as the visibility
// boundary of the transaction.
func (txn *Txn) rangeWriteBatch(fn func(commitID uint64, chunk commit.Chunk, fill bitmap.Bitmap)) {
	if txn.dirty.Count() == 0 {
		return
	}

	lock := txn.owner.slock

	// Collect the distinct latch shards protecting the dirty chunks and lock
	// them in a deterministic ascending order. Locking in a fixed order makes
	// two concurrent batch commits deadlock-free, even when their chunks map
	// onto overlapping shards.
	shards := make([]uint, 0, txn.dirty.Count())
	seen := make(map[uint]struct{}, txn.dirty.Count())
	txn.dirty.Range(func(x uint32) {
		shard := uint(commit.Chunk(x)) % shardCount
		if _, ok := seen[shard]; !ok {
			seen[shard] = struct{}{}
			shards = append(shards, shard)
		}
	})
	sort.Slice(shards, func(i, j int) bool { return shards[i] < shards[j] })
	for _, shard := range shards {
		lock.Lock(shard)
	}

	// All latches are held: publish the transaction under a single commit ID.
	commitID := commit.Next()
	txn.dirty.Range(func(x uint32) {
		chunk := commit.Chunk(x)

		// Compute the fill and set the last commit ID
		txn.owner.lock.RLock()
		fill := chunk.OfBitmap(txn.owner.fill)
		txn.owner.commits[chunk] = commitID // OK, since we have a shard lock
		txn.owner.lock.RUnlock()

		// Call the delegate
		fn(commitID, chunk, fill)
	})

	// Release the latches only after every chunk has been committed. Readers
	// blocked on any of these shards wake up seeing the complete transaction.
	for i := len(shards) - 1; i >= 0; i-- {
		lock.Unlock(shards[i])
	}
}
