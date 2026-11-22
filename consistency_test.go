// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the repo root for details.

package column

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
	"github.com/stretchr/testify/assert"
)

// --------------------------- Test Helpers ----------------------------

// gateLogger is a commit logger whose first armed Append blocks until
// releaseNow is called. It stays passive until Armed, so collection setup
// commits flow through unimpeded.
type gateLogger struct {
	release chan struct{}
	entered chan struct{}
	armed   atomic.Bool
	once    sync.Once
}

func newGateLogger() *gateLogger {
	return &gateLogger{
		release: make(chan struct{}),
		entered: make(chan struct{}),
	}
}

// Arm starts blocking the next Append call.
func (g *gateLogger) Arm() { g.armed.Store(true) }

// Entered returns a channel closed once an armed Append has been reached.
func (g *gateLogger) Entered() <-chan struct{} { return g.entered }

func (g *gateLogger) Append(commit.Commit) error {
	if g.armed.Load() {
		g.once.Do(func() { close(g.entered) })
		<-g.release
	}
	return nil
}

func (g *gateLogger) releaseNow() { close(g.release) }

// newCrossChunkCollection builds a collection with two full chunks (16K rows
// each, 32K rows total) where every row stores the same integer value.
func newCrossChunkCollection(t *testing.T, mode ReadConsistency, logger commit.Logger) *Collection {
	c := NewCollection(Options{
		Capacity:        2 * chunkSize,
		Vacuum:          time.Hour,
		Writer:          logger,
		ReadConsistency: mode,
	})
	assert.NoError(t, c.CreateColumn("v", ForInt64()))

	assert.NoError(t, c.Query(func(txn *Txn) error {
		v := txn.Int64("v")
		for i := 0; i < 2*chunkSize; i++ {
			txn.Insert(func(r Row) error {
				v.Set(1)
				return nil
			})
		}
		return nil
	}))
	assert.Equal(t, 2*chunkSize, c.Count())
	return c
}

// readValue reads a single row through the standard Query path.
func readValue(t *testing.T, c *Collection, idx uint32) int64 {
	var v int64
	assert.NoError(t, c.QueryAt(idx, func(r Row) error {
		v, _ = r.Int64("v")
		return nil
	}))
	return v
}

// readValueAsync performs a point read on a separate goroutine and reports the
// result on the returned channel.
func readValueAsync(t *testing.T, c *Collection, idx uint32) <-chan int64 {
	out := make(chan int64, 1)
	go func() {
		out <- readValue(t, c, idx)
	}()
	return out
}

// readChunk reads a value from a raw column while holding the read latch of
// its chunk, the same lock a Query-based reader takes through rangeRead /
// QueryAt. It lets tests take a consistent snapshot of a chunk while a writer
// is blocked on its write latch.
func readChunk(c *Collection, chunk commit.Chunk, idx uint32) int64 {
	c.slock.RLock(uint(chunk))
	defer c.slock.RUnlock(uint(chunk))
	return readChunkLocked(c, idx)
}

// readChunkLocked reads a raw column value without touching the latch. It is
// used when the caller already holds the read latch of the row's chunk
// (sync.RWMutex does not support recursive read locking).
func readChunkLocked(c *Collection, idx uint32) int64 {
	col, ok := c.cols.Load("v")
	if !ok {
		return -1
	}
	v, _ := col.Value(idx)
	return v.(int64)
}

// readIndex reads an index bitmap while holding the read latch of its chunk.
func readIndex(c *Collection, name string, chunk commit.Chunk) bitmap.Bitmap {
	c.slock.RLock(uint(chunk))
	defer c.slock.RUnlock(uint(chunk))
	return readIndexLocked(c, name, chunk)
}

// readIndexLocked reads an index bitmap without touching the latch. It is
// used when the caller already holds the read latch of the chunk.
func readIndexLocked(c *Collection, name string, chunk commit.Chunk) bitmap.Bitmap {
	col, ok := c.cols.Load(name)
	if !ok {
		return nil
	}
	return col.Index(chunk)
}

// scanValues scans the full collection and reports whether every observed
// value is uniform, along with the observed value.
func scanValues(c *Collection) (uniform bool, value int64, mixed bool) {
	first := true
	uniform = true
	c.Query(func(txn *Txn) error {
		v := txn.Int64("v")
		txn.Range(func(idx uint32) {
			got, _ := v.Get()
			if first {
				value = got
				first = false
			} else if got != value {
				uniform = false
				mixed = true
			}
		})
		return nil
	})
	return
}

// updateAll enqueues an update of every row inside a single transaction.
func updateAll(t *testing.T, c *Collection, value int64) {
	assert.NoError(t, c.Query(func(txn *Txn) error {
		v := txn.Int64("v")
		txn.Range(func(idx uint32) {
			v.Set(value)
		})
		return nil
	}))
}

// assertBlocked fails if the read completes while a batch commit is expected
// to be in progress.
func assertBlocked(t *testing.T, ch <-chan int64, what string) {
	t.Helper()
	select {
	case got := <-ch:
		t.Fatalf("%s observed value %d while batch commit was in progress", what, got)
	case <-time.After(100 * time.Millisecond):
	}
}

// assertReceives fails if the read does not complete after the batch commit
// has finished.
func assertReceives(t *testing.T, ch <-chan int64, want int64, what string) {
	t.Helper()
	select {
	case got := <-ch:
		assert.Equal(t, want, got)
	case <-time.After(5 * time.Second):
		t.Fatalf("%s remained blocked after the batch commit finished", what)
	}
}

// --------------------------- Tests ----------------------------

// TestReadChunkedTornRead demonstrates the problem the batch mode solves.
// A reader holding the latch of the last chunk pins the default chunked
// commit after its first chunk has been applied: the committed chunks carry
// the new value while the pinned chunk still exposes the previous version.
func TestReadChunkedTornRead(t *testing.T) {
	c := newCrossChunkCollection(t, ReadChunked, nil)
	defer c.Close()

	const lastChunk commit.Chunk = 1
	const lastIdx = uint32(2*chunkSize - 1)

	// Pin the last chunk so the writer can commit chunk 0 but cannot enter
	// chunk 1. This is exactly the point a chunk-by-chunk reader can reach.
	c.slock.RLock(uint(lastChunk))

	done := make(chan struct{})
	go func() {
		updateAll(t, c, 2)
		close(done)
	}()

	// Wait until the writer has committed the first chunk and is blocked
	// waiting for the last chunk's write latch.
	deadline := time.Now().Add(5 * time.Second)
	for readChunk(c, 0, 0) != 2 {
		if time.Now().After(deadline) {
			t.Fatal("writer never committed the first chunk")
		}
		time.Sleep(time.Millisecond)
	}

	// Torn read: one side of the boundary is new, the pinned side is old.
	assert.Equal(t, int64(2), readChunk(c, 0, 0))
	assert.Equal(t, int64(1), readChunkLocked(c, lastIdx))

	c.slock.RUnlock(uint(lastChunk))
	<-done

	// After the commit completes, every chunk carries the new version.
	assert.Equal(t, int64(2), readValue(t, c, 0))
	assert.Equal(t, int64(2), readValue(t, c, lastIdx))
}

// TestReadBatchAtomicVisibility verifies that, in batch mode, a reader cannot
// observe a partially committed multi-chunk transaction, regardless of the
// direction in which the chunks are traversed.
func TestReadBatchAtomicVisibility(t *testing.T) {
	t.Run("point reads block", func(t *testing.T) {
		gate := newGateLogger()
		c := newCrossChunkCollection(t, ReadBatch, gate)
		defer c.Close()

		gate.Arm()
		done := make(chan struct{})
		go func() {
			updateAll(t, c, 2)
			close(done)
		}()
		<-gate.Entered() // all dirty latches are now held by the writer

		// Point reads on either side of the chunk boundary must block.
		first := readValueAsync(t, c, 0)
		last := readValueAsync(t, c, uint32(2*chunkSize-1))
		assertBlocked(t, first, "first-chunk reader")
		assertBlocked(t, last, "last-chunk reader")

		gate.releaseNow()
		<-done
		assertReceives(t, first, 2, "first-chunk reader")
		assertReceives(t, last, 2, "last-chunk reader")

		// Once visible, every scan must observe a single uniform version.
		uniform, value, mixed := scanValues(c)
		assert.True(t, uniform)
		assert.False(t, mixed)
		assert.Equal(t, int64(2), value)
	})

	t.Run("reverse traversal", func(t *testing.T) {
		gate := newGateLogger()
		c := newCrossChunkCollection(t, ReadBatch, gate)
		defer c.Close()

		gate.Arm()
		done := make(chan struct{})
		go func() {
			updateAll(t, c, 2)
			close(done)
		}()
		<-gate.Entered()

		// Reading from the last chunk backwards must block as well: a reader
		// cannot evade the visibility boundary through traversal order.
		type pair struct{ last, first int64 }
		out := make(chan pair, 1)
		go func() {
			var p pair
			p.last = readValue(t, c, uint32(2*chunkSize-1))
			p.first = readValue(t, c, 0)
			out <- p
		}()
		select {
		case got := <-out:
			t.Fatalf("reverse reader observed %+v while batch commit was in progress", got)
		case <-time.After(100 * time.Millisecond):
		}

		gate.releaseNow()
		<-done
		select {
		case got := <-out:
			assert.Equal(t, int64(2), got.last)
			assert.Equal(t, int64(2), got.first)
		case <-time.After(5 * time.Second):
			t.Fatal("reverse reader remained blocked after the batch commit finished")
		}
	})
}

// TestReadChunkedIndexTearing shows that, in the default mode, an index can
// be updated for the already committed chunk while a pinned chunk still
// carries the previous index state.
func TestReadChunkedIndexTearing(t *testing.T) {
	c := newCrossChunkCollection(t, ReadChunked, nil)
	defer c.Close()
	assert.NoError(t, c.CreateIndex("v2", "v", func(r Reader) bool {
		return r.Int() == 2
	}))

	const lastChunk commit.Chunk = 1
	c.slock.RLock(uint(lastChunk))

	done := make(chan struct{})
	go func() {
		updateAll(t, c, 2)
		close(done)
	}()

	// Wait until the first chunk's index reflects the new value.
	deadline := time.Now().Add(5 * time.Second)
	for readIndex(c, "v2", 0).Count() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("writer never updated the first chunk's index")
		}
		time.Sleep(time.Millisecond)
	}

	// Index is new on chunk 0 but still the old empty state on chunk 1.
	assert.Equal(t, chunkSize, readIndex(c, "v2", 0).Count())
	assert.Equal(t, 0, readIndexLocked(c, "v2", lastChunk).Count())

	c.slock.RUnlock(uint(lastChunk))
	<-done

	assert.Equal(t, chunkSize, readIndex(c, "v2", 0).Count())
	assert.Equal(t, chunkSize, readIndex(c, "v2", lastChunk).Count())
}

// TestReadBatchIndexVisibility ensures that the computed bitmap indexes share
// the same visibility boundary as the underlying data: while a batch commit
// is in flight, an indexed Query is blocked; after it completes, the index
// reflects the new state for every chunk.
func TestReadBatchIndexVisibility(t *testing.T) {
	gate := newGateLogger()
	c := newCrossChunkCollection(t, ReadBatch, gate)
	defer c.Close()
	assert.NoError(t, c.CreateIndex("v2", "v", func(r Reader) bool {
		return r.Int() == 2
	}))

	c.Query(func(txn *Txn) error {
		assert.Equal(t, 0, txn.With("v2").Count())
		return nil
	})

	gate.Arm()
	done := make(chan struct{})
	go func() {
		updateAll(t, c, 2)
		close(done)
	}()
	<-gate.Entered()

	// The indexed read must block rather than see a partially updated index.
	countDone := make(chan int, 1)
	go func() {
		count := 0
		c.Query(func(txn *Txn) error {
			count = txn.With("v2").Count()
			return nil
		})
		countDone <- count
	}()

	select {
	case got := <-countDone:
		t.Fatalf("indexed reader observed count %d while batch commit was in progress", got)
	case <-time.After(100 * time.Millisecond):
	}

	gate.releaseNow()
	<-done

	select {
	case got := <-countDone:
		assert.Equal(t, 2*chunkSize, got)
	case <-time.After(5 * time.Second):
		t.Fatal("indexed reader remained blocked after the batch commit finished")
	}

	// Data and index stay aligned once the commit is visible.
	c.Query(func(txn *Txn) error {
		assert.Equal(t, 2*chunkSize, txn.With("v2").Count())
		assert.Equal(t, 2*chunkSize, txn.Count())
		return nil
	})
	uniform, value, mixed := scanValues(c)
	assert.True(t, uniform)
	assert.False(t, mixed)
	assert.Equal(t, int64(2), value)
}

// TestReadBatchRollbackKeepsOldVersion checks that a failed transaction leaves
// the previous version fully intact in batch mode: rollback happens before
// any latch is acquired, so nothing is partially applied.
func TestReadBatchRollbackKeepsOldVersion(t *testing.T) {
	c := newCrossChunkCollection(t, ReadBatch, nil)
	defer c.Close()

	err := c.Query(func(txn *Txn) error {
		v := txn.Int64("v")
		txn.Range(func(idx uint32) {
			v.Set(2)
		})
		return assert.AnError
	})
	assert.Error(t, err)

	uniform, value, mixed := scanValues(c)
	assert.True(t, uniform)
	assert.False(t, mixed)
	assert.Equal(t, int64(1), value)
}

// TestReadBatchSingleChunk keeps the single-chunk fast path working: a commit
// touching one chunk must still become visible and keep data consistent.
func TestReadBatchSingleChunk(t *testing.T) {
	c := NewCollection(Options{
		Vacuum:          time.Hour,
		ReadConsistency: ReadBatch,
	})
	defer c.Close()
	assert.NoError(t, c.CreateColumn("v", ForInt64()))

	idx, err := c.Insert(func(r Row) error {
		r.SetInt64("v", 1)
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(1), readValue(t, c, idx))

	assert.NoError(t, c.QueryAt(idx, func(r Row) error {
		r.SetInt64("v", 42)
		return nil
	}))
	assert.Equal(t, int64(42), readValue(t, c, idx))
}

// TestReadBatchConcurrentStress hammers a batch-consistency collection with a
// multi-chunk writer and concurrent readers. Every reader must always observe
// a uniform version across the whole collection; run with -race to validate
// the locking as well.
func TestReadBatchConcurrentStress(t *testing.T) {
	c := newCrossChunkCollection(t, ReadBatch, nil)
	defer c.Close()

	const iterations = 25
	var wg sync.WaitGroup
	var violations int64
	var readers sync.WaitGroup

	// Continuous readers until the writer finishes.
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					uniform, _, mixed := scanValues(c)
					if !uniform || mixed {
						atomic.AddInt64(&violations, 1)
					}
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			updateAll(t, c, int64(1+(i%2))) // alternate 1 -> 2 -> 1 ...
		}
	}()
	wg.Wait()
	close(stop)
	readers.Wait()

	assert.Equal(t, int64(0), atomic.LoadInt64(&violations))
	uniform, value, mixed := scanValues(c)
	assert.True(t, uniform)
	assert.False(t, mixed)
	assert.Contains(t, []int64{1, 2}, value)
}

// TestReadConsistencyOption ensures the default collection keeps the legacy
// chunked behaviour and that the option is propagated to the collection.
func TestReadConsistencyOption(t *testing.T) {
	def := NewCollection(Options{Vacuum: time.Hour})
	defer def.Close()
	assert.Equal(t, ReadChunked, def.opts.ReadConsistency)
	assert.False(t, def.batchReads())

	batch := NewCollection(Options{Vacuum: time.Hour, ReadConsistency: ReadBatch})
	defer batch.Close()
	assert.Equal(t, ReadBatch, batch.opts.ReadConsistency)
	assert.True(t, batch.batchReads())
}
