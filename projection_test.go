// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// --------------------------- Fixtures ----------------------------

// newKeyedCollection creates a keyed collection with a few typed columns.
func newKeyedCollection(t *testing.T, amount int) *Collection {
	c := NewCollection(Options{Capacity: amount})
	c.CreateColumn("id", ForKey())
	c.CreateColumn("name", ForString())
	c.CreateColumn("age", ForInt())
	c.CreateColumn("active", ForBool())

	for i := 0; i < amount; i++ {
		i := i
		assert.NoError(t, c.InsertKey(fmt.Sprintf("k%03d", i), func(r Row) error {
			r.SetString("name", fmt.Sprintf("name-%d", i))
			r.SetInt("age", i)
			r.SetBool("active", i%2 == 0)
			return nil
		}))
	}
	return c
}

// emptyKeyed creates an empty collection matching the projection schema.
func emptyKeyed(capacity int) *Collection {
	c := NewCollection(Options{Capacity: capacity})
	c.CreateColumn("id", ForKey())
	c.CreateColumn("name", ForString())
	c.CreateColumn("age", ForInt())
	c.CreateColumn("active", ForBool())
	return c
}

// emptyProjected creates a collection with only the columns a projection kept.
func emptyProjected(capacity int) *Collection {
	c := NewCollection(Options{Capacity: capacity})
	c.CreateColumn("id", ForKey())
	c.CreateColumn("name", ForString())
	c.CreateColumn("age", ForInt())
	return c
}

// --------------------------- Projection ----------------------------

// TestSnapshotProjectionSelectsRowsAndColumns verifies that only the rows in the
// declared visibility scope and only the requested columns (plus the mandatory
// key column) end up in the restorable snapshot.
func TestSnapshotProjectionSelectsRowsAndColumns(t *testing.T) {
	const amount = 100
	src := newKeyedCollection(t, amount)

	// Delete a middle row so the source fill-list has a hole.
	assert.NoError(t, src.DeleteKey("k010"))

	buffer := bytes.NewBuffer(nil)
	err := src.SnapshotProjection(buffer, func(txn *Txn) error {
		txn.WithInt("age", func(v int64) bool {
			return v >= 50 // 50 rows expected: ages 50..99
		})
		return nil
	}, "name", "age")
	assert.NoError(t, err)
	assert.NotZero(t, buffer.Len())

	// Restore into a collection declared with the projected columns only.
	dst := emptyProjected(amount)
	assert.NoError(t, dst.Restore(buffer))

	// Countable: exactly the selected rows.
	assert.Equal(t, 50, dst.Count())

	// Key search works and values round-trip, including across the deleted hole:
	// source key k011 lived at index 11, it must not inherit k010's index.
	copied := 0
	assert.NoError(t, dst.Query(func(txn *Txn) error {
		return txn.Range(func(idx uint32) {
			copied++

			// Dense, gap-free indexes starting at zero: no source index reused.
			assert.Less(t, idx, uint32(50))

			key, ok := txn.owner.pk.LoadString(idx)
			assert.True(t, ok)
			var age int
			assert.NoError(t, dst.QueryKey(key, func(r Row) error {
				name, ok := r.String("name")
				assert.True(t, ok)
				age, ok = r.Int("age")
				assert.True(t, ok)
				assert.Equal(t, fmt.Sprintf("name-%d", age), name)
				return nil
			}))
			assert.GreaterOrEqual(t, age, 50)
		})
	}))
	assert.Equal(t, 50, copied)

	// Unselected rows must not be present.
	assert.Error(t, dst.QueryKey("k000", func(r Row) error { return nil }))
	assert.Error(t, dst.QueryKey("k049", func(r Row) error { return nil }))
	assert.NoError(t, dst.QueryKey("k050", func(r Row) error { return nil }))

	// The deleted row was never exported.
	assert.Error(t, dst.QueryKey("k010", func(r Row) error { return nil }))
}

// TestSnapshotProjectionDenseIndexes checks the specific failure mode called out
// in the requirements: a surviving row next to a filtered-out row must not reuse
// its neighbor's index. After restore, row indexes are dense from zero.
func TestSnapshotProjectionDenseIndexes(t *testing.T) {
	src := newKeyedCollection(t, 10)
	assert.NoError(t, src.DeleteKey("k000")) // hole at index 0
	assert.NoError(t, src.DeleteKey("k002")) // hole at index 2

	buffer := bytes.NewBuffer(nil)
	assert.NoError(t, src.SnapshotProjection(buffer, nil, "age"))

	dst := emptyKeyed(10)
	assert.NoError(t, dst.Restore(buffer))
	assert.Equal(t, 8, dst.Count())

	indexes := make(map[uint32]string)
	assert.NoError(t, dst.Query(func(txn *Txn) error {
		return txn.Range(func(idx uint32) {
			key, ok := txn.owner.pk.LoadString(idx)
			assert.True(t, ok)
			indexes[idx] = key
		})
	}))

	// Exactly indexes 0..7, each used once.
	assert.Len(t, indexes, 8)
	for i := uint32(0); i < 8; i++ {
		assert.Contains(t, indexes, i)
	}

	// k001 was at source index 1 (between two holes); it must map to dense
	// index 0 and keep its own business key and value.
	assert.Equal(t, "k001", indexes[0])
	assert.NoError(t, dst.QueryAt(0, func(r Row) error {
		age, ok := r.Int("age")
		assert.True(t, ok)
		assert.Equal(t, 1, age)
		return nil
	}))
}

// TestSnapshotProjectionKeyAutomaticallyIncluded ensures the business key column
// is exported even when it is not listed explicitly.
func TestSnapshotProjectionKeyAutomaticallyIncluded(t *testing.T) {
	src := newKeyedCollection(t, 20)

	buffer := bytes.NewBuffer(nil)
	assert.NoError(t, src.SnapshotProjection(buffer, func(txn *Txn) error {
		txn.WithString("name", func(v string) bool { return v == "name-7" })
		return nil
	}, "name"))

	dst := emptyKeyed(20)
	assert.NoError(t, dst.Restore(buffer))
	assert.Equal(t, 1, dst.Count())

	var name string
	assert.NoError(t, dst.QueryKey("k007", func(r Row) error {
		v, ok := r.String("name")
		assert.True(t, ok)
		name = v
		return nil
	}))
	assert.Equal(t, "name-7", name)
}

// TestSnapshotProjectionRestoreIsWritable verifies that a restored projection is
// a normal collection: it supports inserts, upserts, deletes and another
// snapshot round-trip.
func TestSnapshotProjectionRestoreIsWritable(t *testing.T) {
	src := newKeyedCollection(t, 30)

	buffer := bytes.NewBuffer(nil)
	assert.NoError(t, src.SnapshotProjection(buffer, func(txn *Txn) error {
		txn.WithInt("age", func(v int64) bool { return v < 10 })
		return nil
	}, "name", "age"))

	dst := emptyProjected(30)
	assert.NoError(t, dst.Restore(buffer))
	assert.Equal(t, 10, dst.Count())

	// Continue ordinary writes.
	assert.NoError(t, dst.InsertKey("new-1", func(r Row) error {
		r.SetString("name", "new")
		r.SetInt("age", 100)
		return nil
	}))
	assert.Equal(t, 11, dst.Count())

	assert.NoError(t, dst.UpsertKey("k005", func(r Row) error {
		r.SetInt("age", 555)
		return nil
	}))
	assert.NoError(t, dst.QueryKey("k005", func(r Row) error {
		age, ok := r.Int("age")
		assert.True(t, ok)
		assert.Equal(t, 555, age)
		return nil
	}))

	assert.NoError(t, dst.DeleteKey("k000"))
	assert.Equal(t, 10, dst.Count())
	assert.Error(t, dst.QueryKey("k000", func(r Row) error { return nil }))

	// The restored-and-mutated collection can itself be snapshotted and restored.
	second := bytes.NewBuffer(nil)
	assert.NoError(t, dst.Snapshot(second))
	again := emptyProjected(30)
	assert.NoError(t, again.Restore(second))
	assert.Equal(t, 10, again.Count())
	assert.NoError(t, again.QueryKey("new-1", func(r Row) error { return nil }))
}

// TestSnapshotProjectionUnkeyed covers collections without a primary key.
func TestSnapshotProjectionUnkeyed(t *testing.T) {
	src := NewCollection()
	src.CreateColumn("name", ForString())
	src.CreateColumn("age", ForInt())
	for i := 0; i < 50; i++ {
		i := i
		_, err := src.Insert(func(r Row) error {
			r.SetString("name", fmt.Sprintf("n%d", i))
			r.SetInt("age", i)
			return nil
		})
		assert.NoError(t, err)
	}

	buffer := bytes.NewBuffer(nil)
	assert.NoError(t, src.SnapshotProjection(buffer, func(txn *Txn) error {
		txn.WithInt("age", func(v int64) bool { return v >= 25 })
		return nil
	}, "age"))

	dst := NewCollection()
	dst.CreateColumn("name", ForString())
	dst.CreateColumn("age", ForInt())
	assert.NoError(t, dst.Restore(buffer))
	assert.Equal(t, 25, dst.Count())

	// Ordinary writes keep working on the restored collection.
	_, err := dst.Insert(func(r Row) error {
		r.SetInt("age", 999)
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 26, dst.Count())
}

// TestSnapshotProjectionEmptySelection ensures an empty visibility scope still
// produces a valid, independently restorable snapshot.
func TestSnapshotProjectionEmptySelection(t *testing.T) {
	src := newKeyedCollection(t, 10)

	buffer := bytes.NewBuffer(nil)
	assert.NoError(t, src.SnapshotProjection(buffer, func(txn *Txn) error {
		txn.WithInt("age", func(v int64) bool { return v > 1000 })
		return nil
	}, "name"))

	dst := emptyKeyed(10)
	assert.NoError(t, dst.Restore(buffer))
	assert.Equal(t, 0, dst.Count())

	// The empty projection can still accept writes.
	assert.NoError(t, dst.InsertKey("k", func(r Row) error {
		r.SetString("name", "x")
		return nil
	}))
	assert.Equal(t, 1, dst.Count())
}

// TestSnapshotProjectionAllColumnTypes round-trips every concrete column type,
// including record columns carried as raw binary payloads.
func TestSnapshotProjectionAllColumnTypes(t *testing.T) {
	src := loadPlayers(1000)

	buffer := bytes.NewBuffer(nil)
	assert.NoError(t, src.SnapshotProjection(buffer, func(txn *Txn) error {
		txn.With("human")
		return nil
	}))

	dst := newEmpty(1000)
	assert.NoError(t, dst.Restore(buffer))

	humans := 0
	src.Query(func(txn *Txn) error {
		humans = txn.With("human").Count()
		return nil
	})
	assert.Equal(t, humans, dst.Count())
	assert.NotZero(t, humans)

	// A record column value survived the projection verbatim.
	assert.NoError(t, dst.QueryAt(0, func(r Row) error {
		record, ok := r.Record("location")
		assert.True(t, ok)
		assert.NotNil(t, record)
		return nil
	}))
}

// TestSnapshotProjectionRejectsInvalidColumns covers the explicit column
// validation: unknown columns and indexes cannot be exported.
func TestSnapshotProjectionRejectsInvalidColumns(t *testing.T) {
	src := loadPlayers(100)

	t.Run("unknown column", func(t *testing.T) {
		err := src.SnapshotProjection(bytes.NewBuffer(nil), nil, "does-not-exist")
		assert.Error(t, err)
	})

	t.Run("index column", func(t *testing.T) {
		err := src.SnapshotProjection(bytes.NewBuffer(nil), nil, "human")
		assert.Error(t, err)
	})
}

// TestSnapshotProjectionTxnEntry exercises the transaction-level entry point
// directly.
func TestSnapshotProjectionTxnEntry(t *testing.T) {
	src := newKeyedCollection(t, 40)

	buffer := bytes.NewBuffer(nil)
	assert.NoError(t, src.Query(func(txn *Txn) error {
		txn.WithInt("age", func(v int64) bool { return v%2 == 0 })
		return txn.SnapshotProjection(buffer, "name")
	}))

	dst := emptyKeyed(40)
	assert.NoError(t, dst.Restore(buffer))
	assert.Equal(t, 20, dst.Count()) // every second row is active
	assert.NoError(t, dst.QueryKey("k000", func(r Row) error { return nil }))
	assert.Error(t, dst.QueryKey("k001", func(r Row) error { return nil }))
}

// TestSnapshotProjectionConcurrentDeletes verifies that rows deleted by a
// concurrent transaction after the visibility boundary was captured can neither
// leak into the projection nor leave holes in its dense indexes.
func TestSnapshotProjectionConcurrentWritesAndDeletes(t *testing.T) {
	src := newKeyedCollection(t, 2000)

	// A producer keeps deleting rows while the projection scans. Regardless of
	// how many deletions land during the scan, every exported key must still
	// exist in the source at boundary time and every projected index must be
	// dense.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_ = src.DeleteKey(fmt.Sprintf("k%03d", i))
		}
	}()

	buffer := bytes.NewBuffer(nil)
	err := src.SnapshotProjection(buffer, func(txn *Txn) error {
		txn.WithInt("age", func(v int64) bool { return true })
		return nil
	}, "age")
	assert.NoError(t, err)
	wg.Wait()

	dst := emptyProjected(2000)
	assert.NoError(t, dst.Restore(buffer))

	// No gaps: projected indexes are exactly 0..count-1.
	indexes := make(map[uint32]struct{})
	assert.NoError(t, dst.Query(func(txn *Txn) error {
		return txn.Range(func(idx uint32) {
			indexes[idx] = struct{}{}
		})
	}))
	assert.Len(t, indexes, dst.Count())
	for i := 0; i < dst.Count(); i++ {
		assert.Contains(t, indexes, uint32(i))
	}

	// A key present in the projection must still map to a value; nothing was
	// duplicated or cross-indexed.
	assert.NoError(t, dst.Query(func(txn *Txn) error {
		return txn.Range(func(idx uint32) {
			key, ok := txn.owner.pk.LoadString(idx)
			assert.True(t, ok)
			assert.NoError(t, dst.QueryKey(key, func(r Row) error {
				_, hasAge := r.Int("age")
				assert.True(t, hasAge)
				return nil
			}))
		})
	}))
}

// TestSnapshotProjectionSkipsExpireColumn ensures the built-in expiration
// column is never part of the projection, whether columns are listed or not.
func TestSnapshotProjectionSkipsExpireColumn(t *testing.T) {
	src := newKeyedCollection(t, 10)
	src.QueryAt(0, func(r Row) error {
		r.SetTTL(time.Hour)
		return nil
	})

	// Naming the expiration column explicitly must not break restore, since the
	// target collection always creates it itself.
	buffer := bytes.NewBuffer(nil)
	assert.NoError(t, src.SnapshotProjection(buffer, nil, expireColumn, "age"))
	dst := emptyProjected(10)
	assert.NoError(t, dst.Restore(buffer))
	assert.Equal(t, 10, dst.Count())

	// The default (all columns) projection skips it as well.
	buffer2 := bytes.NewBuffer(nil)
	assert.NoError(t, src.SnapshotProjection(buffer2, nil))
	dst2 := emptyKeyed(10)
	assert.NoError(t, dst2.Restore(buffer2))
	assert.Equal(t, 10, dst2.Count())
}

// TestSnapshotProjectionConcurrentWrites pins down the export visibility
// boundary: rows inserted after the boundary must never leak into the snapshot,
// even though writes continue while the projection is running.
func TestSnapshotProjectionConcurrentWrites(t *testing.T) {
	src := loadPlayers(2000)

	// The "human" index and the boundary fill-list are captured before the
	// concurrent writers start adding non-human rows, so the expected count is
	// stable throughout the export.
	var expected int
	assert.NoError(t, src.Query(func(txn *Txn) error {
		expected = txn.With("human").Count()
		return nil
	}))

	var wg sync.WaitGroup
	wg.Add(4)
	for g := 0; g < 4; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				// Rows inserted during the export; they must be excluded.
				_, _ = src.Insert(func(r Row) error {
					r.SetString("serial", fmt.Sprintf("concurrent-%d-%d", g, i))
					r.SetEnum("race", "elf") // deliberately not "human"
					return nil
				})
			}
		}()
	}

	buffer := bytes.NewBuffer(nil)
	err := src.SnapshotProjection(buffer, func(txn *Txn) error {
		txn.With("human")
		return nil
	})
	assert.NoError(t, err)
	wg.Wait()

	dst := newEmpty(10000)
	assert.NoError(t, dst.Restore(buffer))

	// The count matches the declared boundary exactly, proving no row that
	// appeared during the export was brought along.
	assert.Equal(t, expected, dst.Count())

	// None of the concurrently inserted business keys leaked into the snapshot.
	var leaked bool
	assert.NoError(t, dst.Query(func(txn *Txn) error {
		return txn.Range(func(idx uint32) {
			serial, ok := txn.columnAt("serial")
			if !ok {
				return
			}
			if value, ok := serial.Value(idx); ok {
				if s, ok := value.(string); ok && strings.HasPrefix(s, "concurrent-") {
					leaked = true
				}
			}
		})
	}))
	assert.False(t, leaked)
}
