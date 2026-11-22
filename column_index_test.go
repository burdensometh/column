// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"testing"

	"github.com/kelindar/column/commit"
	"github.com/stretchr/testify/assert"
)

// ascendNames walks the sorted index and collects the row offsets in visit order.
func ascendNames(t *testing.T, c *Collection, indexName string, with ...string) []uint32 {
	var got []uint32
	assert.NoError(t, c.Query(func(txn *Txn) error {
		if len(with) > 0 {
			txn.With(with...)
		}
		return txn.Ascend(indexName, func(i uint32) {
			got = append(got, i)
		})
	}))
	return got
}

// TestSortIndexDuplicateKeys verifies that several rows sharing the same sort
// key each keep their own index entry. The tree must keep a total order using
// the stable row number as a tie-breaker, so Ascend returns every row instead
// of only the last inserted one.
func TestSortIndexDuplicateKeys(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("name", ForString())
	c.CreateColumn("age", ForInt())
	c.CreateIndex("adult", "age", func(r Reader) bool {
		return r.Int() >= 18
	})
	c.CreateSortIndex("sorted_name", "name")

	// Four products sharing the same sort name, distinct stable rows.
	ages := []int{10, 15, 20, 25} // rows 2,3 are adults
	for i := 0; i < 4; i++ {
		_, err := c.Insert(func(r Row) error {
			r.SetString("name", "widget")
			r.SetInt("age", ages[i])
			return nil
		})
		assert.NoError(t, err)
	}

	indexCol, _ := c.cols.Load("sorted_name")
	bt := indexCol.Column.(*columnSortIndex).btree
	assert.Equal(t, 4, bt.Len(), "duplicate keys must not collapse into one entry")

	// Ascend must visit every same-named row, in stable row order.
	assert.Equal(t, []uint32{0, 1, 2, 3}, ascendNames(t, c, "sorted_name"))

	// Mixed keys interleave with duplicates; equal keys stay ordered by row.
	idx, err := c.Insert(func(r Row) error {
		r.SetString("name", "anchor")
		r.SetInt("age", 99)
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(4), idx)
	assert.Equal(t, []uint32{4, 0, 1, 2, 3}, ascendNames(t, c, "sorted_name"))

	// Index traversal combined with a With filter keeps the order and returns
	// only the filtered population (rows 2,3 plus the adult "anchor" at 4).
	assert.Equal(t, []uint32{4, 2, 3}, ascendNames(t, c, "sorted_name", "adult"))
}

// TestSortIndexDuplicateUpdate verifies that updating one row only replaces
// that row's index entry while same-named rows remain queryable.
func TestSortIndexDuplicateUpdate(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("name", ForString())
	c.CreateSortIndex("sorted_name", "name")

	for i := 0; i < 3; i++ {
		_, err := c.Insert(func(r Row) error {
			r.SetString("name", "widget")
			return nil
		})
		assert.NoError(t, err)
	}

	indexCol, _ := c.cols.Load("sorted_name")
	si := indexCol.Column.(*columnSortIndex)

	// Move the middle row to a new key; the other duplicates must survive.
	assert.NoError(t, c.QueryAt(1, func(r Row) error {
		r.SetString("name", "gadget")
		return nil
	}))
	assert.Equal(t, 3, si.btree.Len())
	assert.Equal(t, []uint32{1, 0, 2}, ascendNames(t, c, "sorted_name"))

	// Update row 1 back to the shared key: it must not displace rows 0 or 2.
	assert.NoError(t, c.QueryAt(1, func(r Row) error {
		r.SetString("name", "widget")
		return nil
	}))
	assert.Equal(t, 3, si.btree.Len())
	assert.Equal(t, []uint32{0, 1, 2}, ascendNames(t, c, "sorted_name"))

	// The back-map must point every row at its current key, exactly once.
	assert.Len(t, si.backMap, 3)
	for idx := uint32(0); idx < 3; idx++ {
		assert.Equal(t, "widget", si.backMap[idx])
	}
}

// TestSortIndexDuplicateDelete verifies that deleting one row removes only its
// own index entry; rows sharing the key stay indexed, and reused offsets start
// from a clean state.
func TestSortIndexDuplicateDelete(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("name", ForString())
	c.CreateSortIndex("sorted_name", "name")

	for i := 0; i < 4; i++ {
		_, err := c.Insert(func(r Row) error {
			r.SetString("name", "widget")
			return nil
		})
		assert.NoError(t, err)
	}

	indexCol, _ := c.cols.Load("sorted_name")
	si := indexCol.Column.(*columnSortIndex)

	// Delete the middle row; the remaining same-named rows must still resolve.
	assert.True(t, c.DeleteAt(1))
	assert.Equal(t, 3, si.btree.Len())
	assert.Equal(t, []uint32{0, 2, 3}, ascendNames(t, c, "sorted_name"))
	_, present := si.backMap[uint32(1)]
	assert.False(t, present, "back-map entry must be removed with the row")

	// Re-inserting at the recycled offset must create a fresh, distinct entry.
	idx, err := c.Insert(func(r Row) error {
		r.SetString("name", "widget")
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), idx)
	assert.Equal(t, 4, si.btree.Len())
	assert.Equal(t, []uint32{0, 1, 2, 3}, ascendNames(t, c, "sorted_name"))

	// Deleting every row drains both the tree and the back-map.
	for _, i := range []uint32{0, 1, 2, 3} {
		assert.True(t, c.DeleteAt(i))
	}
	assert.Equal(t, 0, si.btree.Len())
	assert.Len(t, si.backMap, 0)
	assert.Empty(t, ascendNames(t, c, "sorted_name"))
}

// TestSortIndexDuplicateOrdering verifies interleaved duplicate keys are
// visited grouped by key and, within a key group, in stable row order. It also
// covers moving a row between key groups and deleting a never-indexed offset.
func TestSortIndexDuplicateOrdering(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("name", ForString())
	c.CreateSortIndex("sorted_name", "name")

	// Deleting an offset that was never indexed must be a safe no-op.
	assert.False(t, c.DeleteAt(7))

	keys := []string{"b", "a", "b", "a", "c", "a"}
	for _, k := range keys {
		_, err := c.Insert(func(r Row) error {
			r.SetString("name", k)
			return nil
		})
		assert.NoError(t, err)
	}

	indexCol, _ := c.cols.Load("sorted_name")
	si := indexCol.Column.(*columnSortIndex)
	assert.Equal(t, 6, si.btree.Len())

	// All "a" rows (1,3,5), then "b" rows (0,2), then "c" (4).
	assert.Equal(t, []uint32{1, 3, 5, 0, 2, 4}, ascendNames(t, c, "sorted_name"))

	// Move row 0 from "b" to "a": duplicates rebalance without losing entries.
	assert.NoError(t, c.QueryAt(0, func(r Row) error {
		r.SetString("name", "a")
		return nil
	}))
	assert.Equal(t, 6, si.btree.Len())
	assert.Equal(t, []uint32{0, 1, 3, 5, 2, 4}, ascendNames(t, c, "sorted_name"))

	// Removing one "a" row leaves the other rows in the same relative order.
	assert.True(t, c.DeleteAt(3))
	assert.Equal(t, 5, si.btree.Len())
	assert.Equal(t, []uint32{0, 1, 5, 2, 4}, ascendNames(t, c, "sorted_name"))
}

// TestSortIndexDuplicateReplay verifies that replayed puts and deletes apply
// the same per-row identity rules as live writes.
func TestSortIndexDuplicateReplay(t *testing.T) {
	primary := NewCollection()
	primary.CreateColumn("name", ForString())
	primary.CreateSortIndex("sorted_name", "name")

	replica := NewCollection()
	replica.CreateColumn("name", ForString())
	replica.CreateSortIndex("sorted_name", "name")

	// Capture the commit stream and replay every change onto the replica.
	writer := make(commit.Channel, 16)
	primary.logger = writer
	done := make(chan struct{})
	go func() {
		defer close(done)
		for change := range writer {
			assert.NoError(t, replica.Replay(change))
		}
	}()

	for i := 0; i < 3; i++ {
		_, err := primary.Insert(func(r Row) error {
			r.SetString("name", "widget")
			return nil
		})
		assert.NoError(t, err)
	}
	assert.NoError(t, primary.QueryAt(1, func(r Row) error {
		r.SetString("name", "gadget")
		return nil
	}))
	assert.True(t, primary.DeleteAt(2))

	close(writer)
	<-done

	indexCol, _ := replica.cols.Load("sorted_name")
	si := indexCol.Column.(*columnSortIndex)
	assert.Equal(t, 2, si.btree.Len())
	assert.Equal(t, []uint32{1, 0}, ascendNames(t, replica, "sorted_name"))
	assert.Len(t, si.backMap, 2)
}
