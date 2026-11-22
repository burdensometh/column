// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/kelindar/column/commit"
	"github.com/stretchr/testify/assert"
)

// --------------------------- Helpers ----------------------------

// sellableRule is a cross-column rule: in stock and currently sellable.
func sellableRule(r RowReader) bool {
	stock, _ := r.Int("stock")
	status, _ := r.String("status")
	return stock > 0 && status == "sellable"
}

// newProducts creates a collection of products with a derived index.
func newProducts(withIndex bool) *Collection {
	col := NewCollection()
	col.CreateColumn("name", ForString())
	col.CreateColumn("stock", ForInt())
	col.CreateColumn("status", ForString())
	if withIndex {
		col.CreateDerivedIndex("sellable", []string{"stock", "status"}, sellableRule)
	}
	return col
}

// insertProduct inserts a single product row.
func insertProduct(col *Collection, name string, stock int, status string) uint32 {
	idx, err := col.Insert(func(r Row) error {
		r.SetString("name", name)
		r.SetInt("stock", stock)
		r.SetString("status", status)
		return nil
	})
	if err != nil {
		panic(err)
	}
	return idx
}

// indexMembers returns the members of the "sellable" index.
func indexMembers(col *Collection) map[uint32]bool {
	members := make(map[uint32]bool)
	col.Query(func(txn *Txn) error {
		return txn.With("sellable").Range(func(idx uint32) {
			members[idx] = true
		})
	})
	return members
}

// scanMembers returns the members by filtering every row by its final values.
func scanMembers(col *Collection) map[uint32]bool {
	members := make(map[uint32]bool)
	col.Query(func(txn *Txn) error {
		stock := txn.Int("stock")
		status := txn.String("status")
		return txn.Range(func(idx uint32) {
			s, ok1 := stock.Get()
			st, ok2 := status.Get()
			if ok1 && ok2 && s > 0 && st == "sellable" {
				members[idx] = true
			}
		})
	})
	return members
}

// assertIndexEqualsScan asserts that the index matches the row-by-row filter.
func assertIndexEqualsScan(t *testing.T, col *Collection) {
	assert.Equal(t, scanMembers(col), indexMembers(col))
}

// --------------------------- Tests ----------------------------

func TestCreateDerivedIndex(t *testing.T) {
	col := newProducts(false)
	defer col.Close()

	// Insert data before the index is created, it should be backfilled
	insertProduct(col, "a", 5, "sellable") // member
	insertProduct(col, "b", 0, "sellable") // no stock
	insertProduct(col, "c", 3, "hold")     // not sellable
	insertProduct(col, "d", 7, "sellable") // member
	insertProduct(col, "e", 1, "archived") // not sellable

	// Create the index and verify that it was built from the existing data
	assert.NoError(t, col.CreateDerivedIndex("sellable", []string{"stock", "status"}, sellableRule))
	assert.Equal(t, 2, len(indexMembers(col)))
	assertIndexEqualsScan(t, col)

	// The index should be usable for querying
	col.Query(func(txn *Txn) error {
		names := txn.String("name")
		var found []string
		txn.With("sellable").Range(func(i uint32) {
			name, _ := names.Get()
			found = append(found, name)
		})
		assert.ElementsMatch(t, []string{"a", "d"}, found)
		return nil
	})
}

func TestCreateDerivedIndexInvalid(t *testing.T) {
	col := newProducts(false)
	defer col.Close()

	assert.Error(t, col.CreateDerivedIndex("", []string{"stock"}, sellableRule))
	assert.Error(t, col.CreateDerivedIndex("sellable", nil, sellableRule))
	assert.Error(t, col.CreateDerivedIndex("sellable", []string{"stock"}, nil))
	assert.Error(t, col.CreateDerivedIndex("sellable", []string{"invalid"}, sellableRule))

	// Duplicate index name should be rejected
	assert.NoError(t, col.CreateDerivedIndex("sellable", []string{"stock", "status"}, sellableRule))
	assert.Error(t, col.CreateDerivedIndex("sellable", []string{"stock"}, sellableRule))
	assert.Error(t, col.CreateDerivedIndex("stock", []string{"stock"}, sellableRule))
}

func TestDerivedIndexMultiColumnCommit(t *testing.T) {
	col := newProducts(false)
	defer col.Close()

	// Count the evaluations and record the values observed by the rule
	var mu sync.Mutex
	var evals int
	type observation struct {
		idx    uint32
		stock  int64
		status string
	}
	var observed []observation
	rule := func(r RowReader) bool {
		stock, _ := r.Int("stock")
		status, _ := r.String("status")
		mu.Lock()
		evals++
		observed = append(observed, observation{r.Index(), stock, status})
		mu.Unlock()
		return stock > 0 && status == "sellable"
	}
	assert.NoError(t, col.CreateDerivedIndex("sellable", []string{"stock", "status"}, rule))

	// Insert 3 rows, each insert evaluates the rule exactly once
	insertProduct(col, "a", 1, "hold")
	insertProduct(col, "b", 2, "sellable")
	insertProduct(col, "c", 3, "hold")
	assert.Equal(t, 3, evals)

	// Update both columns of every row in a single transaction
	mu.Lock()
	evals, observed = 0, nil
	mu.Unlock()
	assert.NoError(t, col.Query(func(txn *Txn) error {
		stock := txn.Int("stock")
		status := txn.String("status")
		return txn.Range(func(idx uint32) {
			stock.Set(10)
			status.Set("sellable")
		})
	}))

	// Each row must be recomputed exactly once, observing only final values
	assert.Equal(t, 3, evals)
	for _, o := range observed {
		assert.Equal(t, int64(10), o.stock)
		assert.Equal(t, "sellable", o.status)
	}
	assert.Equal(t, 3, len(indexMembers(col)))
	assertIndexEqualsScan(t, col)
}

func TestDerivedIndexTransitions(t *testing.T) {
	col := newProducts(true)
	defer col.Close()

	// Start with a member and flip its fields one at a time
	idx := insertProduct(col, "a", 5, "sellable")
	assert.Equal(t, 1, len(indexMembers(col)))

	// Out of stock
	assert.NoError(t, col.QueryAt(idx, func(r Row) error {
		r.SetInt("stock", 0)
		return nil
	}))
	assert.Equal(t, 0, len(indexMembers(col)))

	// Back in stock
	assert.NoError(t, col.QueryAt(idx, func(r Row) error {
		r.SetInt("stock", 3)
		return nil
	}))
	assert.Equal(t, 1, len(indexMembers(col)))

	// No longer sellable
	assert.NoError(t, col.QueryAt(idx, func(r Row) error {
		r.SetString("status", "hold")
		return nil
	}))
	assert.Equal(t, 0, len(indexMembers(col)))

	// Both fields change in the same transaction, final state is a member
	assert.NoError(t, col.QueryAt(idx, func(r Row) error {
		r.SetInt("stock", 8)
		r.SetString("status", "sellable")
		return nil
	}))
	assert.Equal(t, 1, len(indexMembers(col)))

	// Merge the stock down to zero, should leave the index
	assert.NoError(t, col.QueryAt(idx, func(r Row) error {
		r.MergeInt("stock", -8)
		return nil
	}))
	assert.Equal(t, 0, len(indexMembers(col)))
	assertIndexEqualsScan(t, col)
}

func TestDerivedIndexPartialRow(t *testing.T) {
	col := newProducts(true)
	defer col.Close()

	// Insert a row without one of the dependent fields
	idx, err := col.Insert(func(r Row) error {
		r.SetInt("stock", 5)
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(indexMembers(col)))

	// Once the missing field is set, the row becomes a member
	assert.NoError(t, col.QueryAt(idx, func(r Row) error {
		r.SetString("status", "sellable")
		return nil
	}))
	assert.Equal(t, 1, len(indexMembers(col)))
	assertIndexEqualsScan(t, col)
}

func TestDerivedIndexDelete(t *testing.T) {
	col := newProducts(true)
	defer col.Close()

	a := insertProduct(col, "a", 5, "sellable")
	b := insertProduct(col, "b", 3, "sellable")
	insertProduct(col, "c", 0, "sellable")
	assert.Equal(t, 2, len(indexMembers(col)))

	// Deleting a member row removes it from the index
	assert.True(t, col.DeleteAt(a))
	assert.Equal(t, 1, len(indexMembers(col)))
	assertIndexEqualsScan(t, col)

	// Deleting everything empties the index
	assert.NoError(t, col.Query(func(txn *Txn) error {
		txn.DeleteAll()
		return nil
	}))
	assert.Equal(t, 0, len(indexMembers(col)))
	assert.Equal(t, 0, col.Count())

	// The freed offset can be reused and indexed again
	d := insertProduct(col, "d", 1, "sellable")
	assert.Equal(t, 1, len(indexMembers(col)))
	assert.True(t, indexMembers(col)[d])
	assert.False(t, indexMembers(col)[b] && b != d)
	assertIndexEqualsScan(t, col)
}

func TestDerivedIndexRemoveDependentField(t *testing.T) {
	col := newProducts(true)
	defer col.Close()

	idx := insertProduct(col, "a", 5, "sellable")
	assert.Equal(t, 1, len(indexMembers(col)))

	// Replay a commit which removes the "status" field of the row
	buffer := commit.NewBuffer(0)
	buffer.Reset("status")
	buffer.PutOperation(commit.Delete, idx)
	assert.NoError(t, col.Replay(commit.Commit{
		ID:      commit.Next(),
		Chunk:   commit.ChunkAt(idx),
		Updates: []*commit.Buffer{buffer},
	}))

	// The row still exists but is no longer a member of the index
	assert.Equal(t, 1, col.Count())
	assert.Equal(t, 0, len(indexMembers(col)))
	assertIndexEqualsScan(t, col)

	// Setting the field back brings the row back into the index
	assert.NoError(t, col.QueryAt(idx, func(r Row) error {
		r.SetString("status", "sellable")
		return nil
	}))
	assert.Equal(t, 1, len(indexMembers(col)))
	assertIndexEqualsScan(t, col)
}

func TestDerivedIndexSnapshotRestore(t *testing.T) {
	input := newProducts(true)
	defer input.Close()

	// Insert a mix of rows, update both fields in single transactions and
	// delete some of the rows
	for i := 0; i < 100; i++ {
		insertProduct(input, fmt.Sprintf("p%03d", i), i%5, []string{"sellable", "hold"}[i%2])
	}
	for i := 0; i < 100; i += 3 {
		input.QueryAt(uint32(i), func(r Row) error {
			r.SetInt("stock", 9)
			r.SetString("status", "sellable")
			return nil
		})
	}
	for i := 0; i < 100; i += 7 {
		input.DeleteAt(uint32(i))
	}
	assertIndexEqualsScan(t, input)

	// Snapshot and restore into a new collection with the index pre-created
	buffer := bytes.NewBuffer(nil)
	assert.NoError(t, input.Snapshot(buffer))

	output := newProducts(true)
	defer output.Close()
	assert.NoError(t, output.Restore(buffer))

	// The With() result must equal filtering row-by-row by the final values
	assert.Equal(t, input.Count(), output.Count())
	assert.Equal(t, indexMembers(input), indexMembers(output))
	assertIndexEqualsScan(t, output)

	// The restored index must keep tracking subsequent updates
	output.QueryAt(1, func(r Row) error {
		r.SetInt("stock", 4)
		r.SetString("status", "sellable")
		return nil
	})
	output.DeleteAt(2)
	assertIndexEqualsScan(t, output)
}

func TestDerivedIndexReplay(t *testing.T) {
	writer := make(commit.Channel, 64)
	primary := NewCollection(Options{Writer: &writer})
	primary.CreateColumn("stock", ForInt())
	primary.CreateColumn("status", ForString())
	assert.NoError(t, primary.CreateDerivedIndex("sellable", []string{"stock", "status"}, sellableRule))
	defer primary.Close()

	replica := NewCollection()
	replica.CreateColumn("stock", ForInt())
	replica.CreateColumn("status", ForString())
	assert.NoError(t, replica.CreateDerivedIndex("sellable", []string{"stock", "status"}, sellableRule))
	defer replica.Close()

	// Stream all of the commits into the replica
	done := make(chan struct{})
	go func() {
		defer close(done)
		for change := range writer {
			assert.NoError(t, replica.Replay(change))
		}
	}()

	// Insert, update both fields in single transactions and delete
	for i := 0; i < 50; i++ {
		primary.Insert(func(r Row) error {
			r.SetInt("stock", i%4)
			r.SetString("status", "sellable")
			return nil
		})
	}
	for i := 0; i < 50; i += 2 {
		primary.QueryAt(uint32(i), func(r Row) error {
			r.SetInt("stock", 0)
			r.SetString("status", "hold")
			return nil
		})
	}
	for i := 0; i < 50; i += 5 {
		primary.DeleteAt(uint32(i))
	}

	close(writer)
	<-done

	// After the replay, the With() result must equal filtering row-by-row
	assert.Equal(t, primary.Count(), replica.Count())
	assert.Equal(t, indexMembers(primary), indexMembers(replica))
	assertIndexEqualsScan(t, replica)
}

func TestDropDerivedIndex(t *testing.T) {
	col := newProducts(true)
	defer col.Close()

	insertProduct(col, "a", 5, "sellable")
	assert.Equal(t, 1, len(indexMembers(col)))

	// Dropping the index removes it from all of its dependencies
	assert.NoError(t, col.DropIndex("sellable"))
	assert.Error(t, col.DropIndex("sellable"))
	assert.Equal(t, 0, len(indexMembers(col)))

	// Updates to the previously dependent columns should not panic
	assert.NoError(t, col.QueryAt(0, func(r Row) error {
		r.SetInt("stock", 10)
		r.SetString("status", "hold")
		return nil
	}))

	// The index can be re-created under the same name
	assert.NoError(t, col.CreateDerivedIndex("sellable", []string{"stock", "status"}, sellableRule))
	assertIndexEqualsScan(t, col)

	// A derived index cannot be dropped as a trigger
	assert.Error(t, col.DropTrigger("sellable"))
}

func TestDerivedIndexDeclaredColumns(t *testing.T) {
	col := newProducts(false)
	defer col.Close()

	// The rule can only read the declared dependencies
	assert.NoError(t, col.CreateDerivedIndex("sellable", []string{"stock", "status"}, func(r RowReader) bool {
		_, ok := r.Value("name")
		assert.False(t, ok, "undeclared column should not be readable")

		_, ok = r.Value("stock")
		assert.True(t, ok, "declared column should be readable")

		_, ok = r.String("stock")
		assert.False(t, ok, "wrong type should not be readable")
		return sellableRule(r)
	}))

	insertProduct(col, "a", 5, "sellable")
	assertIndexEqualsScan(t, col)
}

func TestDerivedIndexMultiChunk(t *testing.T) {
	col := newProducts(false)
	defer col.Close()

	// Insert enough rows to span multiple chunks
	const n = 20000
	statuses := []string{"sellable", "hold", "archived"}
	assert.NoError(t, col.Query(func(txn *Txn) error {
		for i := 0; i < n; i++ {
			if _, err := txn.Insert(func(r Row) error {
				r.SetInt("stock", i%7)
				r.SetString("status", statuses[i%3])
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}))

	// The index must be built from the existing data across all of the chunks
	assert.NoError(t, col.CreateDerivedIndex("sellable", []string{"stock", "status"}, sellableRule))
	assertIndexEqualsScan(t, col)
	assert.NotEmpty(t, indexMembers(col))

	// Snapshot and restore across chunks, the index must be rebuilt
	buffer := bytes.NewBuffer(nil)
	assert.NoError(t, col.Snapshot(buffer))

	output := newProducts(true)
	defer output.Close()
	assert.NoError(t, output.Restore(buffer))
	assert.Equal(t, col.Count(), output.Count())
	assert.Equal(t, indexMembers(col), indexMembers(output))
	assertIndexEqualsScan(t, output)

	// Update rows in multiple chunks in a single transaction
	assert.NoError(t, col.Query(func(txn *Txn) error {
		stock := txn.Int("stock")
		return txn.Range(func(idx uint32) {
			stock.Set(0)
		})
	}))
	assert.Equal(t, 0, len(indexMembers(col)))
	assertIndexEqualsScan(t, col)
}
