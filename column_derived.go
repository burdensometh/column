// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)

// --------------------------- Row Reader ---------------------------

// RowReader represents a read-only view of a single row, scoped to the set of
// columns a derived index depends on. It reads the final, stored values of the
// row and is only valid during the evaluation of the index rule. Columns that
// were not declared as dependencies of the index cannot be read through it.
type RowReader interface {

	// Index returns the offset of the row being evaluated.
	Index() uint32

	// Value retrieves the value of a dependent column, by its name.
	Value(column string) (any, bool)

	// String retrieves the value of a dependent textual column, by its name.
	String(column string) (string, bool)

	// Float retrieves the value of a dependent numeric column as a float64.
	Float(column string) (float64, bool)

	// Int retrieves the value of a dependent numeric column as an int64.
	Int(column string) (int64, bool)

	// Uint retrieves the value of a dependent numeric column as an uint64.
	Uint(column string) (uint64, bool)

	// Bool retrieves the value of a dependent boolean column, by its name.
	Bool(column string) (bool, bool)
}

// derivedColumn represents a resolved column a derived index depends on.
type derivedColumn struct {
	name string  // The name of the column
	col  *column // The resolved column
}

// derivedRow represents a row cursor passed to the rule of a derived index.
type derivedRow struct {
	idx  uint32          // The offset of the row being evaluated
	deps []derivedColumn // The columns the index depends on
}

// Assert the interface implementation.
var _ RowReader = (*derivedRow)(nil)

// Index returns the offset of the row being evaluated.
func (r *derivedRow) Index() uint32 {
	return r.idx
}

// Value retrieves the value of a dependent column, by its name.
func (r *derivedRow) Value(columnName string) (any, bool) {
	if c := r.columnAt(columnName); c != nil {
		return c.Value(r.idx)
	}
	return nil, false
}

// String retrieves the value of a dependent textual column, by its name.
func (r *derivedRow) String(columnName string) (string, bool) {
	if c := r.columnAt(columnName); c != nil && c.IsTextual() {
		return c.Column.(Textual).LoadString(r.idx)
	}
	return "", false
}

// Float retrieves the value of a dependent numeric column as a float64.
func (r *derivedRow) Float(columnName string) (float64, bool) {
	if c := r.columnAt(columnName); c != nil && c.IsNumeric() {
		return c.Column.(Numeric).LoadFloat64(r.idx)
	}
	return 0, false
}

// Int retrieves the value of a dependent numeric column as an int64.
func (r *derivedRow) Int(columnName string) (int64, bool) {
	if c := r.columnAt(columnName); c != nil && c.IsNumeric() {
		return c.Column.(Numeric).LoadInt64(r.idx)
	}
	return 0, false
}

// Uint retrieves the value of a dependent numeric column as an uint64.
func (r *derivedRow) Uint(columnName string) (uint64, bool) {
	if c := r.columnAt(columnName); c != nil && c.IsNumeric() {
		return c.Column.(Numeric).LoadUint64(r.idx)
	}
	return 0, false
}

// Bool retrieves the value of a dependent boolean column, by its name.
func (r *derivedRow) Bool(columnName string) (bool, bool) {
	if c := r.columnAt(columnName); c != nil {
		if _, ok := c.Column.(*columnBool); ok {
			return c.Contains(r.idx), true
		}
	}
	return false, false
}

// columnAt resolves a dependent column by its name.
func (r *derivedRow) columnAt(columnName string) *column {
	for i := range r.deps {
		if r.deps[i].name == columnName {
			return r.deps[i].col
		}
	}
	return nil
}

// --------------------------- Derived Index ----------------------------

// Assert the interface implementation.
var (
	_ Column   = (*columnDerivedIndex)(nil)
	_ computed = (*columnDerivedIndex)(nil)
)

// columnDerivedIndex represents a bitmap index whose membership is derived
// from the values of multiple columns. The membership of a row is recomputed
// only once, after all of the updates to the row have been applied.
type columnDerivedIndex struct {
	fill  bitmap.Bitmap        // The fill list for the index
	dirty bitmap.Bitmap        // The rows pending recomputation
	deps  []derivedColumn      // The columns this index depends on
	rule  func(RowReader) bool // The rule to apply when recomputing membership
}

// newDerivedIndex creates a new derived bitmap index column.
func newDerivedIndex(indexName string, deps []derivedColumn, rule func(RowReader) bool) *column {
	return columnFor(indexName, &columnDerivedIndex{
		fill:  make(bitmap.Bitmap, 0, 4),
		dirty: make(bitmap.Bitmap, 0, 4),
		deps:  deps,
		rule:  rule,
	})
}

// Grow grows the size of the column until we have enough to store.
func (c *columnDerivedIndex) Grow(idx uint32) {
	c.fill.Grow(idx)
	c.dirty.Grow(idx)
}

// Column returns the name of the first column this index depends on.
func (c *columnDerivedIndex) Column() string {
	return c.deps[0].name
}

// Columns returns the names of all of the columns this index depends on.
func (c *columnDerivedIndex) Columns() []string {
	columns := make([]string, 0, len(c.deps))
	for _, dep := range c.deps {
		columns = append(columns, dep.name)
	}
	return columns
}

// Apply applies a set of operations to the column. Since the membership of a
// row can only be recomputed once all of its dependent columns have been
// updated, this only marks the affected rows and defers the recomputation
// until the end of the chunk commit. Deleted rows are removed immediately.
func (c *columnDerivedIndex) Apply(chunk commit.Chunk, r *commit.Reader) {
	for r.Next() {
		switch r.Type {
		case commit.Put, commit.Merge:
			c.dirty.Set(uint32(r.Offset))
		case commit.Delete:
			c.fill.Remove(uint32(r.Offset))
			c.dirty.Remove(uint32(r.Offset))
		}
	}
}

// recompute recomputes the membership of all of the dirty rows of a chunk.
// This must be called after all of the updates for the chunk were applied, so
// that the dependent columns contain their final values. The provided fill
// list represents the rows currently present in the collection.
func (c *columnDerivedIndex) recompute(chunk commit.Chunk, fill bitmap.Bitmap) {
	dirty := chunk.OfBitmap(c.dirty)
	if len(dirty) == 0 {
		return
	}

	// Recompute the membership of each dirty row from its final values. A row
	// is only a member if it exists, all of its dependent fields are present
	// and the rule evaluates to true.
	offset := chunk.Min()
	row := derivedRow{deps: c.deps}
	dirty.Range(func(x uint32) {
		row.idx = offset + x
		if fill.Contains(x) && c.present(row.idx) && c.rule(&row) {
			c.fill.Set(row.idx)
		} else {
			c.fill.Remove(row.idx)
		}
	})

	dirty.Clear()
}

// present checks whether all of the dependent columns have a value for a row.
func (c *columnDerivedIndex) present(idx uint32) bool {
	for i := range c.deps {
		if _, ok := c.deps[i].col.Value(idx); !ok {
			return false
		}
	}
	return true
}

// Value retrieves a value at a specified index.
func (c *columnDerivedIndex) Value(idx uint32) (v any, ok bool) {
	if idx < uint32(len(c.fill))<<6 {
		v, ok = c.fill.Contains(idx), true
	}
	return
}

// Contains checks whether the column has a value at a specified index.
func (c *columnDerivedIndex) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column.
func (c *columnDerivedIndex) Index(chunk commit.Chunk) bitmap.Bitmap {
	return chunk.OfBitmap(c.fill)
}

// Snapshot writes the entire column into the specified destination buffer.
// This is a no-op given that derived indexes are rebuilt from the data.
func (c *columnDerivedIndex) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
}

// recomputeDerived recomputes the membership of a chunk if the column is a
// derived index, otherwise this is a no-op.
func (c *column) recomputeDerived(chunk commit.Chunk, fill bitmap.Bitmap) {
	if index, ok := c.Column.(*columnDerivedIndex); ok {
		c.lock.RLock()
		index.recompute(chunk, fill)
		c.lock.RUnlock()
	}
}
