// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"
	"sync"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
	"github.com/tidwall/btree"
)

// btreeDegree is the default branching factor used by the view btrees.
const btreeDegree = 32

// --------------------------- Aggregate Result ----------------------------

// Aggregate represents the materialized aggregate of a group of rows.
type Aggregate struct {
	Count int64   // The number of rows in the group.
	Sum   float64 // The sum of the aggregated value column (0 if not configured).
	Min   float64 // The minimum value of the min column (0 if not configured).
	Max   float64 // The maximum value of the max column (0 if not configured).
}

// --------------------------- View Options ----------------------------

// viewConfig represents the configuration of a materialized view.
type viewConfig struct {
	sum string // The column to maintain a running sum of.
	min string // The column to maintain a running minimum of.
	max string // The column to maintain a running maximum of.
}

// ViewOption configures a materialized view.
type ViewOption func(*viewConfig)

// WithViewSum maintains a running sum of the given numeric column per group.
func WithViewSum(columnName string) ViewOption {
	return func(c *viewConfig) {
		c.sum = columnName
	}
}

// WithViewMin maintains a running minimum of the given numeric column per group.
func WithViewMin(columnName string) ViewOption {
	return func(c *viewConfig) {
		c.min = columnName
	}
}

// WithViewMax maintains a running maximum of the given numeric column per group.
func WithViewMax(columnName string) ViewOption {
	return func(c *viewConfig) {
		c.max = columnName
	}
}

// --------------------------- Materialized View ----------------------------

// View represents a materialized aggregate view. It maintains the count,
// sum, minimum and maximum of rows grouped by a user-defined key that can be
// derived from one or more columns. The view is maintained incrementally as
// rows are inserted, updated, deleted or expired, so querying the aggregates
// never requires a full collection scan.
//
// The view is only updated when a transaction is successfully committed.
// Rolled-back transactions never affect the aggregates. Similarly to sorted
// indexes, views are transient and must be recreated after a snapshot is
// restored.
type View struct {
	mu     sync.RWMutex                   // The read-write lock protecting the groups.
	name   string                         // The name of the view.
	key    func(Row) string               // The group-key extraction function.
	sum    *column                        // The column of the sum (optional).
	extr   []viewExtreme                  // The columns contributing to min/max.
	groups *btree.Map[string, *viewGroup] // The groups keyed by their group key.
	rows   map[uint32]viewEntry           // The last materialized state of each row.
}

// viewExtreme represents a value column that contributes to the min/max.
type viewExtreme struct {
	col *column // The numeric column.
	min bool    // Whether the minimum is tracked.
	max bool    // Whether the maximum is tracked.
}

// viewGroup represents the aggregated state of a single group.
type viewGroup struct {
	count int64                      // The number of rows in the group.
	sum   float64                    // The running sum.
	sets  []*btree.Map[float64, int] // The value frequency sets, aligned with view.extr.
}

// viewEntry represents the group-relevant state captured for a single row.
type viewEntry struct {
	key  string    // The group key.
	sum  float64   // The value of the sum column.
	vals []float64 // The values of the extreme columns, aligned with view.extr.
}

// newView creates a new materialized view.
func newView(name string, key func(Row) string, sum *column, extr []viewExtreme) *View {
	return &View{
		name:   name,
		key:    key,
		sum:    sum,
		extr:   extr,
		groups: btree.NewMap[string, *viewGroup](btreeDegree),
		rows:   make(map[uint32]viewEntry),
	}
}

// Name returns the name of the view.
func (v *View) Name() string {
	return v.name
}

// Len returns the number of groups currently held by the view.
func (v *View) Len() int {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.groups.Len()
}

// Get returns the aggregate for a given group key. It returns false when no
// rows currently belong to the group.
func (v *View) Get(groupKey string) (Aggregate, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	g, ok := v.groups.Get(groupKey)
	if !ok {
		return Aggregate{}, false
	}
	return v.aggregateOf(g), true
}

// Range iterates over all of the groups in ascending key order. The callback
// can return false to stop the iteration early.
func (v *View) Range(fn func(groupKey string, aggregate Aggregate) bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	v.groups.Scan(func(key string, g *viewGroup) bool {
		return fn(key, v.aggregateOf(g))
	})
}

// aggregateOf builds the aggregate result from the internal group state.
func (v *View) aggregateOf(g *viewGroup) Aggregate {
	a := Aggregate{
		Count: g.count,
		Sum:   g.sum,
	}

	for i, ex := range v.extr {
		if ex.min {
			if k, _, ok := g.sets[i].Min(); ok {
				a.Min = k
			}
		}
		if ex.max {
			if k, _, ok := g.sets[i].Max(); ok {
				a.Max = k
			}
		}
	}
	return a
}

// --------------------------- State Capture ----------------------------

// readEntry captures the group-relevant state of the row at the given index.
// It must be called while the underlying row state is stable (i.e. under the
// shard latch during a commit, or while writes are paused).
func (v *View) readEntry(row Row, idx uint32) viewEntry {
	row.txn.cursor = idx
	e := viewEntry{
		key:  v.key(row),
		vals: make([]float64, len(v.extr)),
	}

	if v.sum != nil {
		if f, ok := v.sum.Column.(Numeric).LoadFloat64(idx); ok {
			e.sum = f
		}
	}

	for i, ex := range v.extr {
		if f, ok := ex.col.Column.(Numeric).LoadFloat64(idx); ok {
			e.vals[i] = f
		}
	}
	return e
}

// capture snapshots the group-relevant state of the affected rows that are
// present in the provided chunk fill-list. The fill-list and the affected
// bitmap are both chunk-local (i.e. offsets are relative to the chunk start).
// Rows that are affected but no longer present in the fill-list have been
// deleted by the transaction and are simply absent from the result.
func (v *View) capture(txn *Txn, chunk commit.Chunk, fill, affected bitmap.Bitmap) map[uint32]viewEntry {
	row := Row{txn: txn}
	base := chunk.Min()
	out := make(map[uint32]viewEntry, affected.Count())

	affected.Range(func(x uint32) {
		if !fill.Contains(x) {
			return // The row is not present in this state snapshot.
		}

		idx := base + x
		out[idx] = v.readEntry(row, idx)
	})
	return out
}

// backfill rebuilds the whole view from all of the rows present in the
// fill-list. This must be called while all writes are paused.
func (v *View) backfill(txn *Txn, fill bitmap.Bitmap) {
	row := Row{txn: txn}
	v.mu.Lock()
	defer v.mu.Unlock()

	fill.Range(func(idx uint32) {
		e := v.readEntry(row, idx)
		v.add(e)
		v.rows[idx] = e
	})
}

// --------------------------- Incremental Maintenance ----------------------------

// commit applies the post-commit state of the rows touched by a transaction.
// It diffs the freshly captured state against the state previously
// materialized by the view, which makes the maintenance robust to rows whose
// index was reserved by a concurrent transaction that has not committed yet.
func (v *View) commit(chunk commit.Chunk, affected bitmap.Bitmap, after map[uint32]viewEntry) {
	v.mu.Lock()
	defer v.mu.Unlock()

	// Upsert rows that are present after the commit.
	for idx, a := range after {
		p, existed := v.rows[idx]
		switch {
		case !existed:
			v.add(a) // A newly visible row (inserted, or committed by another transaction first).
		case p.key != a.key:
			v.remove(p) // The row moved to another group.
			v.add(a)
		default:
			v.adjust(p, a) // The row stayed in the same group, adjust values.
		}
		v.rows[idx] = a
	}

	// Affected rows that are no longer present were deleted (or expired).
	base := chunk.Min()
	affected.Range(func(x uint32) {
		idx := base + x
		if _, present := after[idx]; present {
			return
		}
		if p, existed := v.rows[idx]; existed {
			v.remove(p)
			delete(v.rows, idx)
		}
	})
}

// groupFor loads or creates the aggregate state for a given group.
func (v *View) groupFor(key string) *viewGroup {
	g, ok := v.groups.Get(key)
	if !ok {
		g = &viewGroup{
			sets: make([]*btree.Map[float64, int], len(v.extr)),
		}
		for i := range g.sets {
			g.sets[i] = btree.NewMap[float64, int](btreeDegree)
		}
		v.groups.Set(key, g)
	}
	return g
}

// add contributes a row to its group.
func (v *View) add(e viewEntry) {
	g := v.groupFor(e.key)
	g.count++
	g.sum += e.sum
	for i, val := range e.vals {
		v.setAdd(g.sets[i], val)
	}
}

// remove removes the contribution of a row from its group and drops the group
// once it becomes empty.
func (v *View) remove(e viewEntry) {
	g, ok := v.groups.Get(e.key)
	if !ok {
		return
	}

	g.count--
	g.sum -= e.sum
	for i, val := range e.vals {
		v.setDel(g.sets[i], val)
	}

	if g.count <= 0 {
		v.groups.Delete(e.key)
	}
}

// adjust applies a value change of a row that stayed in the same group.
func (v *View) adjust(b, a viewEntry) {
	g := v.groupFor(a.key)
	g.sum += a.sum - b.sum
	for i := range v.extr {
		v.setDel(g.sets[i], b.vals[i])
		v.setAdd(g.sets[i], a.vals[i])
	}
}

// setAdd increments the frequency of a value in the multiset.
func (v *View) setAdd(set *btree.Map[float64, int], val float64) {
	if n, ok := set.Get(val); ok {
		set.Set(val, n+1)
	} else {
		set.Set(val, 1)
	}
}

// setDel decrements the frequency of a value in the multiset, removing the
// value entirely once no rows reference it anymore.
func (v *View) setDel(set *btree.Map[float64, int], val float64) {
	if n, ok := set.Get(val); ok {
		if n <= 1 {
			set.Delete(val)
		} else {
			set.Set(val, n-1)
		}
	}
}

// --------------------------- Collection Integration ----------------------------

// CreateView creates a materialized aggregate view. The key function derives
// the group key from one or more columns of a row (e.g. a composite key). The
// view is immediately populated with the current collection state and is then
// maintained incrementally by every successfully committed transaction.
//
// Similarly to sorted indexes, views are transient and are not part of
// snapshots: they must be recreated after a snapshot is restored.
func (c *Collection) CreateView(viewName string, key func(r Row) string, opts ...ViewOption) (*View, error) {
	if viewName == "" || key == nil {
		return nil, fmt.Errorf("column: create view must specify a name and a key function")
	}

	// Configure and resolve the referenced numeric columns.
	var cfg viewConfig
	for _, o := range opts {
		o(&cfg)
	}

	sumCol, err := c.numericColumn(cfg.sum)
	if err != nil {
		return nil, err
	}
	minCol, err := c.numericColumn(cfg.min)
	if err != nil {
		return nil, err
	}
	maxCol, err := c.numericColumn(cfg.max)
	if err != nil {
		return nil, err
	}

	// Build the distinct set of columns contributing to min/max.
	var extr []viewExtreme
	addExtreme := func(col *column, isMin, isMax bool) {
		if col == nil {
			return
		}
		for i := range extr {
			if extr[i].col == col {
				extr[i].min = extr[i].min || isMin
				extr[i].max = extr[i].max || isMax
				return
			}
		}
		extr = append(extr, viewExtreme{col: col, min: isMin, max: isMax})
	}
	addExtreme(minCol, true, false)
	addExtreme(maxCol, false, true)

	view := newView(viewName, key, sumCol, extr)

	// Pause all commits, backfill the view from the current state and register
	// it atomically so no committed change can be missed or double-counted.
	c.writeBarrier(func() {
		for _, existing := range c.views {
			if existing.name == viewName {
				err = fmt.Errorf("column: unable to create view, view '%s' already exists", viewName)
				return
			}
		}

		snapshot := make(bitmap.Bitmap, len(c.fill))
		copy(snapshot, c.fill)

		txn := &Txn{owner: c}
		view.backfill(txn, snapshot)
		c.views = append(c.views, view)
	})

	if err != nil {
		return nil, err
	}
	return view, nil
}

// DropView removes the materialized view with the specified name. If the view
// does not exist, this operation is a no-op.
func (c *Collection) DropView(viewName string) {
	c.writeBarrier(func() {
		views := c.views[:0]
		for _, v := range c.views {
			if v.name != viewName {
				views = append(views, v)
			}
		}
		c.views = views
	})
}

// View returns the materialized view with the specified name.
func (c *Collection) View(viewName string) (*View, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	for _, v := range c.views {
		if v.name == viewName {
			return v, true
		}
	}
	return nil, false
}

// numericColumn resolves a numeric column by its name. An empty name resolves
// to a nil (disabled) column.
func (c *Collection) numericColumn(columnName string) (*column, error) {
	if columnName == "" {
		return nil, nil
	}

	col, ok := c.cols.Load(columnName)
	if !ok {
		return nil, fmt.Errorf("column: unable to create view, column '%v' does not exist", columnName)
	}
	if !col.IsNumeric() {
		return nil, fmt.Errorf("column: unable to create view, column '%v' is not numeric", columnName)
	}
	return col, nil
}

// writeBarrier pauses all commits for the duration of the callback. It
// acquires every shard latch exclusively, which guarantees that no commit
// can be applied while the callback is running, and that no in-flight commit
// is partially visible.
func (c *Collection) writeBarrier(fn func()) {
	const shards = 128

	// Acquire all of the shard latches first. Commits only ever hold a single
	// shard latch at a time, so this cannot deadlock.
	for i := 0; i < shards; i++ {
		c.slock.Lock(uint(i))
	}

	// Once all commits are drained, acquire the collection lock so the
	// fill-list cannot be mutated concurrently either.
	c.lock.Lock()
	fn()
	c.lock.Unlock()

	for i := shards - 1; i >= 0; i-- {
		c.slock.Unlock(uint(i))
	}
}
