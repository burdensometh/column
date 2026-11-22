// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/kelindar/column/commit"
)

// --------------------------- Event ----------------------------

// EventType represents a type of a committed row change.
type EventType uint8

// Various committed row change types.
const (
	EventInsert EventType = iota + 1 // A new row was inserted
	EventUpdate                      // An existing row was updated
	EventDelete                      // An existing row was deleted
)

// String returns a string representation of the event type.
func (t EventType) String() string {
	switch t {
	case EventInsert:
		return "insert"
	case EventUpdate:
		return "update"
	case EventDelete:
		return "delete"
	default:
		return "unknown"
	}
}

// RowChange represents a committed change of a single logical row. All of the
// insert, update and delete operations affecting the same row within one commit
// are merged into a single event which carries the before/after values of the
// declared columns together with the identity of the commit that produced it.
type RowChange struct {
	Commit uint64         // The identity of the commit that produced this change
	Row    uint32         // The index of the changed row
	Type   EventType      // The type of the change (insert, update or delete)
	Before map[string]any // The values of the declared columns before the commit (nil for inserts)
	After  map[string]any // The values of the declared columns after the commit (nil for deletes)
}

// selectFor returns a copy of the event containing only the declared columns. A
// nil declared set carries all of the captured columns. The returned event owns
// its value maps, hence it is detached from the events delivered elsewhere.
func (e RowChange) selectFor(declared []string) RowChange {
	if declared == nil {
		e.Before = cloneValues(e.Before)
		e.After = cloneValues(e.After)
		return e
	}

	e.Before = selectValues(e.Before, declared)
	e.After = selectValues(e.After, declared)
	return e
}

// cloneValues copies a set of values so that a delivered event is detached from
// the events delivered to other subscriptions.
func cloneValues(values map[string]any) map[string]any {
	if values == nil {
		return nil
	}

	clone := make(map[string]any, len(values))
	for k, v := range values {
		clone[k] = v
	}
	return clone
}

// selectValues copies a set of values, keeping only the declared columns.
func selectValues(values map[string]any, declared []string) map[string]any {
	if values == nil {
		return nil
	}

	selected := make(map[string]any, len(declared))
	for _, column := range declared {
		if v, ok := values[column]; ok {
			selected[column] = v
		}
	}
	return selected
}

// --------------------------- Options ----------------------------

// SubscribeOption configures a subscription.
type SubscribeOption func(*subscribeOptions)

// subscribeOptions represents the options for a subscription.
type subscribeOptions struct {
	columns  []string // The declared columns to carry (nil means all data columns)
	capacity int      // The declared capacity of the delivery queue
	blocking bool     // Whether a slow consumer makes committing transactions wait
}

// WithColumns declares the columns whose before/after values are carried by each
// delivered RowChange. If no columns are declared, all of the data columns are
// carried.
func WithColumns(columns ...string) SubscribeOption {
	return func(o *subscribeOptions) {
		o.columns = append(o.columns, columns...)
	}
}

// WithCapacity declares the capacity of the subscription delivery queue. In
// blocking mode this bounds the number of pending events after which commits
// start waiting for the consumer, otherwise it is only a pre-allocation hint
// and the queue grows as needed.
func WithCapacity(capacity int) SubscribeOption {
	return func(o *subscribeOptions) {
		o.capacity = capacity
	}
}

// WithBlocking declares that when this consumer falls behind and its delivery
// queue is full, committing transactions wait until the consumer catches up.
// Without this option the subscription follows the commit stream independently
// and never blocks the committing transactions.
func WithBlocking() SubscribeOption {
	return func(o *subscribeOptions) {
		o.blocking = true
	}
}

// --------------------------- Subscription ----------------------------

// Subscription represents an active committed row change subscription. Each
// subscription owns an independent delivery queue so that a slow consumer can
// catch up at its own pace without affecting other consumers. Delivered events
// are detached copies and are never backed by internal reusable buffers, hence
// they never change after being delivered.
type Subscription struct {
	owner    *Collection // The owner collection
	columns  []string    // The declared columns (nil means all data columns)
	capacity int         // The declared capacity of the delivery queue
	blocking bool        // Whether commits wait when the queue is full
	lock     sync.Mutex  // The lock guarding the delivery queue
	notEmpty sync.Cond   // Signalled when events are delivered
	notFull  sync.Cond   // Signalled when queue space becomes available
	queue    []RowChange // The pending delivery queue
	closed   bool        // Whether the subscription is closed
}

// Subscribe creates a new committed row change subscription on the collection.
// The returned subscription receives one RowChange per logical row for every
// transaction committed after this call. Changes are merged by logical row and
// carry the before/after values of the declared columns (see WithColumns) as
// well as the commit identity. Rolled back transactions emit no events.
func (c *Collection) Subscribe(opts ...SubscribeOption) (*Subscription, error) {
	options := subscribeOptions{
		capacity: 1024,
	}
	for _, fn := range opts {
		fn(&options)
	}

	// Validate the declared columns, they must exist and be data columns
	for _, name := range options.columns {
		column, ok := c.cols.Load(name)
		if !ok {
			return nil, fmt.Errorf("column: unable to subscribe, column '%s' does not exist", name)
		}
		if _, isComputed := column.Column.(computed); isComputed {
			return nil, fmt.Errorf("column: unable to subscribe, column '%s' is computed", name)
		}
	}

	// In blocking mode the queue must have a strictly positive capacity,
	// otherwise a committing transaction would end up waiting forever.
	if options.blocking && options.capacity < 1 {
		options.capacity = 1
	}

	sub := &Subscription{
		owner:    c,
		columns:  options.columns,
		capacity: options.capacity,
		blocking: options.blocking,
		queue:    make([]RowChange, 0, options.capacity),
	}
	sub.notEmpty.L = &sub.lock
	sub.notFull.L = &sub.lock

	c.subs.add(sub)
	return sub, nil
}

// Next blocks until the next committed row change is available and returns it.
// It returns false once the subscription is closed and fully drained.
func (s *Subscription) Next() (RowChange, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for len(s.queue) == 0 && !s.closed {
		s.notEmpty.Wait()
	}
	return s.pop()
}

// TryNext returns the next committed row change if one is immediately
// available, without waiting.
func (s *Subscription) TryNext() (RowChange, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.pop()
}

// pop removes the head of the delivery queue, must be called under the lock.
func (s *Subscription) pop() (RowChange, bool) {
	if len(s.queue) == 0 {
		return RowChange{}, false
	}

	event := s.queue[0]
	s.queue[0] = RowChange{} // Detach the delivered slot for the GC
	s.queue = s.queue[1:]
	if s.blocking {
		s.notFull.Signal()
	}
	return event, true
}

// Close detaches the subscription from the collection. Events that are still
// pending in the delivery queue remain readable until drained.
func (s *Subscription) Close() error {
	s.owner.subs.remove(s)

	s.lock.Lock()
	defer s.lock.Unlock()
	s.closed = true
	s.notEmpty.Broadcast()
	s.notFull.Broadcast()
	return nil
}

// deliver appends committed changes to the delivery queue. Every subscription
// receives its own copy of each event, filtered down to its declared columns,
// so that delivered records are never shared with, or mutated by, the commit
// path or other consumers. In blocking mode a full queue makes the committing
// transaction wait, as declared, until the consumer catches up.
func (s *Subscription) deliver(events []RowChange) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, event := range events {
		if s.closed {
			return
		}

		// If the consumer fell behind and the queue is full, wait as declared
		// until the consumer catches up and frees up some space.
		for s.blocking && !s.closed && len(s.queue) >= s.capacity {
			s.notFull.Wait()
		}
		if s.closed {
			return
		}

		s.queue = append(s.queue, event.selectFor(s.columns))
	}
	s.notEmpty.Broadcast()
}

// --------------------------- Registry ----------------------------

// subscriptions represents a registry of the active subscriptions of a
// collection. The list is kept in an atomic value and updated using
// copy-on-write so that the commit path never blocks on subscription
// management.
type subscriptions struct {
	lock sync.Mutex   // Guards the registration of subscriptions
	list atomic.Value // Of []*Subscription, copy-on-write
}

// newSubscriptions creates a new subscription registry.
func newSubscriptions() *subscriptions {
	registry := new(subscriptions)
	registry.list.Store([]*Subscription(nil))
	return registry
}

// all returns the currently registered subscriptions.
func (s *subscriptions) all() []*Subscription {
	return s.list.Load().([]*Subscription)
}

// add registers a new subscription.
func (s *subscriptions) add(sub *Subscription) {
	s.lock.Lock()
	defer s.lock.Unlock()

	current := s.all()
	updated := make([]*Subscription, 0, len(current)+1)
	updated = append(updated, current...)
	updated = append(updated, sub)
	s.list.Store(updated)
}

// remove detaches a subscription from the registry.
func (s *subscriptions) remove(sub *Subscription) {
	s.lock.Lock()
	defer s.lock.Unlock()

	current := s.all()
	updated := make([]*Subscription, 0, len(current))
	for _, existing := range current {
		if existing != sub {
			updated = append(updated, existing)
		}
	}
	s.list.Store(updated)
}

// plan builds a capture plan for a commit, or returns nil if there are no
// active subscriptions.
func (s *subscriptions) plan(owner *Collection) *capturePlan {
	subs := s.all()
	if len(subs) == 0 {
		return nil
	}

	// Compute the union of the declared columns. If any of the subscriptions
	// carries all of the columns, the plan captures all of the data columns.
	var declared map[string]struct{}
	for _, sub := range subs {
		if sub.columns == nil {
			declared = nil
			break
		}
		if declared == nil {
			declared = make(map[string]struct{}, 8)
		}
		for _, name := range sub.columns {
			declared[name] = struct{}{}
		}
	}

	plan := &capturePlan{
		reader: commit.NewReader(),
	}

	// Resolve the declared column names to the current columns, skipping the
	// ones which might have been dropped since the subscription was created.
	if declared == nil {
		owner.cols.Range(func(column *column) {
			if _, isComputed := column.Column.(computed); !isComputed {
				plan.columns = append(plan.columns, planColumn{name: column.name, col: column})
			}
		})
		return plan
	}

	for name := range declared {
		if column, ok := owner.cols.Load(name); ok {
			plan.columns = append(plan.columns, planColumn{name: name, col: column})
		}
	}
	return plan
}

// dispatch delivers the committed row changes to all of the active
// subscriptions. It is called once the transaction has been fully committed
// and no chunk locks are being held anymore.
func (s *subscriptions) dispatch(events []RowChange) {
	if len(events) == 0 {
		return
	}

	for _, sub := range s.all() {
		sub.deliver(events)
	}
}

// --------------------------- Capture ----------------------------

// planColumn represents a resolved column to capture.
type planColumn struct {
	name string  // The name of the column
	col  *column // The resolved column
}

// capturePlan represents a per-commit capture plan.
type capturePlan struct {
	columns []planColumn   // The union of the declared columns to capture
	reader  *commit.Reader // The reusable reader for the pending operations
}

// capture accumulates the merged row changes of a single chunk of a commit.
type capture struct {
	commit uint64                 // The identity of the commit
	plan   *capturePlan           // The capture plan
	rows   map[uint32]*captureRow // The merged rows of the chunk
}

// captureRow represents a merged state of a single logical row.
type captureRow struct {
	typ       EventType      // The merged type of the change
	cancelled bool           // Whether the insert and delete cancelled out
	before    map[string]any // The captured before-values
}

// captureBefore merges the pending insert/update/delete operations of a chunk
// by logical row and captures the before-values of the declared columns. It
// must be called before the pending operations are applied to the collection.
func (p *capturePlan) captureBefore(txn *Txn, commitID uint64, chunk commit.Chunk) *capture {
	captured := &capture{
		commit: commitID,
		plan:   p,
		rows:   make(map[uint32]*captureRow, 8),
	}

	// Merge the pending operations of the chunk by logical row
	for _, buffer := range txn.updates {
		if buffer.IsEmpty() {
			continue
		}

		isMarker := buffer.Column == rowColumn
		p.reader.Range(buffer, chunk, func(r *commit.Reader) {
			for r.Next() {
				idx := r.Index()
				row, ok := captured.rows[idx]
				if !ok {
					row = new(captureRow)
					captured.rows[idx] = row
				}

				switch {
				case isMarker && r.Type == commit.Insert:
					row.typ = EventInsert
				case isMarker && r.Type == commit.Delete && row.typ == EventInsert:
					row.typ = 0
					row.cancelled = true // Insert and delete cancel each other out
				case isMarker && r.Type == commit.Delete:
					row.typ = EventDelete
				case !isMarker && row.typ == 0 && !row.cancelled:
					row.typ = EventUpdate
				}
			}
		})
	}

	// Capture the before-values of the declared columns for updated and
	// deleted rows. Inserted rows have no prior state.
	for idx, row := range captured.rows {
		if row.typ == EventUpdate || row.typ == EventDelete {
			row.before = p.valuesAt(idx)
		}
	}
	return captured
}

// finish captures the after-values of the declared columns and appends the
// merged row changes, ordered by row index, to the provided set of events. It
// must be called after the pending operations were applied to the collection.
func (c *capture) finish(events []RowChange) []RowChange {
	if len(c.rows) == 0 {
		return events
	}

	// Order the merged rows by their index for a deterministic output
	ordered := make([]uint32, 0, len(c.rows))
	for idx, row := range c.rows {
		if row.typ != 0 {
			ordered = append(ordered, idx)
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })

	// Capture the after-values and build the merged row changes. Deleted rows
	// have no final state, hence their after-values stay nil.
	for _, idx := range ordered {
		row := c.rows[idx]
		event := RowChange{
			Commit: c.commit,
			Row:    idx,
			Type:   row.typ,
			Before: row.before,
		}
		if row.typ != EventDelete {
			event.After = c.plan.valuesAt(idx)
		}
		events = append(events, event)
	}
	return events
}

// valuesAt reads the current values of the plan columns at a given row index.
// The values are read from the column storage itself, hence they are detached
// from the internal reusable commit buffers.
func (p *capturePlan) valuesAt(idx uint32) map[string]any {
	values := make(map[string]any, len(p.columns))
	for _, column := range p.columns {
		if v, ok := column.col.Value(idx); ok {
			values[column.name] = v
		}
	}
	return values
}
