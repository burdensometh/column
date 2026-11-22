// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newSubscribed creates a collection with a few columns for subscription tests.
func newSubscribed(t *testing.T) *Collection {
	t.Helper()
	col := NewCollection()
	require.NoError(t, col.CreateColumn("name", ForString()))
	require.NoError(t, col.CreateColumn("balance", ForFloat64()))
	require.NoError(t, col.CreateColumn("age", ForInt()))
	return col
}

// insertPlayer inserts a single row with all of the columns set.
func insertPlayer(t *testing.T, col *Collection, name string, balance float64, age int) uint32 {
	t.Helper()
	idx, err := col.Insert(func(r Row) error {
		r.SetString("name", name)
		r.SetFloat64("balance", balance)
		r.SetInt("age", age)
		return nil
	})
	require.NoError(t, err)
	return idx
}

func TestSubscribeInsert(t *testing.T) {
	col := newSubscribed(t)
	defer col.Close()

	sub, err := col.Subscribe(WithColumns("name", "balance"))
	require.NoError(t, err)
	defer sub.Close()

	// Insert a single row and expect a single insert event
	idx := insertPlayer(t, col, "merlin", 99.5, 107)
	event, ok := sub.Next()
	require.True(t, ok)
	assert.Equal(t, EventInsert, event.Type)
	assert.Equal(t, idx, event.Row)
	assert.NotZero(t, event.Commit)
	assert.Nil(t, event.Before)
	assert.Equal(t, map[string]any{
		"name":    "merlin",
		"balance": 99.5,
	}, event.After)

	// No more events should be pending
	_, ok = sub.TryNext()
	assert.False(t, ok)
}

func TestSubscribeUpdateMultipleFields(t *testing.T) {
	col := newSubscribed(t)
	defer col.Close()

	idx := insertPlayer(t, col, "merlin", 99.5, 107)

	sub, err := col.Subscribe(WithColumns("name", "balance", "age"))
	require.NoError(t, err)
	defer sub.Close()

	// Update multiple fields of the same row in a single transaction
	require.NoError(t, col.QueryAt(idx, func(r Row) error {
		r.SetString("name", "gandalf")
		r.SetFloat64("balance", 42.0)
		r.SetInt("age", 200)
		return nil
	}))

	// The consumer must receive a single, complete old/new event for the row
	event, ok := sub.Next()
	require.True(t, ok)
	assert.Equal(t, EventUpdate, event.Type)
	assert.Equal(t, idx, event.Row)
	assert.Equal(t, map[string]any{
		"name":    "merlin",
		"balance": 99.5,
		"age":     107,
	}, event.Before)
	assert.Equal(t, map[string]any{
		"name":    "gandalf",
		"balance": 42.0,
		"age":     200,
	}, event.After)

	// The multi-field update must be merged into one event only
	_, ok = sub.TryNext()
	assert.False(t, ok)
}

func TestSubscribeDelete(t *testing.T) {
	col := newSubscribed(t)
	defer col.Close()

	idx := insertPlayer(t, col, "merlin", 99.5, 107)

	sub, err := col.Subscribe(WithColumns("name", "balance"))
	require.NoError(t, err)
	defer sub.Close()

	// Delete the row and expect a delete event with the prior values
	assert.True(t, col.DeleteAt(idx))
	event, ok := sub.Next()
	require.True(t, ok)
	assert.Equal(t, EventDelete, event.Type)
	assert.Equal(t, idx, event.Row)
	assert.Equal(t, map[string]any{
		"name":    "merlin",
		"balance": 99.5,
	}, event.Before)
	assert.Nil(t, event.After)
}

func TestSubscribeRollback(t *testing.T) {
	col := newSubscribed(t)
	defer col.Close()

	sub, err := col.Subscribe()
	require.NoError(t, err)
	defer sub.Close()

	// A transaction which fails must be rolled back and emit no events
	require.Error(t, col.Query(func(txn *Txn) error {
		_, err := txn.Insert(func(r Row) error {
			r.SetString("name", "ghost")
			return nil
		})
		require.NoError(t, err)
		return errors.New("boom")
	}))

	// A rolled back update must emit no events either
	idx := insertPlayer(t, col, "merlin", 99.5, 107)
	_, ok := sub.Next() // Drain the insert event
	require.True(t, ok)

	require.Error(t, col.QueryAt(idx, func(r Row) error {
		r.SetString("name", "gandalf")
		return errors.New("boom")
	}))

	_, ok = sub.TryNext()
	assert.False(t, ok)
}

func TestSubscribeMergeCoalesce(t *testing.T) {
	col := newSubscribed(t)
	defer col.Close()

	idx := insertPlayer(t, col, "merlin", 100.0, 107)

	sub, err := col.Subscribe(WithColumns("balance"))
	require.NoError(t, err)
	defer sub.Close()

	// Merge the same field twice within a single transaction
	require.NoError(t, col.QueryAt(idx, func(r Row) error {
		r.MergeFloat64("balance", 50.0)
		r.MergeFloat64("balance", 25.0)
		return nil
	}))

	// The consumer must see a single event with the final merged value only
	event, ok := sub.Next()
	require.True(t, ok)
	assert.Equal(t, EventUpdate, event.Type)
	assert.Equal(t, map[string]any{"balance": 100.0}, event.Before)
	assert.Equal(t, map[string]any{"balance": 175.0}, event.After)

	_, ok = sub.TryNext()
	assert.False(t, ok)
}

func TestSubscribeInsertDeleteSameTxn(t *testing.T) {
	col := newSubscribed(t)
	defer col.Close()

	sub, err := col.Subscribe()
	require.NoError(t, err)
	defer sub.Close()

	// Insert and delete the same row within a single transaction, the two
	// operations cancel each other out and no event should be emitted
	require.NoError(t, col.Query(func(txn *Txn) error {
		idx, err := txn.Insert(func(r Row) error {
			r.SetString("name", "ghost")
			return nil
		})
		require.NoError(t, err)
		txn.DeleteAt(idx)
		return nil
	}))

	_, ok := sub.TryNext()
	assert.False(t, ok)
}

func TestSubscribeCommitIdentity(t *testing.T) {
	col := newSubscribed(t)
	defer col.Close()

	sub, err := col.Subscribe(WithColumns("name"))
	require.NoError(t, err)
	defer sub.Close()

	// Two rows changed in the same commit must share the same commit identity
	require.NoError(t, col.Query(func(txn *Txn) error {
		for _, name := range []string{"a", "b"} {
			_, err := txn.Insert(func(r Row) error {
				r.SetString("name", name)
				return nil
			})
			require.NoError(t, err)
		}
		return nil
	}))

	first, ok := sub.Next()
	require.True(t, ok)
	second, ok := sub.Next()
	require.True(t, ok)
	assert.NotZero(t, first.Commit)
	assert.Equal(t, first.Commit, second.Commit)

	// A subsequent commit must carry a greater commit identity
	insertPlayer(t, col, "c", 0, 0)
	third, ok := sub.Next()
	require.True(t, ok)
	assert.Greater(t, third.Commit, first.Commit)
}

func TestSubscribeDeclaredColumns(t *testing.T) {
	col := newSubscribed(t)
	defer col.Close()

	sub, err := col.Subscribe(WithColumns("balance"))
	require.NoError(t, err)
	defer sub.Close()

	// Only the declared columns should be carried by the event
	insertPlayer(t, col, "merlin", 99.5, 107)
	event, ok := sub.Next()
	require.True(t, ok)
	assert.Equal(t, map[string]any{"balance": 99.5}, event.After)
	assert.NotContains(t, event.After, "name")
	assert.NotContains(t, event.After, "age")
}

func TestSubscribeInvalidColumn(t *testing.T) {
	col := newSubscribed(t)
	defer col.Close()
	require.NoError(t, col.CreateIndex("rich", "balance", func(r Reader) bool {
		return r.Float() > 100
	}))

	// Subscribing to a missing or a computed column must fail
	_, err := col.Subscribe(WithColumns("missing"))
	assert.Error(t, err)

	_, err = col.Subscribe(WithColumns("rich"))
	assert.Error(t, err)
}

func TestSubscribeBlockingWaits(t *testing.T) {
	col := newSubscribed(t)
	defer col.Close()

	sub, err := col.Subscribe(WithColumns("name"), WithCapacity(1), WithBlocking())
	require.NoError(t, err)
	defer sub.Close()

	// Fill the delivery queue (capacity of 1)
	insertPlayer(t, col, "first", 0, 0)

	// The next commit must wait, as declared, since the consumer is behind
	done := make(chan struct{})
	go func() {
		defer close(done)
		insertPlayer(t, col, "second", 0, 0)
	}()

	select {
	case <-done:
		t.Fatal("commit should wait for the slow consumer")
	case <-time.After(100 * time.Millisecond):
	}

	// Consume one event, which must unblock the pending commit
	event, ok := sub.Next()
	require.True(t, ok)
	assert.Equal(t, "first", event.After["name"])

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("commit should have proceeded after the consumer caught up")
	}

	// The second event must have been delivered
	event, ok = sub.Next()
	require.True(t, ok)
	assert.Equal(t, "second", event.After["name"])
}

func TestSubscribeIndependentFollowRead(t *testing.T) {
	col := newSubscribed(t)
	defer col.Close()

	// A tiny capacity hint, but a non-blocking subscription must follow the
	// commit stream independently and never block the committing transactions
	sub, err := col.Subscribe(WithColumns("name"), WithCapacity(1))
	require.NoError(t, err)
	defer sub.Close()

	const count = 500
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < count; i++ {
			insertPlayer(t, col, fmt.Sprintf("player-%d", i), 0, 0)
		}
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("commits must not wait for a slow independent consumer")
	}

	// Catch up independently and verify that every event was retained in order
	for i := 0; i < count; i++ {
		event, ok := sub.Next()
		require.True(t, ok)
		assert.Equal(t, EventInsert, event.Type)
		assert.Equal(t, fmt.Sprintf("player-%d", i), event.After["name"])
		assert.Equal(t, uint32(i), event.Row)
	}
}

func TestSubscribeDeliveredEventsImmutable(t *testing.T) {
	col := newSubscribed(t)
	defer col.Close()

	sub, err := col.Subscribe(WithColumns("name", "balance"))
	require.NoError(t, err)
	defer sub.Close()

	// Deliver a first event to the consumer
	idx := insertPlayer(t, col, "first", 1.0, 10)
	event, ok := sub.Next()
	require.True(t, ok)
	require.Equal(t, "first", event.After["name"])

	// Hammer the collection with many more commits, forcing the internal
	// reusable commit buffers to be recycled many times over
	for i := 0; i < 500; i++ {
		insertPlayer(t, col, fmt.Sprintf("other-%d", i), float64(i), i)
		require.NoError(t, col.QueryAt(idx, func(r Row) error {
			r.SetString("name", fmt.Sprintf("renamed-%d", i))
			r.SetFloat64("balance", float64(i))
			return nil
		}))
	}

	// The already delivered event must not have changed
	assert.Equal(t, EventInsert, event.Type)
	assert.Equal(t, idx, event.Row)
	assert.Nil(t, event.Before)
	assert.Equal(t, map[string]any{
		"name":    "first",
		"balance": 1.0,
	}, event.After)
}

func TestSubscribeConsumersIndependent(t *testing.T) {
	col := newSubscribed(t)
	defer col.Close()

	sub1, err := col.Subscribe(WithColumns("name"))
	require.NoError(t, err)
	defer sub1.Close()

	sub2, err := col.Subscribe(WithColumns("name"))
	require.NoError(t, err)
	defer sub2.Close()

	insertPlayer(t, col, "merlin", 99.5, 107)

	// Both consumers receive the event
	event1, ok := sub1.Next()
	require.True(t, ok)
	event2, ok := sub2.Next()
	require.True(t, ok)

	// Mutating the event of one consumer must not affect the other one, the
	// delivered records are independent copies
	event1.After["name"] = "mutated"
	assert.Equal(t, "merlin", event2.After["name"])
}

func TestSubscribeClose(t *testing.T) {
	col := newSubscribed(t)
	defer col.Close()

	sub, err := col.Subscribe(WithColumns("name"))
	require.NoError(t, err)

	insertPlayer(t, col, "first", 0, 0)
	require.NoError(t, sub.Close())

	// Pending events remain readable after close
	_, ok := sub.Next()
	assert.True(t, ok)

	// Once drained and closed, Next reports the end of the stream
	_, ok = sub.Next()
	assert.False(t, ok)

	// Commits after the close are not delivered anymore
	insertPlayer(t, col, "second", 0, 0)
	_, ok = sub.TryNext()
	assert.False(t, ok)
}

func TestSubscribeAllColumnsByDefault(t *testing.T) {
	col := newSubscribed(t)
	defer col.Close()

	sub, err := col.Subscribe()
	require.NoError(t, err)
	defer sub.Close()

	// Without a declaration, all of the data columns should be carried
	insertPlayer(t, col, "merlin", 99.5, 107)
	event, ok := sub.Next()
	require.True(t, ok)
	assert.Equal(t, "merlin", event.After["name"])
	assert.Equal(t, 99.5, event.After["balance"])
	assert.Equal(t, 107, event.After["age"])
}

func TestSubscribeEventTypeString(t *testing.T) {
	assert.Equal(t, "insert", EventInsert.String())
	assert.Equal(t, "update", EventUpdate.String())
	assert.Equal(t, "delete", EventDelete.String())
	assert.Equal(t, "unknown", EventType(0).String())
}

func TestSubscribePrimaryKey(t *testing.T) {
	col := NewCollection()
	defer col.Close()
	require.NoError(t, col.CreateColumn("name", ForKey()))
	require.NoError(t, col.CreateColumn("balance", ForFloat64()))

	sub, err := col.Subscribe()
	require.NoError(t, err)
	defer sub.Close()

	// Insert by primary key should emit an insert event carrying the key
	require.NoError(t, col.InsertKey("merlin", func(r Row) error {
		r.SetFloat64("balance", 99.5)
		return nil
	}))

	event, ok := sub.Next()
	require.True(t, ok)
	assert.Equal(t, EventInsert, event.Type)
	assert.Equal(t, "merlin", event.After["name"])
	assert.Equal(t, 99.5, event.After["balance"])

	// Delete by primary key should emit a delete event carrying the prior key
	require.NoError(t, col.DeleteKey("merlin"))
	event, ok = sub.Next()
	require.True(t, ok)
	assert.Equal(t, EventDelete, event.Type)
	assert.Equal(t, "merlin", event.Before["name"])
	assert.Nil(t, event.After)
}

func TestSubscribeConcurrent(t *testing.T) {
	col := newSubscribed(t)
	defer col.Close()

	sub, err := col.Subscribe(WithColumns("balance"))
	require.NoError(t, err)
	defer sub.Close()

	// Concurrent writers committing inserts, updates and deletes
	const writers = 8
	const perWriter = 50
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				idx, err := col.Insert(func(r Row) error {
					r.SetFloat64("balance", float64(base*perWriter+i))
					return nil
				})
				assert.NoError(t, err)
				assert.NoError(t, col.QueryAt(idx, func(r Row) error {
					r.MergeFloat64("balance", 1)
					return nil
				}))
				assert.True(t, col.DeleteAt(idx))
			}
		}(w)
	}
	wg.Wait()

	// Every committed operation must have produced exactly one event per row
	counts := map[EventType]int{}
	for i := 0; i < writers*perWriter*3; i++ {
		event, ok := sub.Next()
		require.True(t, ok)
		counts[event.Type]++
	}
	assert.Equal(t, writers*perWriter, counts[EventInsert])
	assert.Equal(t, writers*perWriter, counts[EventUpdate])
	assert.Equal(t, writers*perWriter, counts[EventDelete])

	_, ok := sub.TryNext()
	assert.False(t, ok)
}
