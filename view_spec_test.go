// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"bytes"
	"errors"
	"testing"

	"github.com/kelindar/column/commit"
	"github.com/stretchr/testify/assert"
)

// These tests cover the operational requirements of the materialized group
// views: group migration, value corrections, rollback isolation, replay
// idempotence and rebuilding the aggregates after a snapshot restore. The
// helper names are prefixed to avoid clashing with other view tests.

func gvCollection(opts ...Options) *Collection {
	c := NewCollection(opts...)
	c.CreateColumn("team", ForString())
	c.CreateColumn("amount", ForInt64())
	return c
}

func gvTeamKey(r Row) string {
	team, _ := r.String("team")
	return team
}

func TestGroupViewMovesContribution(t *testing.T) {
	col := gvCollection()
	view, err := col.CreateView("totals", gvTeamKey, WithViewSum("amount"))
	assert.NoError(t, err)

	idx, err := col.Insert(func(r Row) error {
		r.SetString("team", "red")
		r.SetInt64("amount", 10)
		return nil
	})
	assert.NoError(t, err)

	red, ok := view.Get("red")
	assert.True(t, ok)
	assert.Equal(t, Aggregate{Count: 1, Sum: 10}, red)

	// Move the row to another group: the old group must be revoked and the new
	// group must receive the contribution, never both.
	assert.NoError(t, col.QueryAt(idx, func(r Row) error {
		r.SetString("team", "blue")
		return nil
	}))

	_, ok = view.Get("red")
	assert.False(t, ok, "the old group must be revoked")

	blue, ok := view.Get("blue")
	assert.True(t, ok)
	assert.Equal(t, Aggregate{Count: 1, Sum: 10}, blue)
	assert.Equal(t, 1, view.Len())
}

func TestGroupViewCorrectsAmount(t *testing.T) {
	col := gvCollection()
	view, err := col.CreateView("totals", gvTeamKey, WithViewSum("amount"))
	assert.NoError(t, err)

	idx, _ := col.Insert(func(r Row) error {
		r.SetString("team", "red")
		r.SetInt64("amount", 10)
		return nil
	})

	// Set a corrected amount, then merge a delta into it.
	assert.NoError(t, col.QueryAt(idx, func(r Row) error {
		r.SetInt64("amount", 25)
		return nil
	}))
	red, _ := view.Get("red")
	assert.Equal(t, Aggregate{Count: 1, Sum: 25}, red)

	assert.NoError(t, col.QueryAt(idx, func(r Row) error {
		r.MergeInt64("amount", 5)
		return nil
	}))
	red, _ = view.Get("red")
	assert.Equal(t, Aggregate{Count: 1, Sum: 30}, red)

	// Deleting the row revokes its contribution entirely.
	assert.True(t, col.DeleteAt(idx))
	_, ok := view.Get("red")
	assert.False(t, ok)
	assert.Equal(t, 0, view.Len())
}

func TestGroupViewRollbackIsolated(t *testing.T) {
	col := gvCollection()
	view, err := col.CreateView("totals", gvTeamKey, WithViewSum("amount"))
	assert.NoError(t, err)

	idx, _ := col.Insert(func(r Row) error {
		r.SetString("team", "red")
		r.SetInt64("amount", 10)
		return nil
	})

	before, _ := view.Get("red")
	err = col.Query(func(txn *Txn) error {
		txn.QueryAt(idx, func(r Row) error {
			r.SetInt64("amount", 1000)
			r.SetString("team", "blue")
			return nil
		})
		txn.DeleteAt(idx)
		txn.Insert(func(r Row) error {
			r.SetString("team", "green")
			r.SetInt64("amount", 7)
			return nil
		})
		return errors.New("boom")
	})
	assert.Error(t, err)

	after, ok := view.Get("red")
	assert.True(t, ok)
	assert.Equal(t, before, after)
	_, ok = view.Get("blue")
	assert.False(t, ok)
	_, ok = view.Get("green")
	assert.False(t, ok)
	assert.Equal(t, 1, view.Len())
}

func TestGroupViewReplayIdempotent(t *testing.T) {
	stream := make(commit.Channel, 16)
	source := gvCollection(Options{Writer: stream})
	target := gvCollection()
	view, err := target.CreateView("totals", gvTeamKey, WithViewSum("amount"))
	assert.NoError(t, err)

	_, err = source.Insert(func(r Row) error {
		r.SetString("team", "red")
		r.SetInt64("amount", 10)
		return nil
	})
	assert.NoError(t, err)

	change := <-stream
	assert.NotEmpty(t, change.Updates)

	// Replaying the very same commit multiple times must not double-count:
	// each replay reports the same post-commit state and the view diffs it.
	assert.NoError(t, target.Replay(change.Clone()))
	assert.NoError(t, target.Replay(change.Clone()))

	red, ok := view.Get("red")
	assert.True(t, ok)
	assert.Equal(t, Aggregate{Count: 1, Sum: 10}, red)

	// The main table and the view agree at the same read boundary.
	target.Query(func(txn *Txn) error {
		assert.Equal(t, 1, txn.Count())
		return nil
	})
}

func TestGroupViewRebuildAfterRestore(t *testing.T) {
	col := gvCollection()
	col.Insert(func(r Row) error {
		r.SetString("team", "red")
		r.SetInt64("amount", 10)
		return nil
	})
	col.Insert(func(r Row) error {
		r.SetString("team", "red")
		r.SetInt64("amount", 5)
		return nil
	})

	// Views are transient, just like sorted indexes: they are not written into
	// the snapshot and must be recreated from the real columns after restore.
	var buf bytes.Buffer
	assert.NoError(t, col.Snapshot(&buf))

	restored := gvCollection()
	assert.NoError(t, restored.Restore(&buf))

	view, err := restored.CreateView("totals", gvTeamKey, WithViewSum("amount"))
	assert.NoError(t, err)

	red, ok := view.Get("red")
	assert.True(t, ok)
	assert.Equal(t, Aggregate{Count: 2, Sum: 15}, red)
}
