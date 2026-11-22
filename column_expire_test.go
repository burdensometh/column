// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestIsExpired tests the unified expiration predicate for the edge cases of
// no time-to-live, a deadline exactly at the read timestamp and around it.
func TestIsExpired(t *testing.T) {
	now := time.Now().UnixNano()
	assert.False(t, isExpired(0, now))     // No time-to-live never expires
	assert.False(t, isExpired(now+1, now)) // Deadline in the future
	assert.True(t, isExpired(now, now))    // Deadline exactly at the read time
	assert.True(t, isExpired(now-1, now))  // Deadline in the past
	assert.True(t, isExpired(1, now))      // Long past deadline
}

// TestExpireHiddenFromScans tests that expired rows are not counted in the
// query statistics even if they were not physically reclaimed by the vacuum
// yet, and that With, Count and Range all share the same expiration condition.
func TestExpireHiddenFromScans(t *testing.T) {
	col := NewCollection(Options{Vacuum: time.Hour})
	defer col.Close()
	assert.NoError(t, col.CreateColumn("age", ForInt()))
	assert.NoError(t, col.CreateIndex("adult", "age", func(r Reader) bool {
		return r.Int() >= 18
	}))

	// Insert one expiring, one non-expiring and one far-future row
	insert := func(ttl time.Duration) {
		_, err := col.Insert(func(r Row) error {
			r.SetInt("age", 35)
			r.SetTTL(ttl)
			return nil
		})
		assert.NoError(t, err)
	}
	insert(50 * time.Millisecond) // Expires shortly
	insert(0)                     // Never expires
	insert(time.Hour)             // Expires in the future

	// Wait for the first row to expire, but not for the vacuum to run
	time.Sleep(100 * time.Millisecond)

	// The expired row should be hidden from all of the scans
	assert.NoError(t, col.Query(func(txn *Txn) error {
		assert.Equal(t, 2, txn.Count())
		assert.Equal(t, 2, txn.With("adult").Count())

		count := 0
		assert.NoError(t, txn.Range(func(idx uint32) {
			count++
		}))
		assert.Equal(t, 2, count)
		return nil
	}))

	// The physical count still includes the expired row until the vacuum runs
	assert.Equal(t, 3, col.Count())

	// Once expired, the row is physically reclaimed
	assert.NoError(t, col.expire())
	assert.Equal(t, 2, col.Count())
}

// TestExpireQueryKey tests that a key-based query does not return a row that
// has expired but was not reclaimed by the vacuum yet.
func TestExpireQueryKey(t *testing.T) {
	col := NewCollection(Options{Vacuum: time.Hour})
	defer col.Close()
	assert.NoError(t, col.CreateColumn("key", ForKey()))
	assert.NoError(t, col.CreateColumn("val", ForString()))

	insert := func(key, val string, ttl time.Duration) {
		assert.NoError(t, col.InsertKey(key, func(r Row) error {
			r.SetString("val", val)
			r.SetTTL(ttl)
			return nil
		}))
	}
	insert("expired", "a", 50*time.Millisecond)
	insert("renewable", "b", 50*time.Millisecond)
	insert("live", "c", time.Hour)
	insert("no-ttl", "d", 0)

	// Wait for the first two keys to expire
	time.Sleep(100 * time.Millisecond)

	// Renew one of the expired keys with an upsert, it should be visible again
	assert.NoError(t, col.UpsertKey("renewable", func(r Row) error {
		r.SetTTL(time.Hour)
		return nil
	}))

	// The expired key should not be found, even though it was not reclaimed yet
	assert.Error(t, col.QueryKey("expired", func(r Row) error { return nil }))

	// The renewed, live and timeless keys should still be found
	for _, key := range []string{"renewable", "live", "no-ttl"} {
		assert.NoError(t, col.QueryKey(key, func(r Row) error {
			_, ok := r.String("val")
			assert.True(t, ok)
			return nil
		}))
	}
}

// TestExpireRevalidatesRenewed tests that a delete issued by the vacuum does
// not reclaim a row whose time-to-live was renewed after the scan.
func TestExpireRevalidatesRenewed(t *testing.T) {
	col := NewCollection(Options{Vacuum: time.Hour})
	defer col.Close()
	assert.NoError(t, col.CreateColumn("name", ForString()))

	// Insert a row with a short time-to-live and let it expire
	idx, err := col.Insert(func(r Row) error {
		r.SetString("name", "Roman")
		r.SetTTL(50 * time.Millisecond)
		return nil
	})
	assert.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// Renew the row, extending its time-to-live beyond the read timestamp
	assert.NoError(t, col.QueryAt(idx, func(r Row) error {
		r.SetTTL(time.Hour)
		return nil
	}))

	// Queue a conditional expiration delete, it should be skipped at commit
	// time since the row was renewed and is not expired anymore
	assert.NoError(t, col.Query(func(txn *Txn) error {
		txn.deleteExpired(idx)
		return nil
	}))
	assert.Equal(t, 1, col.Count())

	// Let the row expire for real this time and delete it
	assert.NoError(t, col.QueryAt(idx, func(r Row) error {
		r.SetTTL(50 * time.Millisecond)
		return nil
	}))
	time.Sleep(100 * time.Millisecond)
	assert.NoError(t, col.Query(func(txn *Txn) error {
		txn.deleteExpired(idx)
		return nil
	}))
	assert.Equal(t, 0, col.Count())
}

// TestExpireKeepsClearedTTL tests that a row whose time-to-live was cleared
// after it had expired is not reclaimed by the vacuum.
func TestExpireKeepsClearedTTL(t *testing.T) {
	col := NewCollection(Options{Vacuum: time.Hour})
	defer col.Close()

	// Insert a row with a short time-to-live and let it expire
	idx, err := col.Insert(func(r Row) error {
		r.SetTTL(50 * time.Millisecond)
		return nil
	})
	assert.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// Clear the time-to-live, the row should never expire anymore
	assert.NoError(t, col.QueryAt(idx, func(r Row) error {
		r.SetTTL(0)
		return nil
	}))

	// The vacuum should keep the row, both logically and physically
	assert.NoError(t, col.expire())
	assert.Equal(t, 1, col.Count())
	assert.NoError(t, col.Query(func(txn *Txn) error {
		assert.Equal(t, 1, txn.Count())
		return nil
	}))
}

// TestExpireVacuumEndToEnd tests the periodic cleanup of the expired rows,
// keeping the non-expired ones intact.
func TestExpireVacuumEndToEnd(t *testing.T) {
	col := NewCollection(Options{Vacuum: time.Hour})
	defer col.Close()

	insert := func(ttl time.Duration) {
		_, err := col.Insert(func(r Row) error {
			r.SetTTL(ttl)
			return nil
		})
		assert.NoError(t, err)
	}
	insert(50 * time.Millisecond) // Expires shortly
	insert(time.Hour)             // Expires in the future
	insert(0)                     // Never expires

	// Wait for the first row to expire and run the vacuum manually
	time.Sleep(100 * time.Millisecond)
	assert.NoError(t, col.expire())

	// Only the expired row should be physically reclaimed
	assert.Equal(t, 2, col.Count())
	assert.NoError(t, col.Query(func(txn *Txn) error {
		assert.Equal(t, 2, txn.Count())
		return nil
	}))
}

// TestExpireRestoredRows tests that rows which have already expired are
// invisible to queries right after a restore, and can be reclaimed by the
// vacuum as usual.
func TestExpireRestoredRows(t *testing.T) {
	input := NewCollection(Options{Vacuum: time.Hour})
	defer input.Close()
	assert.NoError(t, input.CreateColumn("name", ForString()))

	insert := func(name string, ttl time.Duration) {
		_, err := input.Insert(func(r Row) error {
			r.SetString("name", name)
			r.SetTTL(ttl)
			return nil
		})
		assert.NoError(t, err)
	}
	insert("expired", 50*time.Millisecond)
	insert("live", time.Hour)
	insert("no-ttl", 0)

	// Wait for the first row to expire and take a snapshot
	time.Sleep(100 * time.Millisecond)
	buffer := bytes.NewBuffer(nil)
	assert.NoError(t, input.Snapshot(buffer))

	// Restore the snapshot into a new collection
	output := NewCollection(Options{Vacuum: time.Hour})
	defer output.Close()
	assert.NoError(t, output.CreateColumn("name", ForString()))
	assert.NoError(t, output.Restore(buffer))

	// The expired row should be hidden from the scans right after the restore
	assert.NoError(t, output.Query(func(txn *Txn) error {
		assert.Equal(t, 2, txn.Count())
		return nil
	}))

	// The vacuum should be able to reclaim the restored row physically
	assert.Equal(t, 3, output.Count())
	assert.NoError(t, output.expire())
	assert.Equal(t, 2, output.Count())
}

// TestRowTTLNotSet tests that a row without a time-to-live (or with an
// explicitly cleared one) reports no remaining time-to-live.
func TestRowTTLNotSet(t *testing.T) {
	col := NewCollection()
	defer col.Close()

	// Insert a row and explicitly clear its time-to-live
	idx, err := col.Insert(func(r Row) error {
		r.SetTTL(0)
		return nil
	})
	assert.NoError(t, err)

	// The row should report no expiration
	assert.NoError(t, col.QueryAt(idx, func(r Row) error {
		_, ok := r.TTL()
		assert.False(t, ok)
		return nil
	}))
}
