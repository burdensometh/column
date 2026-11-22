// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"context"
	"time"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)

// --------------------------- Expiration (Vacuum) ----------------------------

// vacuum cleans up the expired objects on a specified interval.
func (c *Collection) vacuum(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
			c.expire()
		}
	}
}

// expire scans the collection for rows that have expired as of the read
// timestamp and physically deletes them. Every delete is re-validated at
// commit time, so rows whose time-to-live was renewed after the scan are
// not reclaimed based on a stale read.
func (c *Collection) expire() error {
	return c.Query(func(txn *Txn) error {
		txn.initialize()
		txn.expired.Range(func(idx uint32) {
			txn.deleteExpired(idx)
		})
		return nil
	})
}

// --------------------------- Expiration (Filter) ----------------------------

// isExpired reports whether the given expiration time (unix nano) is expired
// as of the given read timestamp. A zero expiration time never expires, while
// an expiration exactly at the read timestamp is considered expired.
func isExpired(expireAt, now int64) bool {
	return expireAt != 0 && expireAt <= now
}

// filterExpired removes the rows that have expired as of the transaction's
// read timestamp from the query index and collects them into the expired set.
func (txn *Txn) filterExpired() {
	expire, ok := txn.columnAt(expireColumn)
	if !ok {
		return
	}

	numeric, ok := expire.Column.(Numeric)
	if !ok {
		return
	}

	// Filter the query index, keeping the expired rows aside
	now := txn.now
	txn.rangeRead(func(chunk commit.Chunk, index bitmap.Bitmap) {
		expire.lock.RLock()
		defer expire.lock.RUnlock()

		if numeric.Index(chunk).Count() == 0 {
			return // No row in this chunk has an expiration set
		}

		offset := chunk.Min()
		index.Filter(func(x uint32) bool {
			if expireAt, ok := numeric.LoadInt64(offset + x); ok && isExpired(expireAt, now) {
				txn.expired.Set(offset + x)
				return false
			}
			return true
		})
	})
}

// expiredAt reports whether the row at the given index has expired as of the
// given read timestamp. Rows without a time-to-live never expire.
func (txn *Txn) expiredAt(idx uint32, now int64) bool {
	expire, ok := txn.columnAt(expireColumn)
	if !ok {
		return false
	}

	numeric, ok := expire.Column.(Numeric)
	if !ok {
		return false
	}

	expire.lock.RLock()
	expireAt, ok := numeric.LoadInt64(idx)
	expire.lock.RUnlock()
	return ok && isExpired(expireAt, now)
}

// deleteExpired marks the row at the given index for deletion. The delete is
// re-validated at commit time and only applied if the row is still expired.
func (txn *Txn) deleteExpired(idx uint32) {
	txn.revalidate = true
	txn.deleteAt(idx)
}

// --------------------------- Expiration (Column) ----------------------------

// TTL returns a read-write accessor for the time-to-live column
func (txn *Txn) TTL() rwTTL {
	return rwTTL{
		rw: rwInt64{
			rdNumber: readNumberOf[int64](txn, expireColumn),
			writer:   txn.bufferFor(expireColumn),
		},
	}
}

type rwTTL struct {
	rw rwInt64
}

// TTL returns the remaining time-to-live duration
func (s rwTTL) TTL() (time.Duration, bool) {
	if expireAt, ok := s.rw.Get(); ok && expireAt != 0 {
		return readTTL(expireAt), true
	}
	return 0, false
}

// ExpiresAt returns the expiration time
func (s rwTTL) ExpiresAt() (time.Time, bool) {
	if expireAt, ok := s.rw.Get(); ok && expireAt != 0 {
		return time.Unix(0, expireAt), true
	}
	return time.Time{}, false
}

// Set sets the time-to-live value at the current transaction cursor
func (s rwTTL) Set(ttl time.Duration) {
	s.rw.Set(writeTTL(ttl))
}

// Extend extends time-to-live of the row current transaction cursor by a specified amount
func (s rwTTL) Extend(delta time.Duration) {
	s.rw.Merge(int64(delta.Nanoseconds()))
}

// readTTL converts expiration to a TTL
func readTTL(expireAt int64) time.Duration {
	return time.Unix(0, expireAt).Sub(time.Now())
}

// writeTTL converts ttl to expireAt
func writeTTL(ttl time.Duration) int64 {
	if ttl > 0 {
		return time.Now().Add(ttl).UnixNano()
	}
	return 0
}

// --------------------------- Expiration (Row) ----------------------------

// TTL retrieves the time left before the row will be cleaned up
func (r Row) TTL() (time.Duration, bool) {
	if expireAt, ok := r.Int64(expireColumn); ok && expireAt != 0 {
		return readTTL(expireAt), true
	}
	return 0, false
}

// SetTTL sets a time-to-live for a row and returns the expiration time
func (r Row) SetTTL(ttl time.Duration) (until time.Time) {
	var nanos int64
	if ttl > 0 {
		until = time.Now().Add(ttl)
		nanos = until.UnixNano()
	}

	// Otherwise, return zero time (never expires)
	r.SetInt64(expireColumn, nanos)
	return
}
