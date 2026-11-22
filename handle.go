// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"errors"
	"fmt"
)

// ErrInvalidHandle is returned when a row handle does not match the current
// generation of its slot. This means the row it referred to was deleted (or
// its transaction was rolled back) and its slot may have been re-used by a
// different row since.
var ErrInvalidHandle = errors.New("column: handle is invalid or refers to a deleted row")

// initialGeneration is the generation of a slot that was never occupied. It
// starts at one so that the zero Handle is never valid.
const initialGeneration = 1

// Handle is a versioned reference to a row in the collection. It packs the
// row index together with the generation of the slot at the time the row was
// committed, allowing external systems to safely locate rows across batch
// loads that delete old rows and re-use their slots.
//
// A handle is only valid while its row exists and its slot generation is
// unchanged. Once the row is deleted, the slot generation is incremented and
// the handle is permanently invalidated - it will never accidentally match a
// different row that later re-uses the same slot. The zero Handle is invalid.
type Handle uint64

// makeHandle packs a row index and a slot generation into a handle.
func makeHandle(idx, generation uint32) Handle {
	return Handle(uint64(generation)<<32 | uint64(idx))
}

// Index returns the row index part of the handle.
func (h Handle) Index() uint32 {
	return uint32(h)
}

// Generation returns the slot generation part of the handle.
func (h Handle) Generation() uint32 {
	return uint32(h >> 32)
}

// String returns a string representation of the handle.
func (h Handle) String() string {
	return fmt.Sprintf("%d:%d", h.Index(), h.Generation())
}

// --------------------------- Collection ----------------------------

// InsertHandle executes a mutable cursor transactionally at a new offset and,
// once committed, returns a versioned handle for the inserted row. If the
// transaction is rolled back, no handle is published.
func (c *Collection) InsertHandle(fn func(Row) error) (handle Handle, err error) {
	err = c.Query(func(txn *Txn) (innerErr error) {
		handle, innerErr = txn.InsertHandle(fn)
		return
	})
	if err != nil {
		return 0, err
	}
	return handle, nil
}

// HandleAt returns the versioned handle for the row currently occupying the
// given index. If no row exists at the index, it returns false.
func (c *Collection) HandleAt(idx uint32) (handle Handle, ok bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if !c.fill.Contains(idx) || int(idx) >= len(c.gens) {
		return 0, false
	}
	return makeHandle(idx, c.gens[idx]), true
}

// QueryHandle jumps at the row referenced by the handle and executes the
// given callback, but only if the handle's generation still matches the slot.
// If the handle is stale (the row was deleted or never committed), it returns
// ErrInvalidHandle and the callback is not executed.
func (c *Collection) QueryHandle(handle Handle, fn func(Row) error) error {
	return c.Query(func(txn *Txn) error {
		return txn.QueryHandle(handle, fn)
	})
}

// DeleteHandle deletes the row referenced by the handle, but only if the
// handle's generation still matches the slot. It returns true if the row was
// deleted and false if the handle was stale.
func (c *Collection) DeleteHandle(handle Handle) (deleted bool) {
	c.Query(func(txn *Txn) error {
		deleted = txn.DeleteHandle(handle)
		return nil
	})
	return
}

// handleOf mints a handle for a row index, if the row currently exists.
func (c *Collection) handleOf(idx uint32) Handle {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if int(idx) >= len(c.gens) || !c.fill.Contains(idx) {
		return 0
	}
	return makeHandle(idx, c.gens[idx])
}

// validHandle checks whether the handle still refers to the row currently
// occupying its slot and returns the row index if it does.
func (c *Collection) validHandle(handle Handle) (uint32, bool) {
	idx := handle.Index()
	c.lock.RLock()
	defer c.lock.RUnlock()
	if int(idx) >= len(c.gens) || !c.fill.Contains(idx) || c.gens[idx] != handle.Generation() {
		return idx, false
	}
	return idx, true
}

// growGensLocked grows the generation vector to cover the given index. The
// caller must hold the collection lock.
func (c *Collection) growGensLocked(idx uint32) {
	for uint32(len(c.gens)) <= idx {
		c.gens = append(c.gens, initialGeneration)
	}
}

// bumpGenLocked increments the generation of a slot, invalidating all of the
// handles previously issued for it. The caller must hold the collection lock.
func (c *Collection) bumpGenLocked(idx uint32) {
	c.growGensLocked(idx)
	c.gens[idx]++
}

// freeLocked marks the index as free and bumps its generation. The caller
// must hold the collection lock.
func (c *Collection) freeLocked(idx uint32) {
	if !c.fill.Contains(idx) {
		return
	}
	c.fill.Remove(idx)
	c.bumpGenLocked(idx)
}

// --------------------------- Transaction ----------------------------

// InsertHandle executes a mutable cursor transactionally at a new offset and
// returns a versioned handle for the row. The handle becomes valid only once
// the enclosing transaction is committed - if the transaction is rolled back,
// the handle is invalidated and must be discarded.
func (txn *Txn) InsertHandle(fn func(Row) error) (Handle, error) {
	if txn.owner.pk != nil {
		return 0, errUnkeyedInsert
	}

	idx, err := txn.insert(fn, 0)
	if err != nil {
		return 0, err
	}
	return txn.owner.handleOf(idx), nil
}

// QueryHandle jumps at the row referenced by the handle and executes the
// given callback, but only if the handle's generation still matches the slot.
// If the handle is stale (the row was deleted or never committed), it returns
// ErrInvalidHandle and the callback is not executed.
func (txn *Txn) QueryHandle(handle Handle, fn func(Row) error) error {
	idx, ok := txn.owner.validHandle(handle)
	if !ok {
		return ErrInvalidHandle
	}
	return txn.QueryAt(idx, fn)
}

// DeleteHandle deletes the row referenced by the handle, but only if the
// handle's generation still matches the slot. It returns true if the row was
// deleted and false if the handle was stale.
func (txn *Txn) DeleteHandle(handle Handle) bool {
	idx, ok := txn.owner.validHandle(handle)
	if !ok {
		return false
	}
	txn.deleteAt(idx)
	return true
}

// --------------------------- Row ----------------------------

// Handle returns the versioned handle of the row at the current cursor. The
// returned handle can be stored by external systems and used to locate this
// row later, as long as the row is not deleted.
func (r Row) Handle() Handle {
	return r.txn.owner.handleOf(r.txn.cursor)
}
