// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"errors"
	"fmt"
	"io"
	"time"
)

var (
	errProjectionColumn = errors.New("column: unable to project, column '%s' does not exist")
	errProjectionIndex  = errors.New("column: unable to project, '%s' is an index and cannot be exported")
)

// SnapshotProjection writes a projected snapshot of the rows selected by this
// transaction into the underlying writer. Unlike Snapshot, which exports the
// whole collection, this method exports only the rows that are currently in the
// transaction's visibility scope (i.e. the fill-list snapshot taken when the
// transaction was initialized, further narrowed down by any With/Without/Union
// filters) and only the explicitly requested columns.
//
// The primary key column, when one exists, is always included even if it is not
// listed in columns, so that the exported collection remains keyed and its
// business keys can be searched after restore. The exported rows are re-indexed
// densely from zero: no index from the source collection is reused, so a row
// which was adjacent to an unselected row can never inherit its index. Indexes
// and triggers are never exported; they can be re-created on the restored
// collection.
//
// The visibility boundary is fixed the moment the selector's transaction is
// initialized (or when this method first touches the transaction's index).
// Concurrent writes committed while the projection is in progress are not
// exported. A row deleted after the boundary is skipped if its key has already
// been removed, and the pending commit log of the source collection is never
// appended to the output, which makes the snapshot self-contained and
// restorable on its own.
//
// The collection the output is restored into must declare the same columns that
// are included in the projection; their declaration order does not matter. A
// record column may be declared with any ForRecord constructor whose
// UnmarshalBinary can read the original payload, since only the encoded binary
// representation is carried over.
func (c *Collection) SnapshotProjection(dst io.Writer, selector func(txn *Txn) error, columns ...string) error {
	return c.Query(func(txn *Txn) error {
		if selector != nil {
			if err := selector(txn); err != nil {
				return err
			}
		}
		return txn.SnapshotProjection(dst, columns...)
	})
}

// SnapshotProjection writes a projected snapshot of the rows currently selected
// by this transaction. See Collection.SnapshotProjection for the semantics of
// the visibility boundary, the primary key handling and the dense re-indexing.
func (txn *Txn) SnapshotProjection(dst io.Writer, columns ...string) error {
	txn.initialize()

	// Resolve and validate the explicitly requested columns and make sure the
	// primary key is always part of the projection.
	selected, err := txn.owner.resolveProjection(columns)
	if err != nil {
		return err
	}

	// Materialize the selected rows into a standalone collection with dense
	// indexes starting at zero. This guarantees that no source index is reused
	// and that the produced snapshot can be counted, key-searched and written
	// to like any other collection after restore.
	projected, err := txn.copyProjection(selected)
	if err != nil {
		return err
	}
	defer projected.Close()

	// A plain snapshot of the materialized collection. No pending commit log is
	// appended, so concurrent writes of the source collection cannot leak into
	// the projection.
	return projected.Snapshot(dst)
}

// resolveProjection resolves the requested column names into concrete columns,
// rejecting unknown columns and indexes. When no column is named explicitly all
// non-index columns of the collection are projected. The primary key column is
// always included for a keyed collection.
func (c *Collection) resolveProjection(columns []string) ([]*column, error) {
	// Default to every data column when none was requested explicitly. Indexes
	// and triggers are not exportable and are excluded by the IsIndex check. The
	// built-in expiration column is skipped as well: every collection already
	// creates it on construction, so exporting it would make restore fail with a
	// duplicate-column error.
	if len(columns) == 0 {
		columns = make([]string, 0, c.cols.Count())
		c.cols.Range(func(col *column) {
			if !col.IsIndex() && col.name != expireColumn {
				columns = append(columns, col.name)
			}
		})
	}

	selected := make([]*column, 0, len(columns)+1)
	known := make(map[string]struct{}, len(columns)+1)

	for _, name := range columns {
		// The built-in expiration column is owned by the collection itself and
		// always exists on the restore target, so it must never be projected.
		if name == expireColumn {
			continue
		}

		col, ok := c.cols.Load(name)
		if !ok {
			return nil, fmt.Errorf(errProjectionColumn.Error(), name)
		}
		if col.IsIndex() {
			return nil, fmt.Errorf(errProjectionIndex.Error(), name)
		}

		if _, ok := known[name]; !ok {
			known[name] = struct{}{}
			selected = append(selected, col)
		}
	}

	// The primary key column is mandatory for a keyed collection so that the
	// business keys are preserved and key lookups keep working after restore.
	if c.pk != nil {
		if _, ok := known[c.pk.name]; !ok {
			known[c.pk.name] = struct{}{}
			pk, ok := c.cols.Load(c.pk.name)
			if !ok {
				return nil, fmt.Errorf(errProjectionColumn.Error(), c.pk.name)
			}
			selected = append(selected, pk)
		}
	}
	return selected, nil
}

// copyProjection materializes the rows visible to this transaction into a new
// collection containing only the selected columns. Rows are inserted densely, in
// index order, and receive brand-new indexes rather than keeping their source
// offsets.
func (txn *Txn) copyProjection(selected []*column) (*Collection, error) {
	capacity := txn.Count()
	dst := NewCollection(Options{
		Capacity: capacity,
		Vacuum:   time.Hour,
	})

	// Re-create every selected column with the same concrete implementation so
	// that the projected snapshot uses an identical schema for its columns.
	names := make([]string, 0, len(selected))
	for _, col := range selected {
		clone, err := cloneColumn(col)
		if err != nil {
			dst.Close()
			return nil, err
		}
		if err := dst.CreateColumn(col.name, clone); err != nil {
			dst.Close()
			return nil, err
		}
		names = append(names, col.name)
	}

	// Copy the rows. The range iterates the transaction's visibility index under
	// the sharded read locks, so every value read is consistent for its chunk.
	// The visibility index itself is the immutable boundary of this export: rows
	// inserted or deleted by concurrent transactions after it was captured are
	// neither added nor removed here, which keeps the projected indexes dense
	// and gap-free.
	var copyErr error
	err := dst.Query(func(out *Txn) error {
		_ = txn.Range(func(idx uint32) {
			if copyErr != nil {
				return
			}

			// If the row was deleted concurrently after the visibility boundary
			// was captured, its primary key has already been removed. Skip it
			// rather than failing, so the remaining rows stay densely indexed.
			key, hasKey := txn.projectionKey(idx)
			if txn.owner.pk != nil && !hasKey {
				return
			}

			values, err := txn.projectionValues(idx, names)
			if err != nil {
				copyErr = err
				return
			}

			if hasKey {
				if err := out.InsertKey(key, func(r Row) error {
					return r.SetMany(values)
				}); err != nil {
					copyErr = err
				}
				return
			}

			if _, err := out.Insert(func(r Row) error {
				return r.SetMany(values)
			}); err != nil {
				copyErr = err
			}
		})
		return copyErr
	})
	if err != nil {
		dst.Close()
		return nil, err
	}
	return dst, nil
}

// projectionKey loads the business key of a row, if the collection is keyed.
func (txn *Txn) projectionKey(idx uint32) (string, bool) {
	if txn.owner.pk == nil {
		return "", false
	}
	return txn.owner.pk.LoadString(idx)
}

// projectionValues reads the selected column values of a single row. Columns in
// which the row has no value are omitted, so the projected row only carries the
// values that were actually present.
func (txn *Txn) projectionValues(idx uint32, names []string) (map[string]any, error) {
	values := make(map[string]any, len(names))
	for _, name := range names {
		col, ok := txn.columnAt(name)
		if !ok {
			return nil, fmt.Errorf(errProjectionColumn.Error(), name)
		}

		if value, ok := col.Value(idx); ok {
			// Record columns store their payload as an encoded binary blob. The
			// concrete Go type is not known outside the target collection's
			// ForRecord constructor, so we carry the raw bytes through PutBytes,
			// which writes the exact same representation into the destination.
			if record, ok := col.Column.(*columnRecord); ok {
				if encoded, ok := record.LoadString(idx); ok {
					values[name] = rawRecord(encoded)
				}
				continue
			}
			values[name] = value
		}
	}
	return values, nil
}

// rawRecord is the binary payload of a record column value. It implements the
// binary marshal/unmarshal contracts so that it can be routed through
// Buffer.PutAny and stored verbatim into a destination record column.
type rawRecord string

// MarshalBinary returns the raw encoded record payload.
func (r rawRecord) MarshalBinary() ([]byte, error) {
	return []byte(r), nil
}

// UnmarshalBinary stores the raw encoded record payload.
func (r *rawRecord) UnmarshalBinary(b []byte) error {
	*r = rawRecord(b)
	return nil
}

// cloneColumn constructs a fresh, empty column of the same concrete type as the
// source. Record columns are re-created with a raw payload type so that their
// encoded binary representation is copied verbatim; callers that need a typed
// record accessor on restore should declare the matching ForRecord constructor
// on the target collection.
func cloneColumn(src *column) (Column, error) {
	switch src.Column.(type) {
	case *columnKey:
		return ForKey(), nil
	case *columnString:
		return ForString(), nil
	case *columnEnum:
		return ForEnum(), nil
	case *columnBool:
		return ForBool(), nil
	case *numericColumn[int]:
		return ForInt(), nil
	case *numericColumn[int16]:
		return ForInt16(), nil
	case *numericColumn[int32]:
		return ForInt32(), nil
	case *numericColumn[int64]:
		return ForInt64(), nil
	case *numericColumn[uint]:
		return ForUint(), nil
	case *numericColumn[uint16]:
		return ForUint16(), nil
	case *numericColumn[uint32]:
		return ForUint32(), nil
	case *numericColumn[uint64]:
		return ForUint64(), nil
	case *numericColumn[float32]:
		return ForFloat32(), nil
	case *numericColumn[float64]:
		return ForFloat64(), nil
	case *columnRecord:
		// The concrete record type is not known during projection; a rawRecord
		// column stores and restores the identical binary payload. Callers that
		// need the typed accessor on restore should declare the column with the
		// matching ForRecord constructor on the target collection.
		return ForRecord(func() *rawRecord {
			return new(rawRecord)
		}), nil
	default:
		return nil, fmt.Errorf("column: unable to project, unsupported column type for '%s'", src.name)
	}
}
