// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"

	"github.com/kelindar/column/commit"
)

// DataType represents the physical value type of a column, as it is encoded
// inside snapshot buffers and incremental commit buffers.
type DataType uint8

// The supported data types for a column migration.
const (
	TypeBool   DataType = iota // Boolean (1-bit) column
	TypeInt                    // Platform-width signed integer column
	TypeInt16                  // 16-bit signed integer column
	TypeInt32                  // 32-bit signed integer column
	TypeInt64                  // 64-bit signed integer column
	TypeUint                   // Platform-width unsigned integer column
	TypeUint16                 // 16-bit unsigned integer column
	TypeUint32                 // 32-bit unsigned integer column
	TypeUint64                 // 64-bit unsigned integer column
	TypeFloat32                // 32-bit floating-point column
	TypeFloat64                // 64-bit floating-point column
	TypeString                 // Variable-size string column
	TypeRecord                 // Variable-size binary record column
)

// String returns a human-readable name of the data type.
func (t DataType) String() string {
	switch t {
	case TypeBool:
		return "bool"
	case TypeInt:
		return "int"
	case TypeInt16:
		return "int16"
	case TypeInt32:
		return "int32"
	case TypeInt64:
		return "int64"
	case TypeUint:
		return "uint"
	case TypeUint16:
		return "uint16"
	case TypeUint32:
		return "uint32"
	case TypeUint64:
		return "uint64"
	case TypeFloat32:
		return "float32"
	case TypeFloat64:
		return "float64"
	case TypeString:
		return "string"
	case TypeRecord:
		return "record"
	default:
		return "unknown"
	}
}

// width returns the on-wire width in bytes of fixed-size types. Variable-size
// types (string and record) return 0.
func (t DataType) width() int {
	switch t {
	case TypeInt16, TypeUint16:
		return 2
	case TypeInt32, TypeUint32, TypeFloat32:
		return 4
	case TypeInt, TypeUint, TypeInt64, TypeUint64, TypeFloat64:
		return 8
	default:
		return 0
	}
}

// signed reports whether the type is a signed integer.
func (t DataType) signed() bool {
	switch t {
	case TypeInt, TypeInt16, TypeInt32, TypeInt64:
		return true
	default:
		return false
	}
}

// integer reports whether the type is an integer type.
func (t DataType) integer() bool {
	switch t {
	case TypeInt, TypeInt16, TypeInt32, TypeInt64,
		TypeUint, TypeUint16, TypeUint32, TypeUint64:
		return true
	default:
		return false
	}
}

// float reports whether the type is a floating-point type.
func (t DataType) float() bool {
	return t == TypeFloat32 || t == TypeFloat64
}

// ColumnMigration declares an explicit, named migration of a column. It tells
// the collection that buffers produced under the old schema (old snapshots as
// well as old incremental commits) may still reference OldName, and that those
// values must be rebound to the column currently registered under NewName and
// converted from OldType to NewType.
type ColumnMigration struct {
	OldName string   // The column name used by old snapshots and old increments
	NewName string   // The column name in the current (new) schema
	OldType DataType // The value type used by old data
	NewType DataType // The value type used by the new column
}

// MigrateColumn registers an explicit column migration. The target column must
// already exist in the collection (created through the normal CreateColumn /
// CreateColumnsOf interface), and the declared source and destination types
// must be compatible without loss of information. Registering a migration for
// an unknown source type, a colliding name, or a lossy conversion is rejected
// with an error and leaves the collection unchanged.
func (c *Collection) MigrateColumn(m ColumnMigration) error {
	if m.OldName == "" || m.NewName == "" {
		return fmt.Errorf("column: migration must specify both old and new column names")
	}

	if m.OldName == m.NewName && m.OldType == m.NewType {
		return fmt.Errorf("column: migration from '%s' to itself is a no-op", m.OldName)
	}

	// The destination column must exist in the current schema and must be a
	// data column, not a computed index or trigger.
	dst, ok := c.cols.Load(m.NewName)
	if !ok {
		return fmt.Errorf("column: unable to migrate '%s' to '%s', target column does not exist",
			m.OldName, m.NewName)
	}
	if dst.IsIndex() {
		return fmt.Errorf("column: unable to migrate '%s' to '%s', target is an index, not a data column",
			m.OldName, m.NewName)
	}

	// The actual type of the destination column must match the declared one.
	if actual, ok := kindOf(dst.Column); !ok || actual != m.NewType {
		return fmt.Errorf("column: unable to migrate '%s' to '%s', declared type %s does not match column type %s",
			m.OldName, m.NewName, m.NewType, actual)
	}

	// A migration may only reference source types we know how to decode.
	if m.OldType.String() == "unknown" || m.NewType.String() == "unknown" {
		return fmt.Errorf("column: unable to migrate '%s' to '%s', unknown data type",
			m.OldName, m.NewName)
	}

	// Name collisions: the old name must not resolve to a live column, and no
	// two migrations may claim the same source or destination column.
	if _, exists := c.cols.Load(m.OldName); exists {
		return fmt.Errorf("column: unable to migrate '%s', a column with that name still exists",
			m.OldName)
	}
	for _, existing := range c.migrate {
		if existing.OldName == m.OldName {
			return fmt.Errorf("column: unable to migrate '%s', a migration from this column already exists",
				m.OldName)
		}
		if existing.NewName == m.NewName {
			return fmt.Errorf("column: unable to migrate into '%s', a migration into this column already exists",
				m.NewName)
		}
	}

	// The conversion itself must be lossless.
	if err := m.validateConversion(); err != nil {
		return err
	}

	c.lock.Lock()
	c.migrate = append(c.migrate, m)
	c.lock.Unlock()
	return nil
}

// validateConversion makes sure that every value representable in OldType can
// be represented in NewType without loss of information.
func (m ColumnMigration) validateConversion() error {
	from, to := m.OldType, m.NewType

	// Identical type, only the name changes: always safe.
	if from == to {
		return nil
	}

	switch {
	// Integer widening within the same signedness, or a strictly wider
	// integer that can hold the full range of the source.
	case from.integer() && to.integer():
		if err := validateIntegerWidening(from, to); err != nil {
			return fmt.Errorf("column: lossy migration '%s' (%s) -> '%s' (%s): %w",
				m.OldName, from, m.NewName, to, err)
		}
		return nil

	// Floating-point widening (float32 -> float64) is lossless.
	case from.float() && to.float() && to.width() > from.width():
		return nil

	default:
		return fmt.Errorf("column: lossy migration '%s' (%s) -> '%s' (%s), conversion is not supported",
			m.OldName, from, m.NewName, to)
	}
}

// validateIntegerWidening verifies that an integer-to-integer conversion cannot
// truncate or reinterpret any source value.
func validateIntegerWidening(from, to DataType) error {
	fromWidth, toWidth := from.width(), to.width()
	if toWidth < fromWidth {
		return fmt.Errorf("narrowing from %d-bit to %d-bit integer", fromWidth*8, toWidth*8)
	}

	// A same-width change of signedness cannot hold the full source range
	// (e.g. uint16 values above 32767 do not fit into int16).
	if toWidth == fromWidth && from.signed() != to.signed() {
		return fmt.Errorf("same-width change of signedness between %s and %s", from, to)
	}

	// A strictly wider destination must have one extra bit of headroom to hold
	// the full unsigned range of a narrower source (e.g. uint32 -> int64 is
	// safe, uint32 -> int32 is rejected above).
	return nil
}

// migrationFor resolves the migration registered for an old column name. It
// returns false when no migration exists for the name.
func (c *Collection) migrationFor(oldName string) (ColumnMigration, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	for _, m := range c.migrate {
		if m.OldName == oldName {
			return m, true
		}
	}
	return ColumnMigration{}, false
}

// kindOf resolves the DataType of a concrete column implementation.
func kindOf(column Column) (DataType, bool) {
	switch column.(type) {
	case *columnBool:
		return TypeBool, true
	case *numericColumn[int]:
		return TypeInt, true
	case *numericColumn[int16]:
		return TypeInt16, true
	case *numericColumn[int32]:
		return TypeInt32, true
	case *numericColumn[int64]:
		return TypeInt64, true
	case *numericColumn[uint]:
		return TypeUint, true
	case *numericColumn[uint16]:
		return TypeUint16, true
	case *numericColumn[uint32]:
		return TypeUint32, true
	case *numericColumn[uint64]:
		return TypeUint64, true
	case *numericColumn[float32]:
		return TypeFloat32, true
	case *numericColumn[float64]:
		return TypeFloat64, true
	case *columnEnum, *columnString, *columnKey:
		return TypeString, true
	case *columnRecord:
		return TypeRecord, true
	default:
		return 0, false
	}
}

// --------------------------- Buffer Migration ----------------------------

// migrateBuffer rebinds an old-schema buffer to the new schema. It resolves the
// migration for the buffer's column name, converts every encoded value to the
// destination type and returns a pool-owned buffer tagged with the new column
// name. The source buffer is never mutated. Buffers without a matching
// migration (the row marker, or columns that still exist under the same name)
// are copied into a fresh buffer so that callers can uniformly hand the result
// to a transaction. An unknown column, an on-wire type that does not match the
// declared source type, or a value that cannot be represented in the
// destination type results in an error.
func (c *Collection) migrateBuffer(buffer *commit.Buffer) (*commit.Buffer, error) {
	if buffer.IsEmpty() || buffer.Column == "" || buffer.Column == rowColumn {
		return buffer, nil
	}

	// If the column still exists under its current name, keep its binding but
	// still copy the bytes so the transaction owns a reusable pool page.
	if _, ok := c.cols.Load(buffer.Column); ok {
		return buffer.Clone(), nil
	}

	migration, ok := c.migrationFor(buffer.Column)
	if !ok {
		return nil, fmt.Errorf("column: unknown column '%s' in restored data and no migration is registered",
			buffer.Column)
	}

	converted := c.txns.acquirePage(migration.NewName)
	if err := convertBuffer(buffer, converted, migration); err != nil {
		c.txns.releasePage(converted)
		return nil, err
	}
	return converted, nil
}
