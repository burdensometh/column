// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"

	"github.com/kelindar/column/commit"
)

// convertBuffer rewrites every operation of an old-schema source buffer into a
// destination buffer bound to the migrated column. The index layout (chunks,
// offsets, operation types) is preserved; only the column binding and the
// physical encoding of each value change.
func convertBuffer(src, dst *commit.Buffer, m ColumnMigration) error {
	reader := commit.NewReader()
	for _, chunk := range chunkList(src) {
		var convertErr error
		reader.Range(src, chunk, func(r *commit.Reader) {
			for r.Next() && convertErr == nil {
				idx := r.Index()
				switch r.Type {
				case commit.Put, commit.Merge:
					convertErr = convertValue(r, dst, idx, m)
				case commit.Delete:
					// Valueless markers (Delete == PutFalse == 0) keep their
					// operation type and index.
					dst.PutOperation(r.Type, idx)
				default:
					convertErr = fmt.Errorf("column: unsupported operation %d while migrating column '%s'",
						r.Type, m.OldName)
				}
			}
		})
		if convertErr != nil {
			return convertErr
		}
	}
	return nil
}

// chunkList returns the distinct chunks present in a buffer.
func chunkList(b *commit.Buffer) []commit.Chunk {
	chunks := make([]commit.Chunk, 0, 1)
	b.RangeChunks(func(chunk commit.Chunk) {
		chunks = append(chunks, chunk)
	})
	return chunks
}

// convertValue decodes a single value using the declared source type, validates
// that the on-wire encoding matches it, performs the lossless conversion and
// appends the converted value to the destination buffer.
func convertValue(r *commit.Reader, dst *commit.Buffer, idx uint32, m ColumnMigration) error {
	from, to := m.OldType, m.NewType

	// Variable-size and boolean columns can only be carried over unchanged;
	// validateConversion() guarantees from == to in these cases.
	switch from {
	case TypeString, TypeRecord:
		if from != to {
			return fmt.Errorf("column: cannot convert %s value of column '%s' into %s",
				from, m.OldName, to)
		}
		dst.PutBytes(r.Type, idx, r.Bytes())
		return nil
	case TypeBool:
		if from != to {
			return fmt.Errorf("column: cannot convert %s value of column '%s' into %s",
				from, m.OldName, to)
		}
		dst.PutOperation(r.Type, idx)
		return nil
	}

	// Fixed-size numeric values. The on-wire width must match the declared
	// source type, otherwise the migration definition does not describe the
	// actual data and we must refuse rather than misread it.
	if err := expectWidth(r, from); err != nil {
		return err
	}

	if from.float() || to.float() {
		return convertFloat(r, dst, idx, m)
	}
	return convertInteger(r, dst, idx, m)
}

// expectWidth verifies that the encoded value has the width declared by the
// migration's source type.
func expectWidth(r *commit.Reader, from DataType) error {
	actual := r.ValueWidth()
	want := from.width()
	if actual != want {
		return fmt.Errorf("column: source type %s expects a %d-byte value but restored value is %d bytes",
			from, want, actual)
	}
	return nil
}

// convertInteger decodes an integer value, range-checks it against the
// destination type and re-encodes it at the destination width.
func convertInteger(r *commit.Reader, dst *commit.Buffer, idx uint32, m ColumnMigration) error {
	from, to := m.OldType, m.NewType

	// Decode the source value into a canonical int64/uint64 representation.
	var signed int64
	var unsigned uint64
	if from.signed() {
		signed = readSigned(r, from)
		unsigned = uint64(signed)
	} else {
		unsigned = readUnsigned(r, from)
		signed = int64(unsigned)
	}

	// Per-value guard. The registration-time check proves the *ranges* are
	// compatible; this catches any residual overflow defensively (for example
	// a same-width signedness change that slipped through).
	if err := rangeCheck(signed, unsigned, from, to); err != nil {
		return err
	}

	op := r.Type
	switch to {
	case TypeInt:
		dst.PutInt(op, idx, int(signed))
	case TypeInt16:
		dst.PutInt16(op, idx, int16(signed))
	case TypeInt32:
		dst.PutInt32(op, idx, int32(signed))
	case TypeInt64:
		dst.PutInt64(op, idx, signed)
	case TypeUint:
		dst.PutUint(op, idx, uint(unsigned))
	case TypeUint16:
		dst.PutUint16(op, idx, uint16(unsigned))
	case TypeUint32:
		dst.PutUint32(op, idx, uint32(unsigned))
	case TypeUint64:
		dst.PutUint64(op, idx, unsigned)
	default:
		return fmt.Errorf("column: cannot encode converted integer as %s", to)
	}
	return nil
}

// convertFloat handles floating-point conversions, including the lossless
// float32 -> float64 widening.
func convertFloat(r *commit.Reader, dst *commit.Buffer, idx uint32, m ColumnMigration) error {
	from, to := m.OldType, m.NewType

	switch {
	case from == TypeFloat32 && to == TypeFloat64:
		dst.PutFloat64(r.Type, idx, float64(r.Float32()))
		return nil
	case from == to:
		if from == TypeFloat32 {
			dst.PutFloat32(r.Type, idx, r.Float32())
		} else {
			dst.PutFloat64(r.Type, idx, r.Float64())
		}
		return nil
	default:
		return fmt.Errorf("column: lossy or unsupported float conversion %s -> %s for column '%s'",
			from, to, m.OldName)
	}
}

// readSigned decodes a signed integer of the given type from the reader.
func readSigned(r *commit.Reader, from DataType) int64 {
	switch from {
	case TypeInt16:
		return int64(r.Int16())
	case TypeInt32:
		return int64(r.Int32())
	default: // TypeInt, TypeInt64
		return r.Int64()
	}
}

// readUnsigned decodes an unsigned integer of the given type from the reader.
func readUnsigned(r *commit.Reader, from DataType) uint64 {
	switch from {
	case TypeUint16:
		return uint64(r.Uint16())
	case TypeUint32:
		return uint64(r.Uint32())
	default: // TypeUint, TypeUint64
		return r.Uint64()
	}
}

// rangeCheck verifies that a decoded source value fits into the destination
// integer type. Negative values can only land in signed destinations, and the
// magnitude must fit within the destination width.
func rangeCheck(signed int64, unsigned uint64, from, to DataType) error {
	if to.signed() {
		// Signed destination: validate against its positive and negative bounds.
		limit := uint64(1) << uint(to.width()*8-1)
		if signed < 0 {
			if uint64(-signed) > limit {
				return overflowError(from, to, signed)
			}
			return nil
		}
		if unsigned >= limit {
			return overflowError(from, to, signed)
		}
		return nil
	}

	// Unsigned destination cannot represent a negative value.
	if signed < 0 {
		return overflowError(from, to, signed)
	}
	if to.width() < 8 && unsigned >= (uint64(1)<<uint(to.width()*8)) {
		return overflowError(from, to, signed)
	}
	return nil
}

func overflowError(from, to DataType, value int64) error {
	return fmt.Errorf("column: lossy migration, value %d (%s) does not fit into %s",
		value, from, to)
}
