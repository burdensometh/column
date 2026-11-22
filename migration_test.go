// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"bytes"
	"testing"

	"github.com/kelindar/column/commit"
	"github.com/stretchr/testify/assert"
)

// newLegacyCollection builds the "old" schema used before the migration:
// a narrow, signed trade_amt column populated with representative values.
func newLegacyCollection(t *testing.T, capacity int) *Collection {
	col := NewCollection(Options{Capacity: capacity})
	assert.NoError(t, col.CreateColumn("name", ForString()))
	assert.NoError(t, col.CreateColumn("trade_amt", ForInt32()))

	values := []int32{0, 1, -7, 42, 1_000_000, -2_000_000, 32767, -32768}
	for i, v := range values {
		_, err := col.Insert(func(r Row) error {
			r.SetString("name", "trader")
			r.SetInt32("trade_amt", v)
			return nil
		})
		assert.NoError(t, err)
		_ = i
	}
	return col
}

// newMigratedCollection builds the "new" schema through the normal column
// creation interface, then registers the explicit trade_amt -> amount
// migration that widens int32 to int64.
func newMigratedCollection(t *testing.T, capacity int) *Collection {
	col := NewCollection(Options{Capacity: capacity})
	assert.NoError(t, col.CreateColumn("name", ForString()))
	assert.NoError(t, col.CreateColumn("amount", ForInt64()))

	assert.NoError(t, col.MigrateColumn(ColumnMigration{
		OldName: "trade_amt",
		NewName: "amount",
		OldType: TypeInt32,
		NewType: TypeInt64,
	}))

	// An index depending on the migrated column must be rebuilt from the
	// migrated data during restore/replay.
	assert.NoError(t, col.CreateIndex("large_trade", "amount", func(r Reader) bool {
		return r.Int() >= 1_000_000
	}))
	return col
}

// The canonical values written by newLegacyCollection.
var legacyAmounts = []int64{0, 1, -7, 42, 1_000_000, -2_000_000, 32767, -32768}

func TestMigrationSnapshotRestore(t *testing.T) {
	// Old service writes a snapshot under the old schema/name/type.
	src := newLegacyCollection(t, 16)
	buffer := bytes.NewBuffer(nil)
	assert.NoError(t, src.Snapshot(buffer))

	// New service restores it through the migration definition.
	dst := newMigratedCollection(t, 16)
	assert.NoError(t, dst.Restore(buffer))

	// No rows were lost and the old column name is not present.
	assert.Equal(t, src.Count(), dst.Count())
	_, hasOld := dst.cols.Load("trade_amt")
	assert.False(t, hasOld)

	// Every value is bound to the new column with the real int64 type.
	for i, want := range legacyAmounts {
		assert.NoError(t, dst.QueryAt(uint32(i), func(r Row) error {
			got, ok := r.Int64("amount")
			assert.True(t, ok, "row %d should have an amount value", i)
			assert.Equal(t, want, got, "row %d migrated value mismatch", i)
			return nil
		}))
	}

	// The affected index is rebuilt from the migrated values: only the
	// 1,000,000 value (row 4) matches the >= 1,000,000 predicate.
	dst.Query(func(txn *Txn) error {
		assert.Equal(t, 1, txn.With("large_trade").Count())
		return nil
	})
}

func TestMigrationReplayIncrement(t *testing.T) {
	// Old producer emits increments keyed by the old column name.
	writer := make(commit.Channel, 16)
	producer := NewCollection(Options{Writer: writer})
	assert.NoError(t, producer.CreateColumn("trade_amt", ForInt32()))

	// New consumer has the widened, renamed column plus the migration.
	consumer := newMigratedCollection(t, 16)

	_, err := producer.Insert(func(r Row) error {
		r.SetInt32("trade_amt", 12345)
		return nil
	})
	assert.NoError(t, err)

	change := <-writer
	assert.NoError(t, consumer.Replay(change))

	consumer.Query(func(txn *Txn) error {
		assert.Equal(t, 1, txn.Count())
		return nil
	})
	assert.NoError(t, consumer.QueryAt(0, func(r Row) error {
		got, ok := r.Int64("amount")
		assert.True(t, ok)
		assert.Equal(t, int64(12345), got)
		return nil
	}))

	// The replayed increment also feeds the dependent index.
	consumer.Query(func(txn *Txn) error {
		assert.Equal(t, 0, txn.With("large_trade").Count())
		return nil
	})

	// A large increment flips the index on, proving ongoing increments rebuild
	// the index using the migrated values.
	_, err = producer.Insert(func(r Row) error {
		r.SetInt32("trade_amt", 5_000_000)
		return nil
	})
	assert.NoError(t, err)
	assert.NoError(t, consumer.Replay(<-writer))
	consumer.Query(func(txn *Txn) error {
		assert.Equal(t, 1, txn.With("large_trade").Count())
		return nil
	})
}

func TestMigrationReplayMerge(t *testing.T) {
	writer := make(commit.Channel, 8)
	producer := NewCollection(Options{Writer: writer})
	assert.NoError(t, producer.CreateColumn("trade_amt", ForInt32()))

	consumer := newMigratedCollection(t, 8)

	// Seed row 0 with an initial value, then merge (atomic add) on the old side.
	_, err := producer.Insert(func(r Row) error {
		r.SetInt32("trade_amt", 100)
		return nil
	})
	assert.NoError(t, err)
	assert.NoError(t, consumer.Replay(<-writer))

	assert.NoError(t, producer.QueryAt(0, func(r Row) error {
		r.MergeInt32("trade_amt", 23)
		return nil
	}))
	assert.NoError(t, consumer.Replay(<-writer))

	assert.NoError(t, consumer.QueryAt(0, func(r Row) error {
		got, ok := r.Int64("amount")
		assert.True(t, ok)
		assert.Equal(t, int64(123), got)
		return nil
	}))
}

func TestMigrationUnknownColumnRejected(t *testing.T) {
	// A snapshot carrying a column that the new schema neither has nor has a
	// migration for must fail rather than silently dropping the data.
	src := NewCollection()
	assert.NoError(t, src.CreateColumn("ghost", ForInt32()))
	_, err := src.Insert(func(r Row) error {
		r.SetInt32("ghost", 1)
		return nil
	})
	assert.NoError(t, err)

	buffer := bytes.NewBuffer(nil)
	assert.NoError(t, src.Snapshot(buffer))

	dst := NewCollection()
	assert.NoError(t, dst.CreateColumn("amount", ForInt64()))
	err = dst.Restore(buffer)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown column")

	// Replay path must reject unknown columns as well.
	writer := make(commit.Channel, 4)
	producer := NewCollection(Options{Writer: writer})
	assert.NoError(t, producer.CreateColumn("ghost", ForInt32()))
	_, err = producer.Insert(func(r Row) error {
		r.SetInt32("ghost", 1)
		return nil
	})
	assert.NoError(t, err)
	assert.Error(t, dst.Replay(<-writer))
}

func TestMigrationNameCollisionRejected(t *testing.T) {
	col := NewCollection()
	assert.NoError(t, col.CreateColumn("trade_amt", ForInt32()))
	assert.NoError(t, col.CreateColumn("amount", ForInt64()))

	// The old name still resolves to a live column: refuse the migration.
	err := col.MigrateColumn(ColumnMigration{
		OldName: "trade_amt",
		NewName: "amount",
		OldType: TypeInt32,
		NewType: TypeInt64,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "still exists")

	// Target column missing.
	err = col.MigrateColumn(ColumnMigration{
		OldName: "missing_old",
		NewName: "does_not_exist",
		OldType: TypeInt32,
		NewType: TypeInt64,
	})
	assert.Error(t, err)

	// Duplicate source registration.
	other := NewCollection()
	assert.NoError(t, other.CreateColumn("amount", ForInt64()))
	migration := ColumnMigration{
		OldName: "trade_amt",
		NewName: "amount",
		OldType: TypeInt32,
		NewType: TypeInt64,
	}
	assert.NoError(t, other.MigrateColumn(migration))
	err = other.MigrateColumn(migration)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestMigrationLossyConversionRejected(t *testing.T) {
	cases := []struct {
		name     string
		from, to DataType
		target   Column
	}{
		{"narrowing int64->int32", TypeInt64, TypeInt32, ForInt32()},
		{"same width signedness uint32->int32", TypeUint32, TypeInt32, ForInt32()},
		{"int to float loses precision", TypeInt64, TypeFloat64, ForFloat64()},
		{"float narrowing", TypeFloat64, TypeFloat32, ForFloat32()},
		{"int to string unsupported", TypeInt32, TypeString, ForString()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col := NewCollection()
			assert.NoError(t, col.CreateColumn("new_col", tc.target))
			err := col.MigrateColumn(ColumnMigration{
				OldName: "old_col",
				NewName: "new_col",
				OldType: tc.from,
				NewType: tc.to,
			})
			assert.Error(t, err)
		})
	}
}

func TestMigrationTypeMismatchRejected(t *testing.T) {
	col := NewCollection()
	assert.NoError(t, col.CreateColumn("amount", ForInt64()))

	// Declared destination type does not match the actual column.
	err := col.MigrateColumn(ColumnMigration{
		OldName: "trade_amt",
		NewName: "amount",
		OldType: TypeInt32,
		NewType: TypeInt64,
	})
	assert.NoError(t, err) // sanity: the correct declaration is accepted

	col2 := NewCollection()
	assert.NoError(t, col2.CreateColumn("amount", ForInt32()))
	err = col2.MigrateColumn(ColumnMigration{
		OldName: "trade_amt",
		NewName: "amount",
		OldType: TypeInt32,
		NewType: TypeInt64, // actual column is int32
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not match column type")
}

func TestMigrationWideningVariantsAccepted(t *testing.T) {
	cases := []struct {
		from, to DataType
		target   Column
	}{
		{TypeInt16, TypeInt32, ForInt32()},
		{TypeInt16, TypeInt64, ForInt64()},
		{TypeUint16, TypeUint32, ForUint32()},
		{TypeUint32, TypeInt64, ForInt64()}, // extra bit of headroom
		{TypeFloat32, TypeFloat64, ForFloat64()},
	}
	for _, tc := range cases {
		col := NewCollection()
		assert.NoError(t, col.CreateColumn("new_col", tc.target))
		assert.NoError(t, col.MigrateColumn(ColumnMigration{
			OldName: "old_col",
			NewName: "new_col",
			OldType: tc.from,
			NewType: tc.to,
		}), tc.from.String()+" -> "+tc.to.String())
	}
}

func TestMigrationRenameSameType(t *testing.T) {
	// A pure rename (no width change) must carry the exact values through.
	src := NewCollection()
	assert.NoError(t, src.CreateColumn("trade_amt", ForInt64()))
	_, err := src.Insert(func(r Row) error {
		r.SetInt64("trade_amt", 9_000_000_000)
		return nil
	})
	assert.NoError(t, err)

	buffer := bytes.NewBuffer(nil)
	assert.NoError(t, src.Snapshot(buffer))

	dst := NewCollection()
	assert.NoError(t, dst.CreateColumn("amount", ForInt64()))
	assert.NoError(t, dst.MigrateColumn(ColumnMigration{
		OldName: "trade_amt",
		NewName: "amount",
		OldType: TypeInt64,
		NewType: TypeInt64,
	}))
	assert.NoError(t, dst.Restore(buffer))

	assert.NoError(t, dst.QueryAt(0, func(r Row) error {
		got, ok := r.Int64("amount")
		assert.True(t, ok)
		assert.Equal(t, int64(9_000_000_000), got)
		return nil
	}))
}

func TestMigrationNewTableUsesNormalInterface(t *testing.T) {
	// Creating a brand-new table keeps going through CreateColumn; the
	// migration API is only for binding legacy data.
	col := NewCollection()
	assert.NoError(t, col.CreateColumn("amount", ForInt64()))
	idx, err := col.Insert(func(r Row) error {
		r.SetInt64("amount", 7)
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), idx)

	// A fresh snapshot/restore round trip with no legacy data works and the
	// migration registry is simply not consulted.
	buffer := bytes.NewBuffer(nil)
	assert.NoError(t, col.Snapshot(buffer))

	dst := NewCollection()
	assert.NoError(t, dst.CreateColumn("amount", ForInt64()))
	assert.NoError(t, dst.Restore(buffer))
	assert.NoError(t, dst.QueryAt(0, func(r Row) error {
		got, ok := r.Int64("amount")
		assert.True(t, ok)
		assert.Equal(t, int64(7), got)
		return nil
	}))
}

func TestMigrationOnWireTypeMismatchRejected(t *testing.T) {
	// Producer writes uint16 values, but the migration claims int32 source.
	writer := make(commit.Channel, 4)
	producer := NewCollection(Options{Writer: writer})
	assert.NoError(t, producer.CreateColumn("trade_amt", ForUint16()))
	_, err := producer.Insert(func(r Row) error {
		r.SetUint16("trade_amt", 5)
		return nil
	})
	assert.NoError(t, err)

	consumer := NewCollection()
	assert.NoError(t, consumer.CreateColumn("amount", ForInt64()))
	assert.NoError(t, consumer.MigrateColumn(ColumnMigration{
		OldName: "trade_amt",
		NewName: "amount",
		OldType: TypeInt32, // actual on-wire width is 2 bytes
		NewType: TypeInt64,
	}))

	err = consumer.Replay(<-writer)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "byte")
}

func TestMigrationDeletesCarriedOver(t *testing.T) {
	writer := make(commit.Channel, 16)
	producer := NewCollection(Options{Writer: writer})
	assert.NoError(t, producer.CreateColumn("trade_amt", ForInt32()))

	consumer := newMigratedCollection(t, 16)

	for i := int32(0); i < 3; i++ {
		_, err := producer.Insert(func(r Row) error {
			r.SetInt32("trade_amt", i)
			return nil
		})
		assert.NoError(t, err)
		assert.NoError(t, consumer.Replay(<-writer))
	}
	assert.Equal(t, 3, consumer.Count())

	// Delete the middle row on the old producer and replay the marker.
	assert.True(t, producer.DeleteAt(1))
	assert.NoError(t, consumer.Replay(<-writer))
	assert.Equal(t, 2, consumer.Count())
}

func TestMigrationBoundaryValues(t *testing.T) {
	// Values at the edges of the source int32 range must survive the widening
	// to int64 without truncation or sign reinterpretation.
	boundaries := []int32{
		1<<31 - 1,  // max int32
		-1 << 31,   // min int32
		1 << 15,    // just past int16
		-(1 << 15), // int16 min
		-1,
	}

	src := NewCollection()
	assert.NoError(t, src.CreateColumn("trade_amt", ForInt32()))
	for _, v := range boundaries {
		_, err := src.Insert(func(r Row) error {
			r.SetInt32("trade_amt", v)
			return nil
		})
		assert.NoError(t, err)
	}

	buffer := bytes.NewBuffer(nil)
	assert.NoError(t, src.Snapshot(buffer))

	dst := NewCollection()
	assert.NoError(t, dst.CreateColumn("amount", ForInt64()))
	assert.NoError(t, dst.MigrateColumn(ColumnMigration{
		OldName: "trade_amt",
		NewName: "amount",
		OldType: TypeInt32,
		NewType: TypeInt64,
	}))
	assert.NoError(t, dst.Restore(buffer))

	for i, want := range boundaries {
		assert.NoError(t, dst.QueryAt(uint32(i), func(r Row) error {
			got, ok := r.Int64("amount")
			assert.True(t, ok)
			assert.Equal(t, int64(want), got)
			return nil
		}))
	}
}

func TestMigrationSnapshotWithConcurrentIncrement(t *testing.T) {
	// The snapshot's trailing commit log is produced while writes continue.
	// Those old-schema increments must be migrated on restore as well.
	writer := make(commit.Channel, 64)
	src := NewCollection(Options{Writer: writer})
	assert.NoError(t, src.CreateColumn("trade_amt", ForInt32()))

	_, err := src.Insert(func(r Row) error {
		r.SetInt32("trade_amt", 10)
		return nil
	})
	assert.NoError(t, err)

	buffer := bytes.NewBuffer(nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		// An increment landing during/after the snapshot.
		src.QueryAt(0, func(r Row) error {
			r.SetInt32("trade_amt", 77)
			return nil
		})
	}()
	assert.NoError(t, src.Snapshot(buffer))
	<-done

	dst := NewCollection()
	assert.NoError(t, dst.CreateColumn("amount", ForInt64()))
	assert.NoError(t, dst.MigrateColumn(ColumnMigration{
		OldName: "trade_amt",
		NewName: "amount",
		OldType: TypeInt32,
		NewType: TypeInt64,
	}))
	assert.NoError(t, dst.Restore(buffer))

	// The final state reflects the migrated increment regardless of whether it
	// was captured in the state section or the trailing commit log.
	assert.NoError(t, dst.QueryAt(0, func(r Row) error {
		got, ok := r.Int64("amount")
		assert.True(t, ok)
		assert.Contains(t, []int64{10, 77}, got)
		return nil
	}))
}

func TestMigrationDoesNotRegisterOnRejectedConversion(t *testing.T) {
	col := NewCollection()
	assert.NoError(t, col.CreateColumn("amount", ForInt32()))

	// A rejected migration must leave the registry empty.
	err := col.MigrateColumn(ColumnMigration{
		OldName: "trade_amt",
		NewName: "amount",
		OldType: TypeInt64, // narrowing into int32
		NewType: TypeInt32,
	})
	assert.Error(t, err)
	assert.Empty(t, col.migrate)
}
