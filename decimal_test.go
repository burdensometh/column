// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"bytes"
	"math"
	"sync"
	"testing"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
	"github.com/stretchr/testify/assert"
)

// --------------------------- Decimal Value ----------------------------

func TestDecimal_ParseAndText(t *testing.T) {
	tests := []struct {
		text  string
		scale uint8
		round RoundingPolicy
		want  int64
		out   string
	}{
		{"123.45", 2, RoundHalfUp, 12345, "123.45"},
		{"0.10", 2, RoundHalfUp, 10, "0.10"},
		{"-0.10", 2, RoundHalfUp, -10, "-0.10"},
		{"1.005", 2, RoundHalfUp, 101, "1.01"},    // half up, away from zero
		{"-1.005", 2, RoundHalfUp, -101, "-1.01"}, // symmetric half up
		{"1.004", 2, RoundHalfUp, 100, "1.00"},
		{"9.999", 2, RoundDown, 999, "9.99"}, // truncation
		{"9.991", 2, RoundUp, 1000, "10.00"}, // any dropped digit rounds up
		{"-9.999", 2, RoundDown, -999, "-9.99"},
		{"123", 2, RoundHalfUp, 12300, "123.00"},
		{".5", 2, RoundHalfUp, 50, "0.50"},
		{"100", 0, RoundHalfUp, 100, "100"},
		{"0", 4, RoundHalfUp, 0, "0.0000"},
	}

	for _, tc := range tests {
		t.Run(tc.text, func(t *testing.T) {
			d, err := parseDecimal(tc.text, tc.scale, tc.round, OverflowPanic)
			assert.NoError(t, err)
			assert.Equal(t, tc.want, d.Unscaled)
			assert.Equal(t, tc.scale, d.Scale)
			assert.Equal(t, tc.out, d.Text())
		})
	}
}

func TestDecimal_ParseInvalid(t *testing.T) {
	for _, text := range []string{"", "  ", "abc", "1.2.3", "1e3", "--1", "."} {
		_, err := parseDecimal(text, 2, RoundHalfUp, OverflowPanic)
		assert.Error(t, err, text)
	}
}

func TestDecimal_FromFloat64Exact(t *testing.T) {
	// The classic floating point accumulation must enter storage exactly.
	d, err := DecimalFromFloat64(0.1+0.2, 2, RoundHalfUp, OverflowPanic)
	assert.NoError(t, err)
	assert.Equal(t, "0.30", d.Text())

	_, err = DecimalFromFloat64(math.NaN(), 2, RoundHalfUp, OverflowPanic)
	assert.Error(t, err)
	_, err = DecimalFromFloat64(math.Inf(1), 2, RoundHalfUp, OverflowPanic)
	assert.Error(t, err)
}

func TestDecimal_NewAndConvert(t *testing.T) {
	d := NewDecimal(12345, 2)
	assert.Equal(t, "123.45", d.Text())
	assert.InDelta(t, 123.45, d.Float64(), 1e-12)

	fromInt := DecimalFromInt64(7, 2)
	assert.Equal(t, int64(700), fromInt.Unscaled)
	assert.Equal(t, "7.00", fromInt.Text())
}

func TestForDecimal_InvalidConfig(t *testing.T) {
	_, err := ForDecimal(19)
	assert.ErrorIs(t, err, errInvalidScale)

	_, err = ForDecimal(5, WithPrecision(3)) // precision < scale
	assert.ErrorIs(t, err, errInvalidPrec)

	_, err = ForDecimal(2, WithPrecision(19))
	assert.ErrorIs(t, err, errInvalidPrec)

	col, err := ForDecimal(2, WithPrecision(4))
	assert.NoError(t, err)
	assert.Equal(t, uint8(2), col.(*columnDecimal).Scale())
	assert.Equal(t, uint8(4), col.(*columnDecimal).Precision())
}

// --------------------------- Overflow Policies ----------------------------

func TestDecimal_OverflowPolicies(t *testing.T) {
	// precision 4, scale 2 -> range -99.99 .. 99.99 => unscaled -9999..9999
	t.Run("panic", func(t *testing.T) {
		col, _ := ForDecimal(2, WithPrecision(4), WithOverflow(OverflowPanic))
		assert.Panics(t, func() {
			col.(*columnDecimal).guard(10000)
		})
	})

	t.Run("saturate", func(t *testing.T) {
		col, _ := ForDecimal(2, WithPrecision(4), WithOverflow(OverflowSaturate))
		c := col.(*columnDecimal)
		assert.Equal(t, int64(9999), c.guard(10000))
		assert.Equal(t, int64(-9999), c.guard(-10000))
		assert.Equal(t, int64(1234), c.guard(1234))
	})

	t.Run("error", func(t *testing.T) {
		col, _ := ForDecimal(2, WithPrecision(4), WithOverflow(OverflowError))
		c := col.(*columnDecimal)
		v, err := c.guardTxn(10000)
		assert.ErrorIs(t, err, errOverflow)
		assert.Equal(t, int64(9999), v) // clamped alongside the error
	})
}

func applyDecimalChanges(column Column, updates ...Update) {
	buf := commit.NewBuffer(10)
	decimal := column.(*columnDecimal)
	buf.PutScaleMarker(decimal.Scale())
	for _, u := range updates {
		switch value := u.Value.(type) {
		case nil:
			buf.PutOperation(u.Type, u.Index)
		case Decimal:
			buf.PutInt64(u.Type, u.Index, value.Unscaled)
		}
	}

	r := new(commit.Reader)
	r.Seek(buf)
	column.Apply(0, r)
}

// --------------------------- Column Contract ----------------------------

func TestDecimalColumn_NumericContract(t *testing.T) {
	col, err := ForDecimal(2, WithPrecision(8))
	assert.NoError(t, err)

	numeric, ok := col.(Numeric)
	assert.True(t, ok)
	scaled, ok := col.(Scaled)
	assert.True(t, ok)
	assert.Equal(t, uint8(2), scaled.Scale())

	col.Grow(8)
	applyDecimalChanges(col,
		Update{commit.Put, 1, Decimal{Unscaled: 1050, Scale: 2}},
		Update{commit.Put, 2, Decimal{Unscaled: 2025, Scale: 2}},
	)

	// Contains / Value and the loaders
	assert.True(t, col.Contains(1))
	v, ok := col.Value(1)
	assert.True(t, ok)
	assert.Equal(t, Decimal{Unscaled: 1050, Scale: 2}, v)

	f64, ok := numeric.LoadFloat64(1)
	assert.True(t, ok)
	assert.InDelta(t, 10.50, f64, 1e-12)

	i64, ok := numeric.LoadInt64(2)
	assert.True(t, ok)
	assert.Equal(t, int64(2025), i64)

	// Filters operate on the fill intersection
	index := bitmap.Bitmap{0xffff}
	numeric.FilterFloat64(0, index, func(v float64) bool { return v > 15.0 })
	assert.Equal(t, 1, index.Count())

	index = bitmap.Bitmap{0xffff}
	numeric.FilterInt64(0, index, func(v int64) bool { return v == 1050 })
	assert.Equal(t, 1, index.Count())
}

func TestDecimalColumn_ZeroIsDistinctFromMissing(t *testing.T) {
	col, _ := ForDecimal(2)
	col.Grow(4)
	applyDecimalChanges(col, Update{commit.Put, 0, Decimal{Unscaled: 0, Scale: 2}})

	zero, hasZero := col.Value(0)
	assert.True(t, hasZero)
	assert.Equal(t, Decimal{Unscaled: 0, Scale: 2}, zero)

	missing, hasMissing := col.Value(1)
	assert.False(t, hasMissing)
	assert.Equal(t, Decimal{Scale: 2}, missing)
}

func TestDecimalColumn_PutDelete(t *testing.T) {
	col, _ := ForDecimal(2)
	col.Grow(4)
	applyDecimalChanges(col,
		Update{commit.Put, 0, Decimal{Unscaled: 100, Scale: 2}},
		Update{commit.Delete, 0, nil},
	)
	_, ok := col.Value(0)
	assert.False(t, ok)
}

func TestDecimalColumn_MergeAccumulatesExactly(t *testing.T) {
	col, _ := ForDecimal(2)
	col.Grow(4)

	// Start at 0.10 then merge 0.20 ten times -> 2.10 with zero float drift.
	applyDecimalChanges(col, Update{commit.Put, 0, Decimal{Unscaled: 10, Scale: 2}})
	for i := 0; i < 10; i++ {
		applyDecimalChanges(col, Update{commit.Merge, 0, Decimal{Unscaled: 20, Scale: 2}})
	}

	v, ok := col.Value(0)
	assert.True(t, ok)
	assert.Equal(t, "2.10", v.(Decimal).Text())
}

// --------------------------- Transaction Integration ----------------------------

func newDecimalCollection(t *testing.T) *Collection {
	col := NewCollection()
	_, err := ForDecimal(2, WithPrecision(10))
	assert.NoError(t, err)
	assert.NoError(t, col.CreateColumn("amount", mustDecimal(2, WithPrecision(10))))
	return col
}

func mustDecimal(scale uint8, opts ...func(*decimalConfig)) Column {
	col, err := ForDecimal(scale, opts...)
	if err != nil {
		panic(err)
	}
	return col
}

func TestDecimalTxn_SetGetText(t *testing.T) {
	col := newDecimalCollection(t)
	idx, err := col.Insert(func(r Row) error {
		return r.SetDecimalText("amount", "12.34")
	})
	assert.NoError(t, err)

	col.QueryAt(idx, func(r Row) error {
		d, ok := r.Decimal("amount")
		assert.True(t, ok)
		assert.Equal(t, "12.34", d.Text())

		text, ok := r.txn.Decimal("amount").Text()
		assert.True(t, ok)
		assert.Equal(t, "12.34", text)
		return nil
	})
}

func TestDecimalTxn_SetFloat64IsExact(t *testing.T) {
	col := newDecimalCollection(t)
	idx, err := col.Insert(func(r Row) error {
		return r.SetDecimalFloat64("amount", 0.1+0.2)
	})
	assert.NoError(t, err)

	col.QueryAt(idx, func(r Row) error {
		d, _ := r.Decimal("amount")
		assert.Equal(t, "0.30", d.Text()) // exact, not 0.30000000000000004
		return nil
	})
}

func TestDecimalTxn_ScaleMismatchRejected(t *testing.T) {
	col := newDecimalCollection(t) // scale 2
	_, err := col.Insert(func(r Row) error {
		return r.SetDecimal("amount", NewDecimal(12345, 3)) // scale 3
	})
	assert.ErrorIs(t, err, errScaleMismatch)
}

func TestDecimalTxn_OverflowError(t *testing.T) {
	col := NewCollection()
	assert.NoError(t, col.CreateColumn("amount",
		mustDecimal(2, WithPrecision(4), WithOverflow(OverflowError))))

	_, err := col.Insert(func(r Row) error {
		return r.SetDecimalText("amount", "100.00") // exceeds 99.99
	})
	assert.ErrorIs(t, err, errOverflow)
	assert.Equal(t, 0, col.Count()) // rolled back
}

func TestDecimalTxn_MergeAndSum(t *testing.T) {
	col := NewCollection()
	assert.NoError(t, col.CreateColumn("amount", mustDecimal(2, WithPrecision(12))))

	for _, text := range []string{"0.10", "0.20", "0.30"} {
		_, err := col.Insert(func(r Row) error {
			return r.SetDecimalText("amount", text)
		})
		assert.NoError(t, err)
	}

	col.Query(func(txn *Txn) error {
		amount := txn.Decimal("amount")

		// Exact int64-backed sum
		sum := amount.Sum()
		assert.Equal(t, "0.60", sum.Text())
		assert.Equal(t, int64(60), sum.Unscaled)

		// Arbitrary precision and text export agree
		assert.Equal(t, "0.60", amount.SumText())
		assert.Equal(t, "0.60", amount.SumBig().Text())

		min, ok := amount.Min()
		assert.True(t, ok)
		assert.Equal(t, "0.10", min.Text())

		max, ok := amount.Max()
		assert.True(t, ok)
		assert.Equal(t, "0.30", max.Text())
		return nil
	})
}

func TestDecimalTxn_MergeFloat(t *testing.T) {
	col := NewCollection()
	assert.NoError(t, col.CreateColumn("amount", mustDecimal(2, WithPrecision(12))))

	idx, _ := col.Insert(func(r Row) error {
		return r.SetDecimalText("amount", "1.00")
	})

	assert.NoError(t, col.QueryAt(idx, func(r Row) error {
		return r.MergeDecimalFloat64("amount", 0.50)
	}))

	col.QueryAt(idx, func(r Row) error {
		d, _ := r.Decimal("amount")
		assert.Equal(t, "1.50", d.Text())
		return nil
	})
}

func TestDecimalTxn_RepeatedMergesCompose(t *testing.T) {
	col := NewCollection()
	assert.NoError(t, col.CreateColumn("amount", mustDecimal(2, WithPrecision(12))))

	idx, _ := col.Insert(func(r Row) error {
		return r.SetDecimalText("amount", "1.00")
	})

	// Three merges inside a single transaction must accumulate on the running
	// value, not each guard against the original committed value.
	assert.NoError(t, col.QueryAt(idx, func(r Row) error {
		acc := r.txn.Decimal("amount")
		for i := 0; i < 3; i++ {
			if err := acc.Merge(NewDecimal(25, 2)); err != nil {
				return err
			}
		}
		return nil
	}))

	col.QueryAt(idx, func(r Row) error {
		d, _ := r.Decimal("amount")
		assert.Equal(t, "1.75", d.Text())
		return nil
	})
}

func TestDecimalTxn_MergeOverflowSaturatesAtApply(t *testing.T) {
	// precision 4, scale 2 -> max 99.99
	col := NewCollection()
	assert.NoError(t, col.CreateColumn("amount",
		mustDecimal(2, WithPrecision(4), WithOverflow(OverflowSaturate))))

	idx, _ := col.Insert(func(r Row) error {
		return r.SetDecimalText("amount", "99.00")
	})

	assert.NoError(t, col.QueryAt(idx, func(r Row) error {
		return r.MergeDecimal("amount", NewDecimal(500, 2)) // +5.00 -> clamp 99.99
	}))

	col.QueryAt(idx, func(r Row) error {
		d, _ := r.Decimal("amount")
		assert.Equal(t, "99.99", d.Text())
		return nil
	})
}

func TestDecimalTxn_Filtering(t *testing.T) {
	col := NewCollection()
	assert.NoError(t, col.CreateColumn("amount", mustDecimal(2, WithPrecision(10))))
	for _, text := range []string{"1.00", "2.00", "3.00", "4.00"} {
		_, err := col.Insert(func(r Row) error {
			return r.SetDecimalText("amount", text)
		})
		assert.NoError(t, err)
	}

	col.Query(func(txn *Txn) error {
		// Exact decimal predicate
		assert.Equal(t, 2, txn.WithDecimal("amount", func(d Decimal) bool {
			return d.Unscaled >= 300
		}).Count())
		return nil
	})

	// Float predicate via the Numeric contract (separate txn, fresh index)
	col.Query(func(txn *Txn) error {
		assert.Equal(t, 3, txn.WithFloat("amount", func(v float64) bool {
			return v >= 2.0
		}).Count())
		return nil
	})
}

func TestDecimalTxn_IndexUsesScaledFloat(t *testing.T) {
	col := NewCollection()
	assert.NoError(t, col.CreateColumn("amount", mustDecimal(2, WithPrecision(10))))
	assert.NoError(t, col.CreateIndex("big", "amount", func(r Reader) bool {
		return r.Float() >= 3.0 // scaled value, not the raw unscaled 300
	}))

	for _, text := range []string{"1.00", "3.50", "4.00"} {
		_, err := col.Insert(func(r Row) error {
			return r.SetDecimalText("amount", text)
		})
		assert.NoError(t, err)
	}

	col.Query(func(txn *Txn) error {
		assert.Equal(t, 2, txn.With("big").Count())
		return nil
	})
}

func TestDecimalTxn_WrongTypePanics(t *testing.T) {
	col := NewCollection()
	assert.NoError(t, col.CreateColumn("name", ForString()))
	assert.Panics(t, func() {
		col.Query(func(txn *Txn) error {
			txn.Decimal("name")
			return nil
		})
	})
}

// --------------------------- Snapshot / Restore ----------------------------

func TestDecimalSnapshot_RoundTrip(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	input := NewCollection()
	assert.NoError(t, input.CreateColumn("amount", mustDecimal(2, WithPrecision(12))))

	for _, text := range []string{"1.11", "2.22", "3.33"} {
		_, err := input.Insert(func(r Row) error {
			return r.SetDecimalText("amount", text)
		})
		assert.NoError(t, err)
	}
	assert.NoError(t, input.Snapshot(buffer))

	output := NewCollection()
	assert.NoError(t, output.CreateColumn("amount", mustDecimal(2, WithPrecision(12))))
	assert.NoError(t, output.Restore(buffer))
	assert.Equal(t, 3, output.Count())

	output.Query(func(txn *Txn) error {
		assert.Equal(t, "6.66", txn.Decimal("amount").Sum().Text())
		return nil
	})
}

func TestDecimalSnapshot_ScaleMismatchRejected(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	input := NewCollection()
	assert.NoError(t, input.CreateColumn("amount", mustDecimal(2, WithPrecision(12))))
	_, err := input.Insert(func(r Row) error {
		return r.SetDecimalText("amount", "1.11")
	})
	assert.NoError(t, err)
	assert.NoError(t, input.Snapshot(buffer))

	// Restoring into a column declared with a different scale must fail.
	output := NewCollection()
	assert.NoError(t, output.CreateColumn("amount", mustDecimal(3, WithPrecision(12))))
	err = output.Restore(buffer)
	assert.ErrorIs(t, err, errScaleMismatch)
}

func TestDecimalReplay_MatchingScaleReplicates(t *testing.T) {
	writer := make(commit.Channel, 16)
	primary := NewCollection(Options{Writer: writer})
	assert.NoError(t, primary.CreateColumn("amount", mustDecimal(2, WithPrecision(12))))

	replica := NewCollection()
	assert.NoError(t, replica.CreateColumn("amount", mustDecimal(2, WithPrecision(12))))

	// Stream commits into the replica as they are produced.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for change := range writer {
			assert.NoError(t, replica.Replay(change))
		}
	}()

	for _, text := range []string{"1.11", "2.22", "3.33"} {
		_, err := primary.Insert(func(r Row) error {
			return r.SetDecimalText("amount", text)
		})
		assert.NoError(t, err)
	}
	close(writer)
	<-done

	replica.Query(func(txn *Txn) error {
		assert.Equal(t, "6.66", txn.Decimal("amount").Sum().Text())
		return nil
	})
}

func TestDecimalReplay_ScaleMismatchRejected(t *testing.T) {
	writer := make(commit.Channel, 8)
	src := NewCollection(Options{Writer: writer})
	assert.NoError(t, src.CreateColumn("amount", mustDecimal(2, WithPrecision(12))))
	_, err := src.Insert(func(r Row) error {
		return r.SetDecimalText("amount", "1.11")
	})
	assert.NoError(t, err)

	var captured commit.Commit
	for change := range writer {
		captured = change
		break
	}

	// The logged batch must carry its scale marker.
	found := false
	for _, u := range captured.Updates {
		if u.Column == "amount" {
			scale, ok := u.ScaleMarker()
			assert.True(t, ok)
			assert.Equal(t, uint8(2), scale)
			found = true
		}
	}
	assert.True(t, found)

	// A scale-3 replica must reject the scale-2 batch.
	replica := NewCollection()
	assert.NoError(t, replica.CreateColumn("amount", mustDecimal(3, WithPrecision(12))))
	err = replica.Replay(captured)
	assert.ErrorIs(t, err, errScaleMismatch)
}

func TestDecimalBuffer_CarriesMarker(t *testing.T) {
	// The marker is written into the transaction buffer on the first write.
	col := NewCollection()
	assert.NoError(t, col.CreateColumn("amount", mustDecimal(2, WithPrecision(12))))
	col.Query(func(txn *Txn) error {
		accessor := txn.Decimal("amount")
		txn.cursor = 0
		assert.NoError(t, accessor.SetText("1.11"))

		buf := txn.bufferFor("amount")
		scale, ok := buf.ScaleMarker()
		assert.True(t, ok)
		assert.Equal(t, uint8(2), scale)
		return nil
	})
}

// --------------------------- Saturating Sum ----------------------------

func TestAddSaturating(t *testing.T) {
	assert.Equal(t, int64(maxInt64), addSaturating(maxInt64, 1))
	assert.Equal(t, int64(minInt64), addSaturating(minInt64, -1))
	assert.Equal(t, int64(5), addSaturating(2, 3))
}

func TestDecimal_ConcurrentMergeIsExact(t *testing.T) {
	col := NewCollection()
	assert.NoError(t, col.CreateColumn("amount", mustDecimal(2, WithPrecision(16))))

	idx, _ := col.Insert(func(r Row) error {
		return r.SetDecimalText("amount", "0.00")
	})

	// 100 goroutines each add 0.01 ten times -> exactly 10.00, with no float drift.
	var wg sync.WaitGroup
	for g := 0; g < 100; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				_ = col.QueryAt(idx, func(r Row) error {
					return r.MergeDecimal("amount", NewDecimal(1, 2))
				})
			}
		}()
	}
	wg.Wait()

	col.QueryAt(idx, func(r Row) error {
		d, ok := r.Decimal("amount")
		assert.True(t, ok)
		assert.Equal(t, "10.00", d.Text())
		return nil
	})
}

func TestDecimal_ExactTextExport(t *testing.T) {
	col := NewCollection()
	assert.NoError(t, col.CreateColumn("amount", mustDecimal(4, WithPrecision(14))))

	for _, text := range []string{"0.0001", "1234.5678", "-0.9999"} {
		_, err := col.Insert(func(r Row) error {
			return r.SetDecimalText("amount", text)
		})
		assert.NoError(t, err)
	}

	// Every value round-trips through exact text, suitable for direct export.
	col.Query(func(txn *Txn) error {
		acc := txn.Decimal("amount")
		var exported []string
		txn.Range(func(_ uint32) {
			text, ok := acc.Text()
			assert.True(t, ok)
			exported = append(exported, text)
		})
		assert.Equal(t, []string{"0.0001", "1234.5678", "-0.9999"}, exported)
		return nil
	})
}
