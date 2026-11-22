// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)

// Ensure the fixed-point column satisfies the numeric contract.
var (
	_ Column  = new(columnDecimal)
	_ Numeric = new(columnDecimal)
	_ Scaled  = new(columnDecimal)
)

// --------------------------- Options ----------------------------

// decimalConfig captures the declared definition of a fixed-point column.
type decimalConfig struct {
	precision uint8          // Total number of significant digits
	scale     uint8          // Number of digits after the decimal point
	rounding  RoundingPolicy // How excess fractional digits are handled
	overflow  OverflowPolicy // How out-of-range values are handled
}

// WithPrecision declares the total number of significant decimal digits the
// column can hold. It must be at least the scale and at most 18.
func WithPrecision(precision uint8) func(*decimalConfig) {
	return func(c *decimalConfig) {
		c.precision = precision
	}
}

// WithRounding declares how values with more fractional digits than the
// column's scale are handled. Defaults to RoundHalfUp.
func WithRounding(policy RoundingPolicy) func(*decimalConfig) {
	return func(c *decimalConfig) {
		c.rounding = policy
	}
}

// WithOverflow declares how values exceeding the column's precision are
// handled. Defaults to OverflowPanic.
func WithOverflow(policy OverflowPolicy) func(*decimalConfig) {
	return func(c *decimalConfig) {
		c.overflow = policy
	}
}

// configureDecimal validates and applies the column options.
func configureDecimal(scale uint8, opts []func(*decimalConfig)) (decimalConfig, error) {
	cfg := decimalConfig{
		precision: 18,
		scale:     scale,
		rounding:  RoundHalfUp,
		overflow:  OverflowPanic,
	}
	for _, fn := range opts {
		fn(&cfg)
	}
	cfg.scale = scale

	if scale > 18 {
		return cfg, errInvalidScale
	}
	if cfg.precision < scale || cfg.precision > 18 {
		return cfg, errInvalidPrec
	}
	return cfg, nil
}

// --------------------------- Scaled Contract ----------------------------

// Scaled is implemented by columns that store fixed-point values. It exposes
// the scaling definition so snapshots, the commit log and replay can refuse to
// mix batches that were encoded with different scaling.
type Scaled interface {
	Scale() uint8
}

// --------------------------- Column ----------------------------

// columnDecimal stores exact fixed-point decimals as unscaled int64 values.
type columnDecimal struct {
	chunks[int64]
	cfg decimalConfig
	min int64 // Smallest representable unscaled value
	max int64 // Largest representable unscaled value
}

// ForDecimal creates a fixed-point decimal column. The scale declares how many
// digits of every value live after the decimal point and is part of the
// column's identity: it is recorded in snapshots and commit logs, and restoring
// or replaying a batch encoded with a different scale is rejected.
func ForDecimal(scale uint8, opts ...func(*decimalConfig)) (Column, error) {
	cfg, err := configureDecimal(scale, opts)
	if err != nil {
		return nil, err
	}

	limit := pow10Int64(cfg.precision) - 1
	return &columnDecimal{
		chunks: make(chunks[int64], 0, 4),
		cfg:    cfg,
		min:    -limit,
		max:    limit,
	}, nil
}

// Scale returns the number of decimal digits declared by the column.
func (c *columnDecimal) Scale() uint8 {
	return c.cfg.scale
}

// Precision returns the total number of significant digits declared by the column.
func (c *columnDecimal) Precision() uint8 {
	return c.cfg.precision
}

// --------------------------- Reads ----------------------------

// Contains checks whether the column has a value at a specified index.
func (c *columnDecimal) Contains(idx uint32) bool {
	chunk := commit.ChunkAt(idx)
	return c.chunks[chunk].fill.Contains(idx - chunk.Min())
}

// load retrieves the unscaled value at a specified index.
func (c *columnDecimal) load(idx uint32) (int64, bool) {
	chunk := commit.ChunkAt(idx)
	index := idx - chunk.Min()
	if int(chunk) < len(c.chunks) && c.chunks[chunk].fill.Contains(index) {
		return c.chunks[chunk].data[index], true
	}
	return 0, false
}

// LoadDecimal retrieves the exact fixed-point value at a specified index. The
// second return value distinguishes a legitimate stored zero from a missing
// value.
func (c *columnDecimal) LoadDecimal(idx uint32) (Decimal, bool) {
	unscaled, ok := c.load(idx)
	return Decimal{Unscaled: unscaled, Scale: c.cfg.scale}, ok
}

// Value retrieves a value at a specified index, satisfying the Column contract.
func (c *columnDecimal) Value(idx uint32) (any, bool) {
	return c.LoadDecimal(idx)
}

// LoadFloat64 retrieves a scaled float64 value at a specified index.
func (c *columnDecimal) LoadFloat64(idx uint32) (float64, bool) {
	unscaled, ok := c.load(idx)
	if !ok {
		return 0, false
	}
	return float64(unscaled) / float64(pow10Int64(c.cfg.scale)), true
}

// LoadInt64 retrieves the unscaled int64 value at a specified index.
func (c *columnDecimal) LoadInt64(idx uint32) (int64, bool) {
	return c.load(idx)
}

// LoadUint64 retrieves the unscaled value as uint64 at a specified index.
func (c *columnDecimal) LoadUint64(idx uint32) (uint64, bool) {
	v, ok := c.load(idx)
	return uint64(v), ok
}

// --------------------------- Filtering ----------------------------

// FilterDecimal filters the selection using an exact fixed-point predicate.
func (c *columnDecimal) FilterDecimal(chunk commit.Chunk, index bitmap.Bitmap, predicate func(Decimal) bool) {
	if int(chunk) >= len(c.chunks) {
		return
	}

	fill, data := c.chunkAt(chunk)
	index.And(fill)
	index.Filter(func(idx uint32) bool {
		return predicate(Decimal{Unscaled: data[idx], Scale: c.cfg.scale})
	})
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *columnDecimal) FilterFloat64(chunk commit.Chunk, index bitmap.Bitmap, predicate func(float64) bool) {
	if int(chunk) >= len(c.chunks) {
		return
	}

	divisor := float64(pow10Int64(c.cfg.scale))
	fill, data := c.chunkAt(chunk)
	index.And(fill)
	index.Filter(func(idx uint32) bool {
		return predicate(float64(data[idx]) / divisor)
	})
}

// FilterInt64 filters on the unscaled integer representation.
func (c *columnDecimal) FilterInt64(chunk commit.Chunk, index bitmap.Bitmap, predicate func(int64) bool) {
	if int(chunk) < len(c.chunks) {
		fill, data := c.chunkAt(chunk)
		index.And(fill)
		index.Filter(func(idx uint32) bool {
			return predicate(data[idx])
		})
	}
}

// FilterUint64 filters on the unscaled integer representation.
func (c *columnDecimal) FilterUint64(chunk commit.Chunk, index bitmap.Bitmap, predicate func(uint64) bool) {
	if int(chunk) < len(c.chunks) {
		fill, data := c.chunkAt(chunk)
		index.And(fill)
		index.Filter(func(idx uint32) bool {
			return predicate(uint64(data[idx]))
		})
	}
}

// --------------------------- Apply & Snapshot ----------------------------

// Apply applies a set of operations to the column.
func (c *columnDecimal) Apply(chunk commit.Chunk, r *commit.Reader) {
	// Defensive check: a batch carrying a scale marker must use the same
	// scaling definition as the column. The collection-level replay guard
	// normally rejects this first. Marker-less buffers use the column's scale.
	if scale := r.Scale(); scale != 0 && scale != c.cfg.scale {
		panic(fmt.Errorf("column: fixed-point scale mismatch at apply (column=%d, batch=%d)",
			c.cfg.scale, scale))
	}

	fill, data := c.chunkAt(chunk)
	for r.Next() {
		offset := r.IndexAtChunk()
		switch r.Type {
		case commit.Put:
			fill[offset>>6] |= 1 << (offset & 0x3f)
			data[offset] = c.guard(r.Int64())
		case commit.Merge:
			fill[offset>>6] |= 1 << (offset & 0x3f)
			merged := data[offset] + r.Int64()
			data[offset] = r.SwapInt64(c.guard(merged))
		case commit.Delete:
			fill.Remove(offset)
		}
	}
}

// guard resolves an out-of-range unscaled value under the overflow policy.
// On the apply path OverflowError cannot be surfaced, so it saturates.
func (c *columnDecimal) guard(value int64) int64 {
	if value >= c.min && value <= c.max {
		return value
	}

	switch c.cfg.overflow {
	case OverflowSaturate, OverflowError:
		return c.clamp(value)
	default:
		panic(errOverflow)
	}
}

// guardTxn resolves an out-of-range value from a typed accessor, where an
// error can still be returned to the caller.
func (c *columnDecimal) guardTxn(value int64) (int64, error) {
	if value >= c.min && value <= c.max {
		return value, nil
	}

	switch c.cfg.overflow {
	case OverflowSaturate:
		return c.clamp(value), nil
	case OverflowError:
		return c.clamp(value), errOverflow
	default:
		panic(errOverflow)
	}
}

// clamp bounds an unscaled value to the representable range.
func (c *columnDecimal) clamp(value int64) int64 {
	if value < c.min {
		return c.min
	}
	if value > c.max {
		return c.max
	}
	return value
}

// Snapshot writes the entire column into the destination buffer. The buffer is
// prefixed with the column's scale marker so the snapshot stays self-describing.
func (c *columnDecimal) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	dst.PutScaleMarker(c.cfg.scale)
	fill, data := c.chunkAt(chunk)
	fill.Range(func(x uint32) {
		dst.PutInt64(commit.Put, chunk.Min()+x, data[x])
	})
}

// --------------------------- Accessors ----------------------------

// rdDecimal represents a read-only accessor for fixed-point decimals.
type rdDecimal struct {
	reader *columnDecimal
	txn    *Txn
}

// Get loads the exact decimal at the current transaction cursor.
func (s rdDecimal) Get() (Decimal, bool) {
	return s.reader.LoadDecimal(s.txn.cursor)
}

// GetInt64 loads the unscaled integer at the current transaction cursor.
func (s rdDecimal) GetInt64() (int64, bool) {
	return s.reader.load(s.txn.cursor)
}

// GetFloat64 loads a scaled float64 at the current transaction cursor.
func (s rdDecimal) GetFloat64() (float64, bool) {
	return s.reader.LoadFloat64(s.txn.cursor)
}

// Text renders the exact decimal text at the current transaction cursor. A
// missing value returns ok == false, distinct from a legitimate zero.
func (s rdDecimal) Text() (string, bool) {
	unscaled, ok := s.reader.load(s.txn.cursor)
	if !ok {
		return "", false
	}
	return formatDecimal(unscaled, s.reader.cfg.scale), true
}

// Sum computes the exact sum of the selected unscaled values, saturating if
// the int64 accumulator overflows. Use SumBig for an arbitrary-precision sum.
func (s rdDecimal) Sum() Decimal {
	s.txn.initialize()
	var sum int64
	s.txn.rangeRead(func(chunk commit.Chunk, index bitmap.Bitmap) {
		if int(chunk) < len(s.reader.chunks) {
			sum = addSaturating(sum, bitmap.Sum(s.reader.chunks[chunk].data, index))
		}
	})
	return Decimal{Unscaled: sum, Scale: s.reader.cfg.scale}
}

// SumBig computes the sum of the selected values using arbitrary-precision
// arithmetic. The returned Decimal is clamped to int64 when the exact sum
// exceeds that range; use SumText() to export the unclamped exact result.
func (s rdDecimal) SumBig() Decimal {
	s.txn.initialize()
	var big bigInt
	s.txn.rangeRead(func(chunk commit.Chunk, index bitmap.Bitmap) {
		if int(chunk) < len(s.reader.chunks) {
			data := s.reader.chunks[chunk].data
			index.Range(func(idx uint32) {
				big.add(data[idx])
			})
		}
	})

	return Decimal{
		Unscaled: clampBigToInt64(&big),
		Scale:    s.reader.cfg.scale,
	}
}

// SumText renders the exact arbitrary-precision sum as decimal text, even when
// the result exceeds int64. This is the export-safe aggregate.
func (s rdDecimal) SumText() string {
	s.txn.initialize()
	var big bigInt
	s.txn.rangeRead(func(chunk commit.Chunk, index bitmap.Bitmap) {
		if int(chunk) < len(s.reader.chunks) {
			data := s.reader.chunks[chunk].data
			index.Range(func(idx uint32) {
				big.add(data[idx])
			})
		}
	})
	return big.format(s.reader.cfg.scale)
}

// Min finds the smallest selected value.
func (s rdDecimal) Min() (Decimal, bool) {
	s.txn.initialize()
	var min int64
	var ok bool
	s.txn.rangeRead(func(chunk commit.Chunk, index bitmap.Bitmap) {
		if int(chunk) < len(s.reader.chunks) {
			if v, hit := bitmap.Min(s.reader.chunks[chunk].data, index); hit && (v < min || !ok) {
				min, ok = v, true
			}
		}
	})
	return Decimal{Unscaled: min, Scale: s.reader.cfg.scale}, ok
}

// Max finds the largest selected value.
func (s rdDecimal) Max() (Decimal, bool) {
	s.txn.initialize()
	var max int64
	var ok bool
	s.txn.rangeRead(func(chunk commit.Chunk, index bitmap.Bitmap) {
		if int(chunk) < len(s.reader.chunks) {
			if v, hit := bitmap.Max(s.reader.chunks[chunk].data, index); hit && (v > max || !ok) {
				max, ok = v, true
			}
		}
	})
	return Decimal{Unscaled: max, Scale: s.reader.cfg.scale}, ok
}

// rwDecimal represents a read-write accessor for fixed-point decimals.
type rwDecimal struct {
	rdDecimal
	writer *commit.Buffer
}

// mark records the column's scaling definition into the transaction buffer on
// its first write. Every batch produced for this column stays self-describing.
func (s rwDecimal) mark() {
	s.writer.PutScaleMarker(s.reader.cfg.scale)
}

// SetText parses and stores an exact decimal text value.
func (s rwDecimal) SetText(value string) error {
	parsed, err := parseDecimal(value, s.reader.cfg.scale, s.reader.cfg.rounding, s.reader.cfg.overflow)
	if err != nil {
		return err
	}
	return s.Set(parsed)
}

// SetFloat64 converts and stores a float64 under the column's policies.
func (s rwDecimal) SetFloat64(value float64) error {
	parsed, err := DecimalFromFloat64(value, s.reader.cfg.scale, s.reader.cfg.rounding, s.reader.cfg.overflow)
	if err != nil {
		return err
	}
	return s.Set(parsed)
}

// Set stores an already-scaled Decimal. The value must share the column's
// scale; it is then validated against the declared precision.
func (s rwDecimal) Set(value Decimal) error {
	if value.Scale != s.reader.cfg.scale {
		return errScaleMismatch
	}

	guarded, err := s.reader.guardTxn(value.Unscaled)
	if err != nil {
		return err
	}

	s.mark()
	s.writer.PutInt64(commit.Put, s.txn.cursor, guarded)
	return nil
}

// SetInt64 stores an unscaled integer directly, validating the precision.
func (s rwDecimal) SetInt64(unscaled int64) error {
	guarded, err := s.reader.guardTxn(unscaled)
	if err != nil {
		return err
	}

	s.mark()
	s.writer.PutInt64(commit.Put, s.txn.cursor, guarded)
	return nil
}

// Merge atomically adds an already-scaled delta to the value at the cursor.
// The validated delta is appended to the transaction log; running-value
// overflow is enforced deterministically at apply time, so repeated merges in
// one transaction compose correctly instead of each guarding against the
// committed value alone.
func (s rwDecimal) Merge(delta Decimal) error {
	if delta.Scale != s.reader.cfg.scale {
		return errScaleMismatch
	}

	// Validate the delta itself against the declared precision.
	guarded, err := s.reader.guardTxn(delta.Unscaled)
	if err != nil {
		return err
	}

	s.mark()
	s.writer.PutInt64(commit.Merge, s.txn.cursor, guarded)
	return nil
}

// MergeFloat64 atomically adds a float64 delta, parsed under the column's
// policies so that no binary floating-point error enters the stored value.
func (s rwDecimal) MergeFloat64(delta float64) error {
	parsed, err := DecimalFromFloat64(delta, s.reader.cfg.scale, s.reader.cfg.rounding, s.reader.cfg.overflow)
	if err != nil {
		return err
	}
	return s.Merge(parsed)
}

// Decimal returns a read-write accessor for a fixed-point decimal column.
func (txn *Txn) Decimal(columnName string) rwDecimal {
	column, ok := txn.columnAt(columnName)
	if !ok {
		panic(fmt.Errorf("column: column '%s' does not exist", columnName))
	}

	reader, ok := column.Column.(*columnDecimal)
	if !ok {
		panic(fmt.Errorf("column: column '%s' is not a fixed-point decimal column", columnName))
	}

	return rwDecimal{
		rdDecimal: rdDecimal{
			reader: reader,
			txn:    txn,
		},
		writer: txn.bufferFor(columnName),
	}
}

// WithDecimal filters the selection using an exact fixed-point predicate.
func (txn *Txn) WithDecimal(columnName string, predicate func(Decimal) bool) *Txn {
	txn.initialize()
	c, ok := txn.columnAt(columnName)
	if !ok {
		txn.index.Clear()
		return txn
	}

	decimal, ok := c.Column.(*columnDecimal)
	if !ok {
		txn.index.Clear()
		return txn
	}

	txn.rangeRead(func(chunk commit.Chunk, index bitmap.Bitmap) {
		decimal.FilterDecimal(chunk, index, predicate)
	})
	return txn
}

// --------------------------- Saturating Arithmetic ----------------------------

// addSaturating sums two int64 values, clamping on overflow instead of wrapping.
func addSaturating(a, b int64) int64 {
	sum := a + b
	// Overflow changes the sign of the result relative to both operands.
	if a > 0 && b > 0 && sum < 0 {
		return maxInt64
	}
	if a < 0 && b < 0 && sum >= 0 {
		return minInt64
	}
	return sum
}

const (
	minInt64 = -1 << 63
	maxInt64 = 1<<63 - 1
)
