// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"errors"
	"math"
	"strconv"
	"strings"
)

// --------------------------- Policies ----------------------------

// RoundingPolicy decides what happens when a value carries more fractional
// digits than the column's scale. The extra digits must be discarded
// deterministically before the value is stored.
type RoundingPolicy uint8

const (
	// RoundHalfUp rounds towards the nearest neighbor, with ties rounded away
	// from zero. This is the default and matches decimal arithmetic used in
	// billing and accounting systems.
	RoundHalfUp RoundingPolicy = iota

	// RoundDown truncates the excess fractional digits towards zero.
	RoundDown

	// RoundUp rounds the retained fraction away from zero whenever any excess
	// fractional digit is present.
	RoundUp
)

// OverflowPolicy decides what happens when a value cannot fit into the
// column's declared precision.
type OverflowPolicy uint8

const (
	// OverflowPanic aborts the transaction with a panic on overflow. This is
	// the default: losing money silently is worse than failing loudly.
	OverflowPanic OverflowPolicy = iota

	// OverflowSaturate clamps the value to the largest (or smallest) number
	// representable by the column's precision instead of failing.
	OverflowSaturate

	// OverflowError returns an error from the typed set/merge accessors. On
	// the commit/replay path, where errors cannot be surfaced to the caller,
	// the value is saturated instead.
	OverflowError
)

// --------------------------- Errors ----------------------------

var (
	errEmptyValue    = errors.New("column: decimal value must not be empty")
	errInvalidNumber = errors.New("column: decimal value is not a valid number")
	errInvalidScale  = errors.New("column: decimal scale must be between 0 and 18")
	errInvalidPrec   = errors.New("column: decimal precision must be between the scale and 18")
	errOverflow      = errors.New("column: decimal overflow, value exceeds the declared precision")
	errScaleMismatch = errors.New("column: fixed-point scale mismatch, batches with different scaling cannot be treated as the same data")
)

// --------------------------- Decimal ----------------------------

// Decimal is a fixed-point decimal value stored as an unscaled integer. The
// real-world value equals Unscaled / 10^Scale, where Scale is defined by the
// owning column. Decimals are exact: they never accumulate floating-point
// error no matter how often they are merged.
//
// The zero value is a legitimate zero and is distinct from a missing column
// value, which is represented by the `ok == false` returned by every accessor.
type Decimal struct {
	Unscaled int64 // The unscaled integer value
	Scale    uint8 // The number of decimal digits (defined by the column)
}

// NewDecimal creates a fixed-point decimal from an unscaled integer and a
// scale. For example NewDecimal(12345, 2) represents 123.45.
func NewDecimal(unscaled int64, scale uint8) Decimal {
	return Decimal{Unscaled: unscaled, Scale: scale}
}

// DecimalFromInt64 creates a fixed-point decimal with no fractional part.
func DecimalFromInt64(value int64, scale uint8) Decimal {
	return Decimal{Unscaled: value * pow10Int64(scale), Scale: scale}
}

// DecimalFromFloat64 converts a float64 into an exact fixed-point decimal.
// The float is first rendered with its shortest round-trip textual form, so
// 0.1 + 0.2 supplied as a value is handled as "0.30000000000000004" rather
// than being silently accepted as exact. Rounding and overflow are resolved
// using the owning column's policies.
func DecimalFromFloat64(value float64, scale uint8, round RoundingPolicy, overflow OverflowPolicy) (Decimal, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return Decimal{}, errInvalidNumber
	}
	return parseDecimal(strconv.FormatFloat(value, 'f', -1, 64), scale, round, overflow)
}

// Text renders the decimal as an exact textual number with exactly Scale
// fractional digits. The output never uses exponent notation and is safe to
// hand directly to downstream exporters.
func (d Decimal) Text() string {
	return formatDecimal(d.Unscaled, d.Scale)
}

// String implements fmt.Stringer and returns the exact textual representation.
func (d Decimal) String() string {
	return d.Text()
}

// Float64 converts the decimal to a float64. Prefer Text() for export since
// the conversion may not be representable exactly in binary floating point.
func (d Decimal) Float64() float64 {
	return float64(d.Unscaled) / float64(pow10Int64(d.Scale))
}

// --------------------------- Scaling Helpers ----------------------------

// pow10Int64 returns 10^n as int64 for n in [0, 18].
func pow10Int64(n uint8) int64 {
	return int64(pow10Uint64Decimal[minUint8(n, 18)])
}

// pow10Uint64Decimal contains 10^n for n in [0, 18].
var pow10Uint64Decimal = [19]uint64{
	1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
	1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18,
}

func minUint8(a, b uint8) uint8 {
	if a < b {
		return a
	}
	return b
}

// digitCount returns the number of decimal digits needed to represent the
// absolute value of an unscaled integer in base 10.
func digitCount(v int64) int {
	if v == 0 {
		return 1
	}

	abs := uint64(v)
	if v < 0 {
		abs = uint64(-v) // Handles math.MinInt64 correctly in unsigned space
	}

	count := 0
	for abs > 0 {
		abs /= 10
		count++
	}
	return count
}

// --------------------------- Parsing ----------------------------

// parseDecimal parses an exact decimal text (no exponent notation) into an
// unscaled integer at the requested scale, applying the rounding and overflow
// policies.
func parseDecimal(text string, scale uint8, round RoundingPolicy, overflow OverflowPolicy) (Decimal, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return Decimal{}, errEmptyValue
	}

	negative := false
	switch text[0] {
	case '+':
		text = text[1:]
	case '-':
		negative = true
		text = text[1:]
	}
	if text == "" {
		return Decimal{}, errInvalidNumber
	}

	// Split the integer and fractional parts and validate every digit. A
	// leading or trailing dot with no digits on one side is invalid.
	intPart, fracPart := text, ""
	if dot := strings.IndexByte(text, '.'); dot >= 0 {
		intPart, fracPart = text[:dot], text[dot+1:]
		if intPart == "" {
			intPart = "0"
		}
		if fracPart == "" {
			return Decimal{}, errInvalidNumber
		}
	}
	if intPart == "" || !allDigits(intPart) || !allDigits(fracPart) {
		return Decimal{}, errInvalidNumber
	}

	// Accumulate the unscaled magnitude in unsigned space so that
	// math.MinInt64 does not overflow.
	var magnitude uint64
	apply := func(digit byte) error {
		if magnitude > (math.MaxUint64-uint64(digit-'0'))/10 {
			return errOverflow
		}
		magnitude = magnitude*10 + uint64(digit-'0')
		return nil
	}

	for i := 0; i < len(intPart); i++ {
		if err := apply(intPart[i]); err != nil {
			return Decimal{}, err
		}
	}

	// Scale the fractional part to the requested number of digits, rounding
	// off the excess.
	kept, dropped := truncateFraction(fracPart, int(scale))
	for i := 0; i < len(kept); i++ {
		if err := apply(kept[i]); err != nil {
			return Decimal{}, err
		}
	}
	for i := len(kept); i < int(scale); i++ {
		magnitude *= 10 // Pad trailing zeros up to the scale
	}

	// Resolve the rounding decision on the dropped tail.
	if decision := roundingDecision(round, dropped); decision != 0 {
		if decision > 0 {
			if magnitude == math.MaxUint64 {
				return Decimal{}, errOverflow
			}
			magnitude++
		}
	}

	// Convert back to a signed unscaled integer.
	if negative {
		if magnitude > uint64(math.MaxInt64)+1 {
			return Decimal{}, errOverflow
		}
	} else if magnitude > math.MaxInt64 {
		return Decimal{}, errOverflow
	}

	unscaled := int64(magnitude)
	if negative {
		unscaled = -unscaled
	}

	return Decimal{Unscaled: unscaled, Scale: scale}, nil
}

// allDigits reports whether the string is empty or consists solely of digits.
func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// truncateFraction keeps up to `scale` fractional digits and returns the
// retained digits together with the dropped tail.
func truncateFraction(fraction string, scale int) (kept, dropped string) {
	if len(fraction) <= scale {
		return fraction, ""
	}
	return fraction[:scale], fraction[scale:]
}

// roundingDecision interprets the dropped tail under the rounding policy and
// returns +1 to round away from zero, -1 to round towards zero, 0 to keep.
func roundingDecision(policy RoundingPolicy, dropped string) int {
	if dropped == "" || isAllZeros(dropped) {
		return 0 // Nothing of significance was dropped
	}

	switch policy {
	case RoundDown:
		return 0
	case RoundUp:
		return 1
	default: // RoundHalfUp
		if dropped[0] >= '5' {
			return 1
		}
		return 0
	}
}

// isAllZeros reports whether a digit string contains only zeros.
func isAllZeros(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != '0' {
			return false
		}
	}
	return true
}

// --------------------------- Formatting ----------------------------

// formatDecimal renders an unscaled integer with the given scale as an exact
// decimal text.
func formatDecimal(unscaled int64, scale uint8) string {
	negative := unscaled < 0
	abs := uint64(unscaled)
	if negative {
		abs = uint64(-unscaled)
	}

	digits := strconv.FormatUint(abs, 10)
	if scale == 0 {
		if negative {
			return "-" + digits
		}
		return digits
	}

	// Pad a leading zero so the integer part is never empty.
	if len(digits) <= int(scale) {
		digits = strings.Repeat("0", int(scale)-len(digits)+1) + digits
	}

	whole := digits[:len(digits)-int(scale)]
	fraction := digits[len(digits)-int(scale):]
	if negative {
		return "-" + whole + "." + fraction
	}
	return whole + "." + fraction
}
