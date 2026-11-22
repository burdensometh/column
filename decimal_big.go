// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"math/big"
	"strings"
)

// bigInt is a thin wrapper around math/big.Int used to accumulate exact
// fixed-point sums whose magnitude may exceed the int64 storage type. It is
// intentionally small: only the operations the decimal column needs are
// exposed.
type bigInt struct {
	value big.Int
}

// add accumulates another unscaled value into the running sum.
func (b *bigInt) add(v int64) {
	b.value.Add(&b.value, big.NewInt(v))
}

// format renders the accumulated sum as exact decimal text at the given
// scale, regardless of whether the magnitude fits into int64.
func (b *bigInt) format(scale uint8) string {
	text := b.value.String()
	if scale == 0 {
		return text
	}

	negative := strings.HasPrefix(text, "-")
	digits := text
	if negative {
		digits = text[1:]
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

// clampBigToInt64 bounds an arbitrary-precision sum to the int64 range.
func clampBigToInt64(b *bigInt) int64 {
	if b.value.IsInt64() {
		return b.value.Int64()
	}
	if b.value.Sign() < 0 {
		return minInt64
	}
	return maxInt64
}
