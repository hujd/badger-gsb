/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import (
	"bytes"
	"encoding/binary"
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersionedKeyRoundTrip(t *testing.T) {
	original := []byte("path/to/key")
	timestamps := []uint64{0, 1, 42, 1_000_000, math.MaxUint64 - 1, math.MaxUint64}

	for _, ts := range timestamps {
		encoded := KeyWithTs(original, ts)
		require.Len(t, encoded, len(original)+8)
		require.Equal(t, original, encoded[:len(original)])

		parsedKey := ParseKey(encoded)
		require.Len(t, parsedKey, len(original))
		require.Equal(t, original, parsedKey)
		require.Equal(t, ts, ParseTs(encoded))

		require.Len(t, append(parsedKey, []byte("/suffix")...), len(original)+7)
	}
}

func TestVersionedKeyTimestampEncoding(t *testing.T) {
	encoded := KeyWithTs([]byte("key"), 42)
	expectedSuffix := make([]byte, 8)
	binary.BigEndian.PutUint64(expectedSuffix, math.MaxUint64-42)
	require.Equal(t, expectedSuffix, encoded[len("key"):])

	older := KeyWithTs([]byte("key"), 10)
	newer := KeyWithTs([]byte("key"), 11)
	require.Equal(t, uint64(10), ParseTs(older))
	require.Equal(t, uint64(11), ParseTs(newer))
	require.Equal(t, 1, bytes.Compare(older[len(older)-8:], newer[len(newer)-8:]))
}

func TestCompareKeys(t *testing.T) {
	older := KeyWithTs([]byte("same"), 10)
	newer := KeyWithTs([]byte("same"), 11)

	require.Equal(t, 1, CompareKeys(older, newer))
	require.Equal(t, -1, CompareKeys(newer, older))
	require.Zero(t, CompareKeys(older, older))

	keys := [][]byte{older, newer}
	sort.Slice(keys, func(i, j int) bool {
		return CompareKeys(keys[i], keys[j]) < 0
	})
	require.Equal(t, [][]byte{newer, older}, keys)

	require.Equal(t, -1, CompareKeys(KeyWithTs([]byte("a"), 100), KeyWithTs([]byte("aa"), 1)))
	require.Equal(t, -1, CompareKeys(KeyWithTs([]byte("a/10"), 1), KeyWithTs([]byte("a/9"), 1)))
}

func TestSameKey(t *testing.T) {
	keyAt10 := KeyWithTs([]byte("same"), 10)
	keyAt11 := KeyWithTs([]byte("same"), 11)
	other := KeyWithTs([]byte("other"), 10)

	require.True(t, SameKey(keyAt10, keyAt11))
	require.False(t, SameKey(keyAt10, other))
	require.False(t, SameKey(keyAt10, []byte("short")))
}

func TestParseShortKey(t *testing.T) {
	require.Nil(t, ParseKey([]byte("short")))
	require.Zero(t, ParseTs([]byte("short")))
	require.Equal(t, []byte{}, ParseKey(make([]byte, 8)))
}

func TestFixedIntegerEncoding(t *testing.T) {
	require.Equal(t, []byte{0x12, 0x34}, U16ToBytes(0x1234))
	require.Equal(t, uint16(0x1234), BytesToU16([]byte{0x12, 0x34}))

	require.Equal(t, []byte{0x12, 0x34, 0x56, 0x78}, U32ToBytes(0x12345678))
	require.Equal(t, uint32(0x12345678), BytesToU32([]byte{0x12, 0x34, 0x56, 0x78}))

	require.Equal(t, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, U64ToBytes(0x0102030405060708))
	require.Equal(t, uint64(0x0102030405060708), BytesToU64([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}))
}

func TestU32SliceByteConversion(t *testing.T) {
	values := []uint32{0x01020304, 0x05060708, 0xffffffff}
	raw := U32SliceToBytes(values)

	require.Len(t, raw, len(values)*4)
	require.Len(t, BytesToU32Slice(raw), len(values))
	require.Equal(t, values, BytesToU32Slice(raw))

	expected := make([]byte, len(values)*4)
	for i, value := range values {
		binary.NativeEndian.PutUint32(expected[i*4:], value)
	}
	require.Equal(t, expected, raw)
	require.Nil(t, U32SliceToBytes(nil))
	require.Nil(t, BytesToU32Slice(nil))
}

func TestU64SliceByteConversion(t *testing.T) {
	values := []uint64{0x0102030405060708, 0x1122334455667788}
	raw := U64SliceToBytes(values)

	require.Len(t, raw, len(values)*8)
	require.Len(t, BytesToU64Slice(raw), len(values))
	require.Equal(t, values, BytesToU64Slice(raw))

	expected := make([]byte, len(values)*8)
	for i, value := range values {
		binary.NativeEndian.PutUint64(expected[i*8:], value)
	}
	require.Equal(t, expected, raw)
	require.Nil(t, U64SliceToBytes(nil))
	require.Nil(t, BytesToU64Slice(nil))
}

func TestSliceResizeReusesBackingArray(t *testing.T) {
	var reusable Slice

	first := reusable.Resize(5)
	require.Len(t, first, 5)
	copy(first, []byte("abcde"))

	second := reusable.Resize(3)
	require.Len(t, second, 3)
	require.Equal(t, []byte("abc"), second)
	require.Equal(t, &first[0], &second[0])

	grown := reusable.Resize(64)
	require.Len(t, grown, 64)
	require.NotEqual(t, &first[0], &grown[0])
}
