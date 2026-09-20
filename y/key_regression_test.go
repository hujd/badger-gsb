package y

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestKeyWithTsRoundTripAndExactEncoding(t *testing.T) {
	original := []byte("vertex/collection/key")

	for _, ts := range []uint64{0, 1, 42, 1 << 40, math.MaxUint64 - 1, math.MaxUint64} {
		encoded := KeyWithTs(original, ts)
		require.Equal(t, len(original)+8, len(encoded), "timestamp must add exactly eight bytes")
		require.Equal(t, original, encoded[:len(original)])

		var suffix [8]byte
		binary.BigEndian.PutUint64(suffix[:], math.MaxUint64-ts)
		require.Equal(t, suffix[:], encoded[len(original):])
		require.Equal(t, ts, ParseTs(encoded))
	}
}

func TestParseKeyReturnsExactlyOriginalKey(t *testing.T) {
	original := []byte("path/prefix")
	encoded := KeyWithTs(original, 12345)

	parsed := ParseKey(encoded)
	require.Equal(t, original, parsed)
	require.Equal(t, len(original), len(parsed), "parsed key must not retain any timestamp byte")
	require.Nil(t, ParseKey([]byte("short")))
}

func TestCompareKeysOrdersByUserKeyThenNewestTimestampFirst(t *testing.T) {
	older := KeyWithTs([]byte("same-prefix"), 1)
	newer := KeyWithTs([]byte("same-prefix"), 2)

	require.Positive(t, CompareKeys(older, newer))
	require.Negative(t, CompareKeys(newer, older))
	require.Zero(t, CompareKeys(older, older))

	require.Negative(t, CompareKeys(KeyWithTs([]byte("a"), 999), KeyWithTs([]byte("aa"), 1)))
	require.True(t, SameKey(older, newer))
	require.False(t, SameKey(older, KeyWithTs([]byte("other"), 1)))
	require.False(t, SameKey(older, newer[:len(newer)-1]))
}

func TestFixedIntegerConversions(t *testing.T) {
	require.Equal(t, []byte{0x12, 0x34}, U16ToBytes(0x1234))
	require.Equal(t, uint16(0x1234), BytesToU16([]byte{0x12, 0x34}))

	require.Equal(t, []byte{0x12, 0x34, 0x56, 0x78}, U32ToBytes(0x12345678))
	require.Equal(t, uint32(0x12345678), BytesToU32([]byte{0x12, 0x34, 0x56, 0x78}))

	require.Equal(t, []byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}, U64ToBytes(0x123456789abcdef0))
	require.Equal(t, uint64(0x123456789abcdef0), BytesToU64([]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}))
}

func TestU32SliceBytesIsFourBytesPerValue(t *testing.T) {
	values := []uint32{1, 2, 1 << 20, math.MaxUint32}
	originalFirst := values[0]
	raw := U32SliceToBytes(values)

	require.Equal(t, 4*len(values), len(raw), "serializing uint32 values must use four bytes each")
	require.Equal(t, 4*len(values), cap(raw))

	var expected []byte
	for _, value := range values {
		var buf [4]byte
		binary.NativeEndian.PutUint32(buf[:], value)
		expected = append(expected, buf[:]...)
	}
	require.Equal(t, expected, raw)
	require.Equal(t, values, BytesToU32Slice(raw))

	raw[0] ^= 0xff
	require.Equal(t, originalFirst^0xff, BytesToU32Slice(raw)[0], "conversion must return a view over the input bytes")
	require.Nil(t, U32SliceToBytes(nil))
	require.Nil(t, BytesToU32Slice(nil))

	partial := BytesToU32Slice([]byte{1, 2, 3})
	require.Len(t, partial, 0)
}

func TestU64SliceBytesIsEightBytesPerValue(t *testing.T) {
	values := []uint64{1, 2, 1 << 40, math.MaxUint64}
	originalFirst := values[0]
	raw := U64SliceToBytes(values)

	require.Equal(t, 8*len(values), len(raw))
	require.Equal(t, 8*len(values), cap(raw))
	require.Equal(t, values, BytesToU64Slice(raw))

	raw[0] ^= 0xff
	require.Equal(t, originalFirst^0xff, BytesToU64Slice(raw)[0])
	require.Nil(t, U64SliceToBytes(nil))
	require.Nil(t, BytesToU64Slice(nil))
}

func TestSliceResizeAndCopy(t *testing.T) {
	var reusable Slice

	first := reusable.Resize(4)
	require.Len(t, first, 4)
	copy(first, "data")

	second := reusable.Resize(3)
	require.Len(t, second, 3)
	require.Equal(t, "dat", string(second))
	require.Equal(t, &first[0], &second[0])

	larger := reusable.Resize(20)
	require.Len(t, larger, 20)

	source := []byte("independent")
	copied := Copy(source)
	copied[0] = 'X'
	require.Equal(t, "independent", string(source))
	require.Equal(t, "Xndependent", string(copied))
}

func TestFixedDuration(t *testing.T) {
	require.Equal(t, "00s", FixedDuration(0))
	require.Equal(t, "05s", FixedDuration(5*time.Second))
	require.Equal(t, "01m15s", FixedDuration(75*time.Second))
	require.Equal(t, "01h01m01s", FixedDuration(time.Hour+time.Minute+time.Second))
}

func TestThrottlePropagatesWorkerError(t *testing.T) {
	expected := errors.New("worker failed")
	throttle := NewThrottle(1)

	require.NoError(t, throttle.Do())
	throttle.Done(expected)

	require.ErrorIs(t, throttle.Finish(), expected)
	require.ErrorIs(t, throttle.Finish(), expected, "later Finish calls must retain the first failure")
}

func TestThrottleUnblocksWaitingWorkerWithError(t *testing.T) {
	expected := errors.New("stop waiting workers")
	throttle := NewThrottle(0)

	done := make(chan struct{})
	go func() {
		throttle.errCh <- expected
		close(done)
	}()

	require.ErrorIs(t, throttle.Do(), expected)
	<-done
	require.NoError(t, throttle.Finish())
}

func TestThrottleSuccessfulWorkers(t *testing.T) {
	throttle := NewThrottle(3)
	var wg sync.WaitGroup

	for i := 0; i < 18; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, throttle.Do())
			throttle.Done(nil)
		}()
	}

	wg.Wait()
	require.NoError(t, throttle.Finish())
}

func TestThrottleDoneWithoutDoPanics(t *testing.T) {
	throttle := NewThrottle(1)
	require.Panics(t, func() { throttle.Done(nil) })
}

func TestPageBufferWriteByteLenAndWriteTo(t *testing.T) {
	buffer := NewPageBuffer(4)
	require.NoError(t, buffer.WriteByte('a'))
	_, err := buffer.Write([]byte("bcdefgh"))
	require.NoError(t, err)
	require.Equal(t, 8, buffer.Len())

	var written bytes.Buffer
	n, err := buffer.WriteTo(&written)
	require.NoError(t, err)
	require.Equal(t, int64(8), n)
	require.Equal(t, "abcdefgh", written.String())

	_, err = buffer.WriteTo(failingWriter{consumed: 2})
	require.Error(t, err)
}

type failingWriter struct {
	consumed int
}

func (w failingWriter) Write(p []byte) (int, error) {
	if len(p) < w.consumed {
		w.consumed = len(p)
	}
	return w.consumed, errors.New("write failed")
}

func TestNewKVWithoutAllocator(t *testing.T) {
	kv := NewKV(nil)
	require.NotNil(t, kv)
	kv.Key = []byte("key")
	require.Equal(t, "key", string(kv.Key))
}

func TestIBytesToString(t *testing.T) {
	require.Equal(t, "9 B", IBytesToString(9, 1))
	require.Equal(t, "10 B", IBytesToString(10, 0))
	require.Equal(t, "1.0 KiB", IBytesToString(1024, 1))
	require.Equal(t, "1.5 KiB", IBytesToString(1536, 1))
	require.Equal(t, "1.0 MiB", IBytesToString(1<<20, 1))
}

func TestRateMonitor(t *testing.T) {
	monitor := NewRateMonitor(2)
	require.EqualValues(t, 0, monitor.Rate())

	monitor.Capture(100)
	time.Sleep(20 * time.Millisecond)
	monitor.Capture(300)

	require.Positive(t, monitor.Rate())
}

func TestPageBufferReaderAtOffset(t *testing.T) {
	buffer := NewPageBuffer(4)
	_, err := buffer.Write([]byte("0123456789"))
	require.NoError(t, err)

	reader := buffer.NewReaderAt(7)
	got := make([]byte, 3)
	n, err := reader.Read(got)
	require.Equal(t, 3, n)
	require.True(t, errors.Is(err, io.EOF) || err == nil)
	require.Equal(t, "789", string(got))

	n, err = reader.Read(got)
	require.Zero(t, n)
	require.ErrorIs(t, err, io.EOF)
}

func TestErrorWrappersWithoutFatalFailure(t *testing.T) {
	Check(nil)
	Check2("ignored value", nil)
	AssertTrue(true)
	AssertTruef(true, "message %d", 1)

	require.NoError(t, Wrap(nil, "context"))
	err := Wrap(errors.New("boom"), "context")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "context"))
	require.True(t, strings.Contains(err.Error(), "boom"))
}
