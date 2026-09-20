/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import (
	"bytes"
	"crypto/aes"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/pb"
)

type failingWriter struct{ failAfter int }

func (w failingWriter) Write(p []byte) (int, error) {
	if w.failAfter == 0 {
		return 0, errors.New("write failed")
	}
	if w.failAfter <= len(p) {
		return w.failAfter, io.ErrShortWrite
	}
	return len(p), nil
}

func TestCopyAndSafeCopyDoNotAliasSource(t *testing.T) {
	source := []byte("source")
	copied := Copy(source)
	require.Equal(t, source, copied)

	reused := []byte("old-data")
	safe := SafeCopy(reused[:0], source)
	require.Equal(t, source, safe)

	source[0] = 'x'
	require.Equal(t, []byte("source"), copied)
	require.Equal(t, []byte("source"), safe)
}

func TestFixedDuration(t *testing.T) {
	require.Equal(t, "00s", FixedDuration(0))
	require.Equal(t, "05s", FixedDuration(5*time.Second))
	require.Equal(t, "02m05s", FixedDuration(2*time.Minute+5*time.Second))
	require.Equal(t, "01h02m03s", FixedDuration(time.Hour+2*time.Minute+3*time.Second))
	require.Equal(t, "25h00m00s", FixedDuration(25*time.Hour))
}

func TestPageBufferWriteByteLenAndWriteTo(t *testing.T) {
	buffer := NewPageBuffer(4)

	require.NoError(t, buffer.WriteByte('a'))
	_, err := buffer.Write([]byte("bcdefghij"))
	require.NoError(t, err)

	require.Equal(t, 10, buffer.Len())
	require.Equal(t, []byte("abcdefghij"), buffer.Bytes())

	var output bytes.Buffer
	n, err := buffer.WriteTo(&output)
	require.NoError(t, err)
	require.Equal(t, int64(10), n)
	require.Equal(t, []byte("abcdefghij"), output.Bytes())

	_, err = buffer.WriteTo(failingWriter{})
	require.Error(t, err)
}

func TestPageBufferReaderStartsInsideLaterPage(t *testing.T) {
	buffer := NewPageBuffer(4)
	_, err := buffer.Write([]byte("abcdefghij"))
	require.NoError(t, err)

	reader := buffer.NewReaderAt(5)
	output, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, []byte("fghij"), output)
}

func TestFileHelpers(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "data")

	file, err := CreateSyncedFile(name, true)
	require.NoError(t, err)
	_, err = file.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, file.Close())

	_, err = CreateSyncedFile(name, true)
	require.Error(t, err)

	file, err = OpenExistingFile(name, 0)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	readOnly, err := OpenExistingFile(name, ReadOnly)
	require.NoError(t, err)
	_, err = readOnly.Write([]byte("x"))
	require.Error(t, err)
	require.NoError(t, readOnly.Close())

	require.NoError(t, os.WriteFile(filepath.Join(dir, "other"), []byte("old"), 0600))
	synced, err := OpenSyncedFile(filepath.Join(dir, "other"), false)
	require.NoError(t, err)
	require.NoError(t, synced.Close())

	truncated, err := OpenTruncFile(filepath.Join(dir, "truncated"), false)
	require.NoError(t, err)
	require.NoError(t, truncated.Close())

	contents, err := os.ReadFile(filepath.Join(dir, "other"))
	require.NoError(t, err)
	require.Equal(t, []byte("old"), contents)

	_, err = OpenExistingFile(filepath.Join(dir, "missing"), 0)
	require.Error(t, err)
}

func TestIBytesToString(t *testing.T) {
	require.Equal(t, "9 B", IBytesToString(9, 1))
	require.Equal(t, "1.0 KiB", IBytesToString(1024, 1))
	require.Equal(t, "1.5 KiB", IBytesToString(1536, 1))
	require.Equal(t, "1.00 MiB", IBytesToString(1024*1024, 2))
}

func TestNewKVWithoutAllocator(t *testing.T) {
	kv := NewKV(nil)
	require.NotNil(t, kv)

	kv.Key = []byte("key")
	require.Equal(t, []byte("key"), kv.Key)
}

func TestRateMonitor(t *testing.T) {
	monitor := NewRateMonitor(2)
	require.Zero(t, monitor.Rate())

	monitor.lastCapture = time.Now().Add(-time.Second)
	monitor.Capture(100)
	require.InDelta(t, float64(100), float64(monitor.Rate()), 5)

	monitor.lastCapture = time.Now().Add(-time.Second)
	monitor.lastSent = 100
	monitor.Capture(300)
	require.InDelta(t, float64(150), float64(monitor.Rate()), 10)
}

func TestWrapAndChecksumError(t *testing.T) {
	require.Nil(t, Wrap(nil, "message"))

	previousDebugMode := debugMode
	debugMode = true
	t.Cleanup(func() { debugMode = previousDebugMode })

	err := errors.New("base")
	wrapped := Wrap(err, "message")
	require.Error(t, wrapped)
	require.True(t, strings.Contains(wrapped.Error(), "message"))
	require.True(t, strings.Contains(wrapped.Error(), "base"))

	formatted := Wrapf(err, "value=%d", 42)
	require.Error(t, formatted)
	require.Contains(t, formatted.Error(), "value=42")

	mismatch := VerifyChecksum([]byte("data"), &pb.Checksum{
		Algo: pb.Checksum_CRC32C,
		Sum:  1,
	})
	require.ErrorIs(t, mismatch, ErrChecksumMismatch)
}

func TestValueStructEncodeDecode(t *testing.T) {
	values := []struct {
		meta     byte
		userMeta byte
		expiry   uint64
		value    []byte
	}{
		{0x01, 0x02, 0, []byte("")},
		{0x03, 0x04, 127, []byte("small")},
		{0x05, 0x06, math.MaxUint64, []byte("large-expiry")},
	}

	for _, input := range values {
		structValue := ValueStruct{
			Meta:      input.meta,
			UserMeta:  input.userMeta,
			ExpiresAt: input.expiry,
			Value:     input.value,
		}

		encoded := make([]byte, structValue.EncodedSize())
		n := structValue.Encode(encoded)
		require.Equal(t, uint32(len(encoded)), n)

		var decoded ValueStruct
		decoded.Decode(encoded)
		require.Equal(t, structValue, decoded)

		var buffered bytes.Buffer
		structValue.EncodeTo(&buffered)
		require.Equal(t, encoded, buffered.Bytes())
	}
}

func TestEncryptionHelpers(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	iv, err := GenerateIV()
	require.NoError(t, err)
	require.Len(t, iv, aes.BlockSize)

	plaintext := []byte("secret value spanning more than one AES block")

	encrypted, err := XORBlockAllocate(plaintext, key, iv)
	require.NoError(t, err)
	require.NotEqual(t, plaintext, encrypted)

	decrypted, err := XORBlockAllocate(encrypted, key, iv)
	require.NoError(t, err)
	require.Equal(t, plaintext, decrypted)

	var streamOutput bytes.Buffer
	require.NoError(t, XORBlockStream(&streamOutput, plaintext, key, iv))
	require.Equal(t, encrypted, streamOutput.Bytes())

	_, err = XORBlockAllocate(plaintext, []byte("short"), iv)
	require.Error(t, err)

	var destination bytes.Buffer
	require.Error(t, XORBlockStream(&destination, plaintext, []byte("short"), iv))
}

func TestZSTDRoundTripAndBound(t *testing.T) {
	plaintext := bytes.Repeat([]byte("badger-y-coverage-"), 128)

	compressed, err := ZSTDCompress(nil, plaintext, 3)
	require.NoError(t, err)
	require.NotEmpty(t, compressed)

	decoder, err := zstd.NewReader(nil)
	require.NoError(t, err)
	_, err = decoder.DecodeAll(compressed, nil)
	require.NoError(t, err)

	preallocated := make([]byte, 0, len(plaintext))
	decompressed, err := ZSTDDecompress(preallocated, compressed)
	require.NoError(t, err)
	require.Equal(t, plaintext, decompressed)

	_, err = ZSTDDecompress(nil, []byte("not zstd"))
	require.Error(t, err)

	require.Equal(t, 64, ZSTDCompressBound(0))
	require.GreaterOrEqual(t, ZSTDCompressBound(100), 100)
	require.GreaterOrEqual(t, ZSTDCompressBound(256<<10), 256<<10)
}

func TestBloomFilterEdgesAndSize(t *testing.T) {
	require.Equal(t, 7, BloomBitsPerKey(1000, 0.01))
	require.Equal(t, 10, BloomBitsPerKey(1000, 0.001))

	empty := NewFilter(nil, 10)
	require.Len(t, empty, 9)
	require.False(t, Filter{0x00}.MayContain(0))

	noHashBits := Filter([]byte{0x00, 31})
	require.True(t, noHashBits.MayContain(1))

	reused := appendFilter(make([]byte, 8, 16), nil, 0)
	require.Equal(t, uint8(1), reused[len(reused)-1])

	hashes := []uint32{Hash([]byte("alpha")), Hash([]byte("beta"))}
	filter := NewFilter(hashes, 100)
	for _, hash := range hashes {
		require.True(t, filter.MayContain(hash))
	}
}
