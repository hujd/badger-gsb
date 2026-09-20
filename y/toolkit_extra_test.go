package y

import (
	"bytes"
	"crypto/aes"
	"expvar"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/stretchr/testify/require"
)

func TestFileOpeners(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing")

	_, err := OpenExistingFile(missing, 0)
	require.Error(t, err)

	synced := filepath.Join(dir, "synced")
	file, err := CreateSyncedFile(synced, true)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	_, err = CreateSyncedFile(synced, true)
	require.Error(t, err)

	file, err = OpenSyncedFile(filepath.Join(dir, "opened"), true)
	require.NoError(t, err)
	_, err = file.Write([]byte("payload"))
	require.NoError(t, err)
	require.NoError(t, file.Close())

	file, err = OpenExistingFile(filepath.Join(dir, "opened"), ReadOnly)
	require.NoError(t, err)
	_, err = file.Write([]byte("x"))
	require.Error(t, err)
	require.NoError(t, file.Close())

	truncated := filepath.Join(dir, "truncated")
	require.NoError(t, os.WriteFile(truncated, []byte("old data"), 0600))
	file, err = OpenTruncFile(truncated, false)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	data, err := os.ReadFile(truncated)
	require.NoError(t, err)
	require.Empty(t, data)
}

func TestValueStructEncodeDecode(t *testing.T) {
	original := ValueStruct{
		Meta:      7,
		UserMeta:  9,
		ExpiresAt: 1234567890,
		Value:     []byte("value payload"),
	}

	encoded := make([]byte, original.EncodedSize())
	n := original.Encode(encoded)
	require.Equal(t, original.EncodedSize(), n)

	var decoded ValueStruct
	decoded.Decode(encoded)
	require.Equal(t, original.Meta, decoded.Meta)
	require.Equal(t, original.UserMeta, decoded.UserMeta)
	require.Equal(t, original.ExpiresAt, decoded.ExpiresAt)
	require.Equal(t, original.Value, decoded.Value)

	var stream bytes.Buffer
	original.EncodeTo(&stream)
	require.Equal(t, encoded, stream.Bytes())
}

func TestXORBlockAllocateAndStream(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	iv := bytes.Repeat([]byte{0x17}, aes.BlockSize)
	src := []byte("streaming encryption payload")

	encrypted, err := XORBlockAllocate(src, key, iv)
	require.NoError(t, err)
	require.Len(t, encrypted, len(src))
	require.NotEqual(t, src, encrypted)

	decrypted, err := XORBlockAllocate(encrypted, key, iv)
	require.NoError(t, err)
	require.Equal(t, src, decrypted)

	var encryptedStream bytes.Buffer
	require.NoError(t, XORBlockStream(&encryptedStream, src, key, iv))
	require.Equal(t, encrypted, encryptedStream.Bytes())

	_, err = XORBlockAllocate(src, []byte("short"), iv)
	require.Error(t, err)
	require.Error(t, XORBlockStream(&bytes.Buffer{}, src, []byte("short"), iv))
}

func TestGenerateIV(t *testing.T) {
	first, err := GenerateIV()
	require.NoError(t, err)
	require.Len(t, first, aes.BlockSize)

	second, err := GenerateIV()
	require.NoError(t, err)
	require.NotEqual(t, first, second)
}

func TestBloomHelpers(t *testing.T) {
	require.Equal(t, 7, BloomBitsPerKey(1000, 0.01))

	empty := NewFilter(nil, -5)
	require.Len(t, empty, 9)
	require.Equal(t, uint8(1), empty[len(empty)-1])
	require.False(t, empty.MayContainKey([]byte("zero bits cannot contain a key")))

	keys := []uint32{Hash([]byte("one")), Hash([]byte("two"))}
	prefix := []byte("prefix")
	appended := appendFilter(prefix, keys, 10)
	require.Equal(t, prefix, appended[:len(prefix)])
	require.True(t, Filter(appended[len(prefix):]).MayContainKey([]byte("one")))

	reserved := Filter(make([]byte, 9))
	reserved[8] = 31
	require.True(t, reserved.MayContain(Hash([]byte("future encoding"))))
	require.False(t, Filter(nil).MayContain(Hash([]byte("missing"))))
}

func TestExtendReusesCapacityAndClearsTrailer(t *testing.T) {
	buf := make([]byte, 2, 8)
	buf[0] = 1
	buf[1] = 2

	overall, trailer := extend(buf, 4)
	require.Len(t, overall, 6)
	require.Len(t, trailer, 4)
	require.Equal(t, &buf[0], &overall[0])
	require.Equal(t, []byte{0, 0, 0, 0}, trailer)

	overall, trailer = append(overall, 9, 9), nil
	overall, trailer = extend(overall, 20)
	require.Len(t, overall, 28)
	require.Len(t, trailer, 20)
	require.Equal(t, byte(1), overall[0])
	require.Equal(t, []byte{0, 0, 0, 0, 9, 9}, overall[2:8])
}

func TestZSTDRoundTripAndBound(t *testing.T) {
	data := bytes.Repeat([]byte("badger value log compression "), 128)

	compressed, err := ZSTDCompress(nil, data, 1)
	require.NoError(t, err)
	require.NotEmpty(t, compressed)
	require.Less(t, len(compressed), len(data))

	decompressed, err := ZSTDDecompress(nil, compressed)
	require.NoError(t, err)
	require.Equal(t, data, decompressed)

	_, err = ZSTDDecompress(nil, []byte("not zstd"))
	require.Error(t, err)

	smallBound := ZSTDCompressBound(1024)
	require.Greater(t, smallBound, 1024)
	largeBound := ZSTDCompressBound(256 << 10)
	require.Equal(t, 256<<10+(256<<10>>8), largeBound)
}

func TestMetricsOnlyMutateWhenEnabled(t *testing.T) {
	key := "y-toolkit-test-metrics"

	NumGetsAdd(false, 100)
	before := numGets.Value()
	NumGetsAdd(true, 3)
	require.Equal(t, before+3, numGets.Value())

	NumLSMGetsAdd(false, key, 100)
	NumLSMGetsAdd(true, key, 4)
	require.Equal(t, int64(4), expvar.Get(BADGER_METRIC_PREFIX+"get_num_lsm").(*expvar.Map).Get(key).(*expvar.Int).Value())

	NumBytesCompactionWrittenAdd(false, key, 100)
	NumBytesCompactionWrittenAdd(true, key, 7)

	size := new(expvar.Int)
	size.Set(123)
	LSMSizeSet(false, key, size)
	require.Nil(t, LSMSizeGet(false, key))
	LSMSizeSet(true, key, size)
	require.Same(t, size, LSMSizeGet(true, key))

	LSMSizeSet(true, key, nil)
	require.Nil(t, LSMSizeGet(true, key))
	VlogSizeSet(true, key, size)
	require.Same(t, size, VlogSizeGet(true, key))
	PendingWritesSet(true, key, size)

	NumIteratorsCreatedAdd(true, 1)
	NumGetsWithResultsAdd(true, 1)
	NumReadsVlogAdd(true, 1)
	NumBytesWrittenUserAdd(true, 1)
	NumWritesVlogAdd(true, 1)
	NumBytesReadsVlogAdd(true, 1)
	NumBytesReadsLSMAdd(true, 1)
	NumBytesWrittenVlogAdd(true, 1)
	NumBytesWrittenToL0Add(true, 1)
	NumPutsAdd(true, 1)
	NumMemtableGetsAdd(true, 1)
	NumCompactionTablesAdd(true, 1)
	NumLSMBloomHitsAdd(true, key, 1)
}

func TestChecksumMismatchCarriesValues(t *testing.T) {
	data := []byte("protected payload")
	checksum := &pb.Checksum{Algo: pb.Checksum_XXHash64, Sum: CalculateChecksum(data, pb.Checksum_XXHash64) + 1}

	err := VerifyChecksum(data, checksum)
	require.Error(t, err)
	require.Contains(t, err.Error(), ErrChecksumMismatch.Error())
	require.Contains(t, err.Error(), "expected")
}
