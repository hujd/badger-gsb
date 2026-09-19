package backupchain_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/backupchain"
	"github.com/stretchr/testify/require"
)

func newDB(t *testing.T, dir string) *badger.DB {
	t.Helper()
	opt := badger.DefaultOptions(dir).WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(opt)
	require.NoError(t, err)
	return db
}

func set(t *testing.T, db *badger.DB, k, v string) {
	t.Helper()
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(k), []byte(v))
	}))
}

func get(t *testing.T, db *badger.DB, k string) (string, bool) {
	t.Helper()
	var out []byte
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(k))
		if err != nil {
			return err
		}
		out, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return "", false
	}
	return string(out), true
}

func TestFullBackupRestoresEveryKey(t *testing.T) {
	dir := t.TempDir()
	src := newDB(t, filepath.Join(dir, "src"))
	defer src.Close()
	for _, k := range []string{"a", "b", "c"} {
		set(t, src, k, "value-"+k)
	}

	mg, err := backupchain.Open(filepath.Join(dir, "chain"), src)
	require.NoError(t, err)
	seg, err := mg.Backup()
	require.NoError(t, err)
	require.Equal(t, "full", seg.Kind)

	dst := newDB(t, filepath.Join(dir, "dst"))
	defer dst.Close()
	require.NoError(t, backupchain.Restore(filepath.Join(dir, "chain"), dst))

	for _, k := range []string{"a", "b", "c"} {
		v, ok := get(t, dst, k)
		require.True(t, ok, "key %s missing after restore", k)
		require.Equal(t, "value-"+k, v)
	}
}

func TestIncrementalSegmentsCarryLaterWrites(t *testing.T) {
	dir := t.TempDir()
	src := newDB(t, filepath.Join(dir, "src"))
	defer src.Close()
	chain := filepath.Join(dir, "chain")
	mg, err := backupchain.Open(chain, src)
	require.NoError(t, err)

	set(t, src, "before-1", "one")
	set(t, src, "before-2", "two")
	first, err := mg.Backup()
	require.NoError(t, err)
	require.Equal(t, "full", first.Kind)

	set(t, src, "after-1", "three")
	set(t, src, "after-2", "four")
	second, err := mg.Backup()
	require.NoError(t, err)
	require.Equal(t, "incremental", second.Kind)
	require.Equal(t, first.UntilTs, second.SinceTs)

	dst := newDB(t, filepath.Join(dir, "dst"))
	defer dst.Close()
	require.NoError(t, backupchain.Restore(chain, dst))
	for k, want := range map[string]string{
		"before-1": "one", "before-2": "two", "after-1": "three", "after-2": "four",
	} {
		v, ok := get(t, dst, k)
		require.True(t, ok, "key %s missing after restore", k)
		require.Equal(t, want, v)
	}
}

func TestPruneDropsOldestSegments(t *testing.T) {
	dir := t.TempDir()
	src := newDB(t, filepath.Join(dir, "src"))
	defer src.Close()
	chain := filepath.Join(dir, "chain")
	mg, err := backupchain.Open(chain, src)
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		set(t, src, fmt.Sprintf("k-%d", i), "v")
		_, err := mg.Backup()
		require.NoError(t, err)
	}
	dropped, err := mg.Prune(2)
	require.NoError(t, err)
	require.Greater(t, dropped, 0)

	// The oldest segment is gone from disk and no longer referenced.
	_, err = os.Stat(filepath.Join(chain, "segment-0000.bin"))
	require.True(t, os.IsNotExist(err))
	for _, seg := range mg.Manifest().Segments {
		_, err := os.Stat(filepath.Join(chain, seg.File))
		require.NoError(t, err)
	}
	require.NoError(t, backupchain.Verify(chain))
}

func TestVerifyDetectsTamperedSegment(t *testing.T) {
	dir := t.TempDir()
	src := newDB(t, filepath.Join(dir, "src"))
	defer src.Close()
	chain := filepath.Join(dir, "chain")
	mg, err := backupchain.Open(chain, src)
	require.NoError(t, err)
	set(t, src, "a", "b")
	seg, err := mg.Backup()
	require.NoError(t, err)

	require.NoError(t, backupchain.Verify(chain))
	require.NoError(t, os.WriteFile(filepath.Join(chain, seg.File), []byte("broken"), 0o644))
	require.Error(t, backupchain.Verify(chain))
}

func del(t *testing.T, db *badger.DB, k string) {
	t.Helper()
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(k))
	}))
}

// TestRestoreKeepsNewestVersionWithinSegment compacts several versions of the
// same key inside a single segment: the newest value must win, and a newest
// entry that is a delete marker must stay deleted.
func TestRestoreKeepsNewestVersionWithinSegment(t *testing.T) {
	dir := t.TempDir()
	src := newDB(t, filepath.Join(dir, "src"))
	defer src.Close()

	set(t, src, "updated", "v1")
	set(t, src, "updated", "v2")
	set(t, src, "updated", "v3")
	set(t, src, "deleted", "gone")
	del(t, src, "deleted")

	chain := filepath.Join(dir, "chain")
	mg, err := backupchain.Open(chain, src)
	require.NoError(t, err)
	_, err = mg.Backup()
	require.NoError(t, err)

	dst := newDB(t, filepath.Join(dir, "dst"))
	defer dst.Close()
	require.NoError(t, backupchain.Restore(chain, dst))

	v, ok := get(t, dst, "updated")
	require.True(t, ok)
	require.Equal(t, "v3", v)
	_, ok = get(t, dst, "deleted")
	require.False(t, ok, "deleted key resurrected by restore")
}

// TestRestoreAcrossChainKeepsNewestVersion repeats the check across several
// incremental segments.
func TestRestoreAcrossChainKeepsNewestVersion(t *testing.T) {
	dir := t.TempDir()
	src := newDB(t, filepath.Join(dir, "src"))
	defer src.Close()
	chain := filepath.Join(dir, "chain")
	mg, err := backupchain.Open(chain, src)
	require.NoError(t, err)

	set(t, src, "updated", "v1")
	set(t, src, "deleted", "d1")
	set(t, src, "stable", "s1")
	_, err = mg.Backup()
	require.NoError(t, err)

	set(t, src, "updated", "v2")
	del(t, src, "deleted")
	_, err = mg.Backup()
	require.NoError(t, err)

	set(t, src, "updated", "v3")
	set(t, src, "updated", "v4")
	set(t, src, "updated", "v5")
	_, err = mg.Backup()
	require.NoError(t, err)

	dst := newDB(t, filepath.Join(dir, "dst"))
	defer dst.Close()
	require.NoError(t, backupchain.Restore(chain, dst))

	v, ok := get(t, dst, "updated")
	require.True(t, ok)
	require.Equal(t, "v5", v)
	_, ok = get(t, dst, "deleted")
	require.False(t, ok, "deleted key resurrected by restore")
	v, ok = get(t, dst, "stable")
	require.True(t, ok)
	require.Equal(t, "s1", v)
}

// TestPruneThenRestore checks that pruning older segments does not strand the
// remaining incrementals: the first kept segment becomes a full snapshot and
// keys that only existed in pruned segments still restore, including the
// latest value and deletions.
func TestPruneThenRestore(t *testing.T) {
	dir := t.TempDir()
	src := newDB(t, filepath.Join(dir, "src"))
	defer src.Close()
	chain := filepath.Join(dir, "chain")
	mg, err := backupchain.Open(chain, src)
	require.NoError(t, err)

	// Segment 0: full backup with the first batch of data.
	for i := 0; i < 20; i++ {
		set(t, src, fmt.Sprintf("old-%02d", i), fmt.Sprintf("v0-%d", i))
	}
	set(t, src, "will-delete", "alive")
	_, err = mg.Backup()
	require.NoError(t, err)

	// Segment 1: update part of the old batch, delete one key, add new keys.
	for i := 0; i < 10; i++ {
		set(t, src, fmt.Sprintf("old-%02d", i), fmt.Sprintf("v1-%d", i))
	}
	del(t, src, "will-delete")
	_, err = mg.Backup()
	require.NoError(t, err)

	// Segment 2.
	for i := 0; i < 10; i++ {
		set(t, src, fmt.Sprintf("new-%02d", i), "n")
	}
	_, err = mg.Backup()
	require.NoError(t, err)

	// Segment 3: one more update.
	set(t, src, "old-00", "v3")
	_, err = mg.Backup()
	require.NoError(t, err)

	dropped, err := mg.Prune(2)
	require.NoError(t, err)
	require.Equal(t, 2, dropped)

	segs := mg.Manifest().Segments
	require.Len(t, segs, 2)
	require.Equal(t, "full", segs[0].Kind, "oldest kept segment must be promoted to full")
	require.Equal(t, "incremental", segs[1].Kind)
	require.NoError(t, backupchain.Verify(chain))

	// Dropped files are gone, kept files are intact.
	_, err = os.Stat(filepath.Join(chain, "segment-0000.bin"))
	require.True(t, os.IsNotExist(err))

	dst := newDB(t, filepath.Join(dir, "dst"))
	defer dst.Close()
	require.NoError(t, backupchain.Restore(chain, dst))

	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("old-%02d", i)
		v, ok := get(t, dst, k)
		require.True(t, ok, "key %s lost after pruning old segments", k)
		want := fmt.Sprintf("v0-%d", i)
		if i < 10 {
			want = fmt.Sprintf("v1-%d", i)
		}
		if k == "old-00" {
			want = "v3"
		}
		require.Equal(t, want, v)
	}
	for i := 0; i < 10; i++ {
		_, ok := get(t, dst, fmt.Sprintf("new-%02d", i))
		require.True(t, ok)
	}
	_, ok := get(t, dst, "will-delete")
	require.False(t, ok, "deleted key resurrected after prune+restore")
}
