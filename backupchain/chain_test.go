package backupchain_test

import (
	"encoding/json"
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

func del(t *testing.T, db *badger.DB, k string) {
	t.Helper()
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(k))
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

// dump returns every live key/value pair of db.
func dump(t *testing.T, db *badger.DB) map[string]string {
	t.Helper()
	out := map[string]string{}
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			v, err := item.ValueCopy(nil)
			require.NoError(t, err)
			out[string(item.KeyCopy(nil))] = string(v)
		}
		return nil
	}))
	return out
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

func TestRestoreReturnsLatestVersionOfEachKey(t *testing.T) {
	dir := t.TempDir()
	src := newDB(t, filepath.Join(dir, "src"))
	defer src.Close()
	chain := filepath.Join(dir, "chain")
	mg, err := backupchain.Open(chain, src)
	require.NoError(t, err)

	set(t, src, "churned", "v1")
	_, err = mg.Backup()
	require.NoError(t, err)

	// Several rewrites of the same key inside one incremental window: the
	// newest one must be the one that survives the backup.
	set(t, src, "churned", "v2")
	set(t, src, "churned", "v3")
	set(t, src, "churned", "v4")
	_, err = mg.Backup()
	require.NoError(t, err)

	dst := newDB(t, filepath.Join(dir, "dst"))
	defer dst.Close()
	require.NoError(t, backupchain.Restore(chain, dst))
	v, ok := get(t, dst, "churned")
	require.True(t, ok)
	require.Equal(t, "v4", v)
}

func TestRestoreHonorsDeletes(t *testing.T) {
	dir := t.TempDir()
	src := newDB(t, filepath.Join(dir, "src"))
	defer src.Close()
	chain := filepath.Join(dir, "chain")
	mg, err := backupchain.Open(chain, src)
	require.NoError(t, err)

	set(t, src, "deleted-after-full", "x")
	set(t, src, "kept", "y")
	_, err = mg.Backup()
	require.NoError(t, err)

	del(t, src, "deleted-after-full")
	// Written and deleted again inside the same incremental window.
	set(t, src, "deleted-within-segment", "z")
	del(t, src, "deleted-within-segment")
	_, err = mg.Backup()
	require.NoError(t, err)

	dst := newDB(t, filepath.Join(dir, "dst"))
	defer dst.Close()
	require.NoError(t, backupchain.Restore(chain, dst))

	_, ok := get(t, dst, "deleted-after-full")
	require.False(t, ok, "deleted key resurrected by restore")
	_, ok = get(t, dst, "deleted-within-segment")
	require.False(t, ok, "key deleted within one segment resurrected by restore")
	v, ok := get(t, dst, "kept")
	require.True(t, ok)
	require.Equal(t, "y", v)
}

func TestPruneKeepsChainRestorable(t *testing.T) {
	dir := t.TempDir()
	src := newDB(t, filepath.Join(dir, "src"))
	defer src.Close()
	chain := filepath.Join(dir, "chain")
	mg, err := backupchain.Open(chain, src)
	require.NoError(t, err)

	// Four segments: the oldest data lives only in the full base segment.
	for i := 0; i < 4; i++ {
		set(t, src, fmt.Sprintf("batch-%d", i), fmt.Sprintf("value-%d", i))
		_, err := mg.Backup()
		require.NoError(t, err)
	}
	set(t, src, "batch-1", "rewritten")
	del(t, src, "batch-2")
	_, err = mg.Backup()
	require.NoError(t, err)

	dropped, err := mg.Prune(2)
	require.NoError(t, err)
	require.Equal(t, 3, dropped)

	manifest := mg.Manifest()
	require.Len(t, manifest.Segments, 2)
	require.Equal(t, "full", manifest.Segments[0].Kind,
		"pruned chain must start with a full base segment")
	require.NoError(t, backupchain.Verify(chain))

	// Restoring the pruned chain must yield the same state as the source.
	dst := newDB(t, filepath.Join(dir, "dst"))
	defer dst.Close()
	require.NoError(t, backupchain.Restore(chain, dst))
	require.Equal(t, dump(t, src), dump(t, dst))
	_, ok := get(t, dst, "batch-0")
	require.True(t, ok, "oldest batch lost after prune")

	// The chain must keep accepting backups after a prune, without
	// clobbering the retained segment files.
	set(t, src, "post-prune", "p")
	seg, err := mg.Backup()
	require.NoError(t, err)
	require.Equal(t, manifest.Segments[1].Index+1, seg.Index)
	require.NoError(t, backupchain.Verify(chain))

	dst2 := newDB(t, filepath.Join(dir, "dst2"))
	defer dst2.Close()
	require.NoError(t, backupchain.Restore(chain, dst2))
	require.Equal(t, dump(t, src), dump(t, dst2))
}

func TestRestoreRejectsChainWithoutFullBase(t *testing.T) {
	dir := t.TempDir()
	src := newDB(t, filepath.Join(dir, "src"))
	defer src.Close()
	chain := filepath.Join(dir, "chain")
	mg, err := backupchain.Open(chain, src)
	require.NoError(t, err)

	set(t, src, "a", "1")
	first, err := mg.Backup()
	require.NoError(t, err)
	set(t, src, "b", "2")
	_, err = mg.Backup()
	require.NoError(t, err)

	// Simulate a chain whose base segment was removed externally.
	require.NoError(t, os.Remove(filepath.Join(chain, first.File)))
	manifest := mg.Manifest()
	manifest.Segments = manifest.Segments[1:]
	buf, err := json.Marshal(manifest)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(chain, "manifest.json"), buf, 0o644))

	dst := newDB(t, filepath.Join(dir, "dst"))
	defer dst.Close()
	require.Error(t, backupchain.Restore(chain, dst),
		"restoring a chain without its full base must fail loudly")
}
