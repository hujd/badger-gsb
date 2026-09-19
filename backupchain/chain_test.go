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
