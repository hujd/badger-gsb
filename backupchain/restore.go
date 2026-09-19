package backupchain

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	badger "github.com/dgraph-io/badger/v4"
)

// Restore replays the chain kept in dir into dst, oldest segment first. dst is
// expected to be empty: every segment is applied on top of it in chain order.
func Restore(dir string, dst *badger.DB) error {
	if dst == nil {
		return fmt.Errorf("backupchain: nil db")
	}
	m, err := loadManifest(dir)
	if err != nil {
		return err
	}
	if len(m.Segments) == 0 {
		return fmt.Errorf("backupchain: no segments in %s", dir)
	}

	segs := append([]Segment(nil), m.Segments...)
	sort.Slice(segs, func(i, j int) bool { return segs[i].Index < segs[j].Index })

	// A chain that does not start with a full segment, or that has a gap in
	// its version coverage, cannot restore the full database state. Refuse to
	// replay it instead of silently producing a partial restore.
	if segs[0].Kind != "full" {
		return fmt.Errorf("backupchain: chain in %s does not start with a full segment", dir)
	}
	for i := 1; i < len(segs); i++ {
		if segs[i].SinceTs != segs[i-1].UntilTs {
			return fmt.Errorf("backupchain: chain in %s is broken between %s and %s",
				dir, segs[i-1].File, segs[i].File)
		}
	}

	for _, seg := range segs {
		f, err := os.Open(filepath.Join(dir, seg.File))
		if err != nil {
			return err
		}
		err = dst.Load(f, 16)
		f.Close()
		if err != nil {
			return fmt.Errorf("backupchain: replaying %s: %w", seg.File, err)
		}
	}
	return nil
}

// Verify checks that every segment of the chain in dir is present and still
// matches the checksum recorded in the manifest.
func Verify(dir string) error {
	m, err := loadManifest(dir)
	if err != nil {
		return err
	}
	for _, seg := range m.Segments {
		sum, err := checksum(filepath.Join(dir, seg.File))
		if err != nil {
			return err
		}
		if sum != seg.Checksum {
			return fmt.Errorf("backupchain: %s: checksum mismatch", seg.File)
		}
	}
	return nil
}
