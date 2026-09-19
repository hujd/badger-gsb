package backupchain

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/pb"
	"google.golang.org/protobuf/proto"
)

// Manager owns a backup chain stored as a set of segment files plus a manifest.
type Manager struct {
	dir string
	db  *badger.DB

	mu sync.Mutex
	m  *Manifest
}

// Open prepares dir as the storage location of a backup chain for db. An
// existing manifest is picked up so that further backups continue the chain.
func Open(dir string, db *badger.DB) (*Manager, error) {
	if db == nil {
		return nil, fmt.Errorf("backupchain: nil db")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	m, err := loadManifest(dir)
	if err != nil {
		return nil, err
	}
	return &Manager{dir: dir, db: db, m: m}, nil
}

// Manifest returns a copy of the current manifest.
func (mg *Manager) Manifest() Manifest {
	mg.mu.Lock()
	defer mg.mu.Unlock()
	out := Manifest{Version: mg.m.Version}
	out.Segments = append(out.Segments, mg.m.Segments...)
	return out
}

// Backup writes the next segment of the chain and returns it. The first segment
// of a chain is a full backup of the database, every later segment is
// incremental and only carries the entries written since the previous segment.
func (mg *Manager) Backup() (Segment, error) {
	mg.mu.Lock()
	defer mg.mu.Unlock()

	var since uint64
	kind := "full"
	if n := len(mg.m.Segments); n > 0 {
		kind = "incremental"
		since = mg.m.Segments[n-1].UntilTs
	}

	tmp, err := os.CreateTemp(mg.dir, "segment-*")
	if err != nil {
		return Segment{}, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	until, err := mg.db.Backup(tmp, since)
	if err != nil {
		tmp.Close()
		return Segment{}, err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return Segment{}, err
	}
	if err := tmp.Close(); err != nil {
		return Segment{}, err
	}

	// Compact the segment so that a chain of many segments stays small.
	if err := compactSegment(tmpName); err != nil {
		return Segment{}, err
	}

	final := filepath.Join(mg.dir, fmt.Sprintf("segment-%04d.bin", len(mg.m.Segments)))
	if err := os.Rename(tmpName, final); err != nil {
		return Segment{}, err
	}
	st, err := os.Stat(final)
	if err != nil {
		return Segment{}, err
	}
	sum, err := checksum(final)
	if err != nil {
		return Segment{}, err
	}

	seg := Segment{
		Index:    len(mg.m.Segments),
		Kind:     kind,
		File:     filepath.Base(final),
		SinceTs:  since,
		UntilTs:  until,
		Size:     st.Size(),
		Checksum: sum,
		Created:  time.Now(),
	}
	mg.m.Segments = append(mg.m.Segments, seg)
	if err := mg.m.save(mg.dir); err != nil {
		return Segment{}, err
	}
	return seg, nil
}

// Prune removes the oldest segments so that at most keep segments are left, and
// reports how many segments were dropped.
func (mg *Manager) Prune(keep int) (int, error) {
	mg.mu.Lock()
	defer mg.mu.Unlock()

	if keep < 1 {
		return 0, fmt.Errorf("backupchain: keep must be at least 1")
	}
	if len(mg.m.Segments) <= keep {
		return 0, nil
	}
	drop := len(mg.m.Segments) - keep
	dropped := mg.m.Segments[:drop]
	kept := mg.m.Segments[drop:]

	// The remaining segments may be incrementals that only contain keys
	// changed after the dropped segments. Fold the dropped segments into
	// the first remaining one, promoting it to a full snapshot covering
	// everything written up to its UntilTs. Otherwise keys that only ever
	// appeared in the dropped segments would be lost on restore.
	if err := mg.promoteToFull(dropped, &kept[0]); err != nil {
		return 0, err
	}

	for _, seg := range dropped {
		if err := os.Remove(filepath.Join(mg.dir, seg.File)); err != nil && !os.IsNotExist(err) {
			return 0, err
		}
	}
	mg.m.Segments = append([]Segment(nil), kept...)
	if err := mg.m.save(mg.dir); err != nil {
		return 0, err
	}
	return drop, nil
}

// promoteToFull merges the KVs of lead into firstKept, keeping only the
// newest version of every key, and rewrites firstKept.File in place as a full
// backup. firstKept's metadata is updated accordingly.
func (mg *Manager) promoteToFull(lead []Segment, firstKept *Segment) error {
	var kvs []*pb.KV
	for _, seg := range append(append([]Segment(nil), lead...), *firstKept) {
		entries, err := readSegment(filepath.Join(mg.dir, seg.File))
		if err != nil {
			return err
		}
		kvs = append(kvs, entries...)
	}

	merged := newestPerKey(kvs)

	final := filepath.Join(mg.dir, firstKept.File)
	tmp := final + ".promote"
	defer os.Remove(tmp)
	if err := writeSegmentAt(tmp, merged); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return err
	}

	firstKept.Kind = "full"
	if len(lead) > 0 {
		firstKept.SinceTs = lead[0].SinceTs
	}
	st, err := os.Stat(final)
	if err != nil {
		return err
	}
	sum, err := checksum(final)
	if err != nil {
		return err
	}
	firstKept.Size = st.Size()
	firstKept.Checksum = sum
	return nil
}

// compactSegment rewrites a badger backup stream keeping a single copy of every
// key. Without it a chain would keep every historical version of every key.
func compactSegment(path string) error {
	kvs, err := readSegment(path)
	if err != nil {
		return err
	}
	if len(kvs) == 0 {
		return nil
	}
	return writeSegment(path, newestPerKey(kvs))
}

// newestPerKey returns the highest-version entry for every key. Versions are
// what badger uses to order the history of a key: the highest one is its
// current value, and a highest entry with the delete bit set means the key is
// deleted. Keeping an older entry instead would resurrect stale values and
// deleted keys on restore. The result is ordered by key, then version, which
// is the order badger's loader expects within a stream.
func newestPerKey(kvs []*pb.KV) []*pb.KV {
	sort.SliceStable(kvs, func(i, j int) bool {
		if c := bytes.Compare(kvs[i].Key, kvs[j].Key); c != 0 {
			return c < 0
		}
		return kvs[i].Version < kvs[j].Version
	})
	kept := kvs[:0]
	for i, kv := range kvs {
		if i+1 < len(kvs) && bytes.Equal(kvs[i+1].Key, kv.Key) {
			continue
		}
		kept = append(kept, kv)
	}
	return kept
}

// readSegment reads a badger backup file: a sequence of length prefixed
// protobuf KVList messages.
func readSegment(path string) ([]*pb.KV, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 1<<20)
	var out []*pb.KV
	for {
		var size uint64
		if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if size == 0 {
			continue
		}
		buf := make([]byte, size)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		var list pb.KVList
		if err := proto.Unmarshal(buf, &list); err != nil {
			return nil, err
		}
		out = append(out, list.Kv...)
	}
	return out, nil
}

const segmentBatch = 1000

// writeSegment writes kvs back in the badger backup format.
func writeSegment(path string, kvs []*pb.KV) error {
	return writeSegmentAt(path, kvs)
}

// writeSegmentAt writes kvs in the badger backup format to path, creating or
// truncating the file.
func writeSegmentAt(path string, kvs []*pb.KV) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 1<<20)
	for start := 0; start < len(kvs); start += segmentBatch {
		end := start + segmentBatch
		if end > len(kvs) {
			end = len(kvs)
		}
		list := &pb.KVList{Kv: kvs[start:end]}
		buf, err := proto.Marshal(list)
		if err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, uint64(len(buf))); err != nil {
			return err
		}
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return f.Sync()
}
