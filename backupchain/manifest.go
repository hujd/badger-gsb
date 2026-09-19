package backupchain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Segment describes one segment of a backup chain.
type Segment struct {
	Index    int       `json:"index"`
	Kind     string    `json:"kind"` // full | incremental
	File     string    `json:"file"`
	SinceTs  uint64    `json:"since_ts"`
	UntilTs  uint64    `json:"until_ts"`
	Size     int64     `json:"size"`
	Checksum string    `json:"checksum"`
	Created  time.Time `json:"created"`
}

// Manifest is the on-disk description of a backup chain.
type Manifest struct {
	Version  int       `json:"version"`
	Segments []Segment `json:"segments"`
}

const (
	manifestName = "manifest.json"
	manifestVer  = 1
)

func manifestPath(dir string) string { return filepath.Join(dir, manifestName) }

func loadManifest(dir string) (*Manifest, error) {
	f, err := os.Open(manifestPath(dir))
	if os.IsNotExist(err) {
		return &Manifest{Version: manifestVer}, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var m Manifest
	if err := json.NewDecoder(f).Decode(&m); err != nil {
		return nil, fmt.Errorf("backupchain: cannot read manifest: %w", err)
	}
	return &m, nil
}

// save writes the manifest through a temporary file so that a crash in the
// middle of the write cannot leave a half-written manifest behind.
func (m *Manifest) save(dir string) error {
	buf, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := manifestPath(dir) + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, manifestPath(dir))
}

// checksum returns the SHA-256 of a segment file.
func checksum(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
