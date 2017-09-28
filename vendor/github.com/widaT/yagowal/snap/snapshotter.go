package snap

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	pioutil "github.com/widaT/yagowal/ioutil"
	"github.com/widaT/yagowal/serialize"
	"github.com/widaT/yagowal/structure"
)

const (
	snapSuffix = ".snap"
)

var (

	ErrNoSnapshot    = errors.New("snap: no available snapshot")
	ErrEmptySnapshot = errors.New("snap: empty snapshot")
	ErrCRCMismatch   = errors.New("snap: crc mismatch")
	crcTable         = crc32.MakeTable(crc32.Castagnoli)

	// A map of valid files that can be present in the snap folder.
	validFiles = map[string]bool{
		"db": true,
	}
)

type Snapshotter struct {
	dir string
}

func New(dir string) *Snapshotter {
	return &Snapshotter{
		dir: dir,
	}
}

func (s *Snapshotter) save(snapshot *structure.SnapshotRecord) error {
	fname := fmt.Sprintf("%016x%s",snapshot.Index, snapSuffix)
	b := serialize.MustMarshal(snapshot)
	crc := crc32.Update(0, crcTable, b)
	snap := structure.SnapshotFile{Crc: crc, Data: b}
	d, err := snap.Marshal()
	if err != nil {
		return err
	}

	err = pioutil.WriteAndSyncFile(filepath.Join(s.dir, fname), d, 0666)
	if err != nil {
		err1 := os.Remove(filepath.Join(s.dir, fname))
		if err1 != nil {
			return err1
		}
	}
	return err
}

func (s *Snapshotter) SaveSnap(snapshot structure.SnapshotRecord) error {
	if snapshot.Index == 0 {
		return nil
	}
	return s.save(&snapshot)
}

func (s *Snapshotter) Load() (*structure.SnapshotRecord, error) {
	names, err := s.snapNames()
	if err != nil {
		return nil, err
	}
	var snap *structure.SnapshotRecord
	for _, name := range names {
		if snap, err = loadSnap(s.dir, name); err == nil {
			break
		}
	}
	if err != nil {
		return nil, ErrNoSnapshot
	}
	return snap, nil
}

func loadSnap(dir, name string) (*structure.SnapshotRecord, error) {
	fpath := filepath.Join(dir, name)
	snap, err := Read(fpath)
	if err != nil {
		renameBroken(fpath)
	}
	return snap, err
}

// Read reads the snapshot named by snapname and returns the snapshot.
func Read(snapname string) (*structure.SnapshotRecord, error) {
	b, err := ioutil.ReadFile(snapname)
	if err != nil {
		return nil, err
	}

	if len(b) == 0 {
		return nil, ErrEmptySnapshot
	}

	var serializedSnap structure.SnapshotFile
	if err = serializedSnap.Unmarshal(b); err != nil {
		return nil, err
	}

	if len(serializedSnap.Data) == 0 || serializedSnap.Crc == 0 {
		return nil, ErrEmptySnapshot
	}

	crc := crc32.Update(0, crcTable, serializedSnap.Data)
	if crc != serializedSnap.Crc {
		return nil, ErrCRCMismatch
	}

	var snap structure.SnapshotRecord
	if err = snap.Unmarshal(serializedSnap.Data); err != nil {
		return nil, err
	}
	return &snap, nil
}

// snapNames returns the filename of the snapshots in logical time order (from newest to oldest).
// If there is no available snapshots, an ErrNoSnapshot will be returned.
func (s *Snapshotter) snapNames() ([]string, error) {
	dir, err := os.Open(s.dir)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	snaps := checkSuffix(names)
	if len(snaps) == 0 {
		return nil, ErrNoSnapshot
	}
	sort.Sort(sort.Reverse(sort.StringSlice(snaps)))
	return snaps, nil
}

func checkSuffix(names []string) []string {
	snaps := []string{}
	for i := range names {
		if strings.HasSuffix(names[i], snapSuffix) {
			snaps = append(snaps, names[i])
		} else {
			// If we find a file which is not a snapshot then check if it's
			// a vaild file. If not throw out a warning.
			if _, ok := validFiles[names[i]]; !ok {
				//plog.Warningf("skipped unexpected non snapshot file %v", names[i])
			}
		}
	}
	return snaps
}

func renameBroken(path string) {
	brokenPath := path + ".broken"
	if err := os.Rename(path, brokenPath); err != nil {
		//plog.Warningf("cannot rename broken snapshot file %v to %v: %v", path, brokenPath, err)
	}
}
