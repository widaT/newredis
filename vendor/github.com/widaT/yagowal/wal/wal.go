package wal

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/widaT/yagowal/serialize"
	"github.com/widaT/yagowal/structure"
)

const (
	entryType uint64 = iota + 1
	snapshotType
	LOGKEYPREFIX = "LOG_%016x"
)

type WAL struct {
	dir string
	Db * leveldb.DB
	enti    uint64   // index of the last entry saved to the wal
}

func New(dir string) (w *WAL, err error) {
	w = &WAL{dir:dir}
	w.Db,err = leveldb.OpenFile(w.dir,nil)
	return
}

func (w *WAL) Close() {
	w.Db.Close()
}

func (w *WAL)save(Index uint64,log []byte)  error {
	go w.Db.Put([]byte(fmt.Sprintf(LOGKEYPREFIX,Index)), log, nil)
	return nil
}

func (w *WAL) Save(ents []structure.Entry) error {
	if  len(ents) == 0 {
		return nil
	}
	for i := range ents {
		if err := w.SaveEntry(&ents[i]); err != nil {
			return err
		}
	}
	return nil
}

func (w *WAL) SaveEntry(e *structure.Entry) error {
	b := serialize.MustMarshal(e)
	rec := &structure.Record{Type: entryType, Data: b}
	data,err:= rec.Marshal()
	if err != nil {
		return err
	}
	if err := w.save(e.Index,data); err != nil {
		return err
	}
	w.enti = e.Index
	return nil
}

func (w *WAL) SaveSnapshot(e structure.Snapshot) error {
	b := serialize.MustMarshal(&e)
	rec := &structure.Record{Type: snapshotType, Data: b}
	data,err:= rec.Marshal()
	if err != nil {
		return err
	}
	if err := w.save(e.Index,data); err != nil {
		return err
	}
	if w.enti < e.Index {
		w.enti = e.Index
	}
	return nil
}

func (w *WAL) ReadAll(snapshot *structure.Snapshot) (ents []structure.Entry, err error) {
	iter := w.Db.NewIterator(nil, nil)
	var snap uint64 = 0
	if snapshot != nil {
		iter.Seek([]byte(fmt.Sprintf(LOGKEYPREFIX,snapshot.Index)))
		snap = snapshot.Index
	}
	for iter.Next() {
		rec := structure.Record{}
		rec.Unmarshal(iter.Value())
		switch rec.Type {
		case entryType:
			e := serialize.MustUnmarshalEntry(rec.Data)
			if e.Index > snap{
				ents = append(ents[:e.Index-snap-1], e)
			}
			w.enti = e.Index
		}
	}
	err = iter.Error()
	return
}
