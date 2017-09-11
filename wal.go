package newredis

import (
	"log"
	"bytes"
	"encoding/gob"
	"sync"
	"github.com/widaT/yagowal/wal"
	"github.com/widaT/yagowal/structure"
	"github.com/widaT/yagowal/snap"
	//"fmt"
	"encoding/binary"
)

func IntToBytes(n int) []byte {
	tmp := int32(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, &tmp)
	return bytesBuffer.Bytes()
}

func BytesToInt(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)
	var tmp int32
	binary.Read(bytesBuffer, binary.BigEndian, &tmp)
	return int(tmp)
}

type  unstable struct {
	ents []structure.Entry
	snap chan *structure.SnapshotRecord
	sync.RWMutex
}

func  (u *unstable)appendents(ents []structure.Entry)  {
	u.Lock()
	defer  u.Unlock()
	u.ents = append(u.ents,ents...)
}

func (u *unstable) truncate() (ents []structure.Entry) {
	u.Lock()
	defer  u.Unlock()
	ents = u.ents
	u.ents = nil
	return
}

type Wal struct {
	wal *wal.WAL
	snapshotter *snap.Snapshotter
	waldir string
	snapdir string
	nowIndex uint64
	snapshotIndex uint64
	snapcount uint64
	s *Server
	//mu sync.RWMutex
}


func (n *Wal) saveSnap(snap structure.SnapshotRecord) error {
	if err := n.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	walSnap := structure.Snapshot{
		Index: snap.Index,
	}
	if err := n.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	return nil
}



func (w *Wal) replayWAL() {
	snapshot := w.loadSnapshot()

	var ents  []structure.Entry
	var err error
	if snapshot != nil {
		ents, err = w.wal.ReadAll(&structure.Snapshot{Index:snapshot.Index})
	}else {
		ents, err = w.wal.ReadAll(nil)
	}

	if err != nil {
		log.Fatalf("raft-redis: failed to read WAL (%v)", err)
	}

	if snapshot != nil {
		err = w.s.db.recoverFromSnapshot(snapshot.Data)
		if err != nil {
			log.Fatalf("recoverFromSnapshot failed to read WAL (%v)", err)
		}
		w.s.w.snapshotIndex = snapshot.Index
		w.s.w.nowIndex = snapshot.Index
	}
	//fmt.Println(ents)
	w.s.db.recovebool = true
	if len(ents) > 0 {
		for _, ent := range ents {
			//fmt.Println(ent)
			var dataKv Opt

			dec := gob.NewDecoder(bytes.NewBuffer(ent.Data))
			if err := dec.Decode(&dataKv); err != nil {
				log.Fatalf("raftexample: could not decode message (%v)", err)
				continue
			}
			//fmt.Println(dataKv)

			switch  dataKv.Method {
			case "rpush":
				w.s.db.Rpush(dataKv.Key, dataKv.Args[0], dataKv.Args[1:]...)
			case "lpush":
				w.s.db.Lpush(dataKv.Key, dataKv.Args[0], dataKv.Args[1:]...)
			case "lpop":
				w.s.db.Lpop(dataKv.Key)
			case "rpop":
				w.s.db.Rpop(dataKv.Key)
			case "set":
				w.s.db.Set(dataKv.Key, dataKv.Args[0])
			case "hset":
				w.s.db.Hset(dataKv.Key, string(dataKv.Args[0]), dataKv.Args[1])
			case "sadd":
				var strs []string
				for _, b := range dataKv.Args {
					strs = append(strs, string(b))
				}
				w.s.db.Sadd(dataKv.Key, strs...)
			case "del":
				var strs []string
				for _, b := range dataKv.Args {
					strs = append(strs, string(b))
				}
				w.s.db.Del(strs[0], strs[1:]...)
			case "zadd":
				key := dataKv.Args[0]
				score := BytesToInt(dataKv.Args[1])
				w.s.db.Zadd(dataKv.Key,score,string(key))
			case "incr":
				w.s.db.Incr(dataKv.Key)
			case "mset":
				var strs []string
				for _, b := range dataKv.Args {
					strs = append(strs, string(b))
				}
				w.s.db.Mset(strs...)
			case "spop":
				w.s.db.spop(dataKv.Key, dataKv.Args[0])
			}
		}
		w.s.w.nowIndex = ents[len(ents)-1].Index
	}
	w.s.db.recovebool = false
}

func (n *Wal)loadSnapshot() *structure.SnapshotRecord {
	snapshot, err := n.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("raft-redis: error loading snapshot (%v)", err)
	}
	return snapshot
}

func (n *Wal)save(opt *Opt)  error {
/*		_Server.w.mu.Lock()
		defer  _Server.w.mu.Lock()*/
/*	var b bytes.Buffer
	ens := gob.NewEncoder(&b)
	err := ens.Encode(*opt)
	if err != nil {
		return err
	}
	es := structure.Entry{Index: _Server.w.nowIndex +1,Data:b.Bytes()}
	n.wal.SaveEntry(&es)
	if _Server.w.nowIndex - _Server.w.snapshotIndex  >= _Server.w.snapcount {
		data ,err :=w.s.db.getSnapshot()
		if err != nil {
			return err
		}
		n.saveSnap(structure.SnapshotRecord{Data:data,Index:_Server.w.nowIndex})
		_Server.w.snapshotIndex = _Server.w.nowIndex
	}
	_Server.w.nowIndex ++
	*/return nil
}

func NewWal( s *Server) *Wal  {

	return &Wal{s:s}
}