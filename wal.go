package newredis

import (
	"log"
	"bytes"
	"sync"
	"github.com/widaT/yagowal/wal"
	"github.com/widaT/yagowal/structure"
	"github.com/widaT/yagowal/snap"
	"encoding/binary"
	"os"
	"github.com/vmihailenco/msgpack"
	//"time"
	"time"
)


var ents []structure.Entry



func FloatToBytes(n float64) []byte {
	tmp := float64(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, &tmp)
	return bytesBuffer.Bytes()
}

func BytesToFloat(b []byte) float64 {
	bytesBuffer := bytes.NewBuffer(b)
	var tmp float64
	binary.Read(bytesBuffer, binary.BigEndian, &tmp)
	return tmp
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
			if err := msgpack.Unmarshal(ent.Data,&dataKv); err != nil {
				log.Fatalf("raftexample: could not decode message (%v)", err)
				continue
			}
			//fmt.Println(dataKv)
			switch  dataKv.Method {
			case "rpush":
				w.s.db.Rpush( dataKv.Args...)
			case "lpush":
				w.s.db.Lpush(dataKv.Args...)
			case "lpop":
				w.s.db.Lpop(dataKv.Key)
			case "rpop":
				w.s.db.Rpop(dataKv.Key)
			case "set":
				w.s.db.Set(dataKv.Key, dataKv.Args[0])
			case "hset":
				w.s.db.Hset(dataKv.Key, string(dataKv.Args[0]), dataKv.Args[1])
			case "sadd":
				w.s.db.Sadd(dataKv.Key, dataKv.Args...)
			case "del":
				w.s.db.Del(dataKv.Args...)
			case "zadd":
				key := dataKv.Args[0]
				score := BytesToFloat(dataKv.Args[1])
				w.s.db.Zadd(dataKv.Key,score,string(key))
			case "incr":
				w.s.db.Incr(dataKv.Key)
			case "mset":
				w.s.db.Mset(dataKv.Args...)
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




func (wal *Wal)save(opt *Opt)  error {
	switch wal.s.conf.walsavetype {
	case "es"://every second
		server := wal.s
		b,err := msgpack.Marshal(opt)
		if err != nil {
			return err
		}
		wal.s.mu.Lock()
		ents = append(ents,structure.Entry{Index: server.w.nowIndex + 1, Data:b})
		wal.s.mu.Unlock()
		server.w.nowIndex ++
	case "aw"://all way
		server := wal.s
		b,err := msgpack.Marshal(opt)
		if err != nil {
			return err
		}
		es := structure.Entry{Index: server.w.nowIndex + 1, Data:b}
		go wal.wal.SaveEntry(&es)
		if server.w.nowIndex-wal.snapshotIndex >= server.w.snapcount {
			data, err := wal.s.db.getSnapshot()
			if err != nil {
				return err
			}
			wal.saveSnap(structure.SnapshotRecord{Data: data, Index: server.w.nowIndex})
			server.w.snapshotIndex = server.w.nowIndex
		}
		server.w.nowIndex ++
	default:
		//@do nothing

	}
	return nil
}

func InitNewWal( s *Server) {
	s.w  = &Wal{snapdir:s.conf.datadir+"snap/",waldir:s.conf.datadir+"wal/",snapcount:s.conf.snapCount}
	s.w.s = s
	var err error
	s.w.wal,err =wal.New(s.w.waldir)
	if err != nil {
		log.Fatal(err)
	}
	_,err = os.Stat(s.w.snapdir)
	if os.IsNotExist(err) {
		if err := os.Mkdir(s.w.snapdir, 0750); err != nil {
			log.Fatalf("raft-redis: cannot create dir for wal (%v)", err)
		}
	}
	s.w.snapshotter = snap.New(s.w.snapdir)
	s.w.replayWAL()
	if s.conf.walsavetype  == "es" {
		go func() {
			for {
				if len(ents) > 0 {
					entscopy := ents
					ents =[]structure.Entry{}
					//s.mu.Unlock()
					for _,v := range entscopy {
						s.w.wal.SaveEntry(&v)
					}
				}
				time.Sleep(1 * time.Second)
			}
		}()
	}
}