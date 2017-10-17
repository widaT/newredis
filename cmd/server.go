package main

import (
	"log"
	"github.com/widaT/newredis"
	"flag"
	"fmt"
	"net/http"
	_  "net/http/pprof"
	"os"
)

const VERSION = "newredis v0.1"

func main() {
	count := flag.Uint64("c", 10000, "snapshot count")
	s := flag.Bool("sync", false, "sync every wal recorder")
	w := flag.String("w", "aw", "use wal to save data to disk al allways,es every second ,no no use wal")
	d := flag.String("data", "data/", "dir to save wal and snapshot")
	p := flag.Int("p", 6380, "port for net listen")
	P := flag.Bool("P", false, "profiling this program")
	flag.Parse()

	if flag.Arg(0) == "version" {
		fmt.Println(VERSION)
		return
	}
	if *P {
		go func() {
			http.ListenAndServe("localhost:6060", nil)
		}()
	}
	dirpath := *d
	_, err := os.Stat(dirpath)
	if err != nil {
		if err := os.Mkdir(dirpath, 0750); err != nil {
			log.Fatalf("raft-redis: cannot create dir for wal (%v)", err)
		}
	}

	c := newredis.DefaultConfig().SnapCount(*count).OpenWal(*w).Laddr(fmt.Sprintf(":%d", *p)).DataDir(dirpath).Sync(*s)
	go log.Printf("started server at %s wal model %s", c.Gaddr(), c.Gwalsavetype())
	err = newredis.ListenAndServe(c,
		func(conn newredis.Conn) bool {
			return true
		},
		func(conn newredis.Conn, err error) {
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}
