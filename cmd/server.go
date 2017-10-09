package main

import (
	"log"
	"github.com/widaT/newredis"
	"flag"
	"fmt"
	"net/http"
	_  "net/http/pprof"
)

const VERSION = "newredis v0.1"
func main() {
	s := flag.Uint64("s",1000000,"snapshot count")
	w :=flag.String("w","aw","use wal to save data to disk al allways,es every second ,no no use wal")
	p :=flag.Int("p",6380,"port for net listen")
	P :=flag.Bool("P",false,"profiling this program")
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

	c := newredis.DefaultConfig().SnapCount(*s).OpenWal(*w).Laddr(fmt.Sprintf(":%d",*p))
	go log.Printf("started server at %s wal model %s", c.Gaddr(),c.Gwalsavetype())
	err := newredis.ListenAndServe(c,
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