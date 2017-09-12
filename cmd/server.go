package main

import (
	"log"
	"github.com/widaT/newredis"
	"flag"
	"fmt"
)

//var addr = ":6380"
func main() {
	s := flag.Uint64("s",1000000,"snapshot count")
	w :=flag.String("w","aw","use wal to save data to disk,es every second ,al allways,no no use wal")
	p :=flag.Int("p",6380,"net port")
	flag.Parse()
	c := newredis.DefaultConfig().SnapCount(*s).OpenWal(*w).Laddr(fmt.Sprintf(":%d",*p))
	go log.Printf("started server at %s", c.Gaddr())
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