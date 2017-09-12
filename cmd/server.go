package main

import (
	"log"
	"github.com/widaT/newredis"
)

//var addr = ":6380"
func main() {
	c := newredis.DefaultConfig().SnapCount(10)
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