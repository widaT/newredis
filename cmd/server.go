package main

import (
	"log"
	"github.com/widaT/newredis"
)

var addr = ":6380"

func main() {
	go log.Printf("started server at %s", addr)
	err := newredis.ListenAndServe(addr,
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
