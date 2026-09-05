package main

import (
	"flag"
	"fmt"
	"log"
)

func main() {
	port := flag.Int("port", 8080, "TCP port to listen on")
	capacity := flag.Int("capacity", 1000, "maximum number of keys the store holds before LRU eviction")
	flag.Parse()

	store := NewStore(*capacity)
	addr := fmt.Sprintf(":%d", *port)

	if err := ListenAndServe(addr, store); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
