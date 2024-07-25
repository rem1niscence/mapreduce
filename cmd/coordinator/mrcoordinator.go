package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"log"
	"os"
	"time"

	mr "github.com/rem1niscence/mapReduce"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	reduceFolder, err := os.MkdirTemp("../testdata/tmp", "mr")
	if err != nil {
		log.Fatal("create temp folder failed:", err)
	}
	defer os.RemoveAll(reduceFolder)

	m := mr.NewCoordinator(reduceFolder, os.Args[1:], 10)
	for !m.Done() {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
