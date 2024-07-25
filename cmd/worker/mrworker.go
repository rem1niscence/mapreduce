package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import (
	"fmt"
	"log"
	"os"
	"plugin"
	"time"

	mr "github.com/rem1niscence/mapReduce"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}

	mapFunc, reduceFunc := loadPlugin(os.Args[1])

	worker, err := mr.NewWorker(mapFunc, reduceFunc)
	if err != nil {
		log.Fatalf("cannot create worker: %v", err)
	}

	stop := make(chan struct{})

	go RequestJob(worker, stop)

	<-stop
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	mapSymbol, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapFunc := mapSymbol.(func(string, string) []mr.KeyValue)
	reduceSymbol, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reduceFunc := reduceSymbol.(func(string, []string) string)

	return mapFunc, reduceFunc
}

// RequestJob periodically pings the coordinator for new jobs to perform
func RequestJob(worker *mr.Worker, stopCh chan struct{}) {
	ticker := time.NewTicker(2 * time.Second)

	fmt.Println("Requesting job from coordinator")
	for range ticker.C {

		// Ping server
		worker.PerformTask()
	}

	stopCh <- struct{}{}
}
