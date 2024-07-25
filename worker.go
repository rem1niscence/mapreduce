package mr

import (
	"fmt"
	"log"
	"net/rpc"
)

type MapFunc func(string, string) []KeyValue
type ReduceFunc func(string, []string) string

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type Worker struct {
	Mapper    MapFunc
	Reducer   ReduceFunc
	rpcClient *rpc.Client
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
// func ihash(key string) int {
// 	h := fnv.New32a()
// 	h.Write([]byte(key))
// 	return int(h.Sum32() & 0x7fffffff)
// }

func (w *Worker) PerformJob() {
	// Get job from server
	reply := JobArgs{}
	err := w.rpcClient.Call("Coordinator.RunJob", EmptyArgs{}, &reply)
	if err != nil {
		log.Println("error getting job from coordinator", err)
	}

	fmt.Printf("job name: %s file name: %s\n", reply.JobType, reply.Filenames)
}

// main/mrworker.go calls this function.
func NewWorker(mapf MapFunc, reducef ReduceFunc) (*Worker, error) {
	sockName := coordinatorSock()
	client, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		return nil, err
	}

	w := Worker{
		Mapper:    mapf,
		Reducer:   reducef,
		rpcClient: client,
	}

	return &w, nil
}

// Stop closes the rpc connection
// TODO: Signal application to stop
func (w *Worker) Stop() error {
	return w.rpcClient.Close()
}
