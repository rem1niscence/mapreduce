package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type task struct {
	taskType string
	files    []string
	start    time.Time
	taskNum  int
}

type Coordinator struct {
	activeMaps  []task
	pendingMaps []string

	activeReduces  []task
	pendingReduces []string

	reducePath string
	mu         sync.Mutex
	taskNum    int
	nReduce    int
}

func (c *Coordinator) Run() {

}

func (c *Coordinator) RequestTask(args EmptyArgs, reply *TaskArgs) error {
	if len(c.pendingMaps) > 0 {
		return c.NewMapTask(reply)
	}
	// else if len(c.pendingReduces) > 0 {

	// }
	fmt.Println("No tasks available")
	return nil
}

func (c *Coordinator) NewMapTask(reply *TaskArgs) error {
	c.mu.Lock()
	file := c.pendingMaps[len(c.pendingMaps)-1]
	c.pendingMaps = c.pendingMaps[:len(c.pendingMaps)-1]

	reply.TaskType = "map"
	reply.Filenames = []string{file}
	reply.ReducePath = c.reducePath
	reply.NReduce = c.nReduce
	reply.TaskNumber = c.taskNum

	c.activeMaps = append(c.activeMaps, task{
		taskType: "map",
		files:    []string{file},
		start:    time.Now(),
		taskNum:  c.taskNum,
	})
	c.taskNum++

	c.mu.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire task has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func NewCoordinator(reduceFolder string, files []string, nReduce int) *Coordinator {

	c := Coordinator{
		mu:          sync.Mutex{},
		pendingMaps: files,
		reducePath:  reduceFolder,
		nReduce:     nReduce,
	}

	// Your code here.

	c.server()
	return &c
}
