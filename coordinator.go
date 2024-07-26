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

func (c *Coordinator) MonitorPendingTasks() {
	ticker := time.NewTicker(500 * time.Millisecond)

	// TODO: Implement a way to check if the coordinator is done to break out of the loop
	for range ticker.C {
		c.mu.Lock()
		for _, task := range c.activeMaps {
			if time.Since(task.start) > 10*time.Second {
				c.pendingMaps = append(c.pendingMaps, task.files...)
				c.activeMaps = c.activeMaps[1:]
			}
		}
		for _, task := range c.activeReduces {
			if time.Since(task.start) > 10*time.Second {
				c.pendingReduces = append(c.pendingReduces, task.files...)
				c.pendingMaps = c.pendingMaps[1:]
			}
		}
		c.mu.Unlock()
	}
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

func (c *Coordinator) CompleteTask(task TaskArgs, reply *EmptyArgs) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch task.TaskType {
	case "map":
		for i, t := range c.activeMaps {
			if t.taskNum == task.TaskNumber {
				c.activeMaps = append(c.activeMaps[:i], c.activeMaps[i+1:]...)
				return
			}
		}
		c.pendingReduces = append(c.pendingReduces, task.Filenames...)
	case "reduce":
		for i, t := range c.activeReduces {
			if t.taskNum == task.TaskNumber {
				c.activeReduces = append(c.activeReduces[:i], c.activeReduces[i+1:]...)
				return
			}
		}
	}

	// TODO: Check if all tasks are done to exit the program

	return fmt.Errorf("unknown task type: %s", task.TaskType)
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

	go c.MonitorPendingTasks()

	c.server()
	return &c
}
