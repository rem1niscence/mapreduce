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

type Coordinator struct {
	maps    Tasks
	reduces Tasks

	reducePath string
	mu         sync.Mutex
	nReduce    int
}

func (c *Coordinator) Run() {

}

func (c *Coordinator) MonitorPendingTasks() {
	ticker := time.NewTicker(500 * time.Millisecond)

	// TODO: Implement a way to check if the coordinator is done to break out of the loop
	for range ticker.C {
		for _, task := range c.maps.Active() {
			if time.Since(task.start) > 10*time.Second {
				c.maps.Complete(task.taskNum)
				c.maps.Add(task.files)
			}
		}
		for _, task := range c.reduces.Active() {
			if time.Since(task.start) > 10*time.Second {
				c.reduces.Complete(task.taskNum)
				c.reduces.Add(task.files)
			}
		}
	}
}

func (c *Coordinator) RequestTask(args EmptyArgs, reply *TaskArgs) error {
	if len(c.maps.pending) > 0 {
		task, err := c.maps.Request()
		if err != nil {
			return err
		}
		reply.TaskType = task.taskType
		reply.Filenames = task.files
		reply.Number = task.taskNum
		reply.NReduce = c.nReduce
		reply.ReducePath = c.reducePath

		return nil
	}

	if len(c.reduces.pending) > 0 {
		task, err := c.reduces.Request()
		if err != nil {
			return err
		}
		reply.TaskType = task.taskType
		reply.Filenames = task.files
		reply.Number = task.taskNum
		reply.ReducePath = c.reducePath

		return nil
	}

	fmt.Println("No tasks available")
	return nil
}

func (c *Coordinator) CompleteTask(task TaskArgs, reply *EmptyArgs) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch task.TaskType {
	case MapTask:
		c.maps.Complete(task.Number)
		c.reduces.Add(task.Filenames)
	case ReduceTask:
		c.reduces.Complete(task.Number)
	}

	// TODO: Check if all tasks are done to exit the program
	return fmt.Errorf("unknown task type: %s", task.TaskType)
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
		mu: sync.Mutex{},
		maps: Tasks{
			taskType: MapTask,
			pending:  files,
		},
		reduces: Tasks{
			taskType: ReduceTask,
		},
		reducePath: reduceFolder,
		nReduce:    nReduce,
	}

	go c.MonitorPendingTasks()

	c.server()
	return &c
}
