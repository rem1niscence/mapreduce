package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

// Coordinator is the coordinator struct that manages the map and reduce tasks
type Coordinator struct {
	maps    Tasks
	reduces Tasks

	reducePath string
	mu         sync.Mutex
	nReduce    int
	Done       chan struct{}
	closeOnce  sync.Once
}

// MonitorPendingTasks checks for tasks that have been pending for too long, and
// re-adds them to the pending queue.
func (c *Coordinator) MonitorPendingTasks() {
	ticker := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-c.Done:
			return
		case <-ticker.C:
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
}

// RequestTask is an RPC method that returns a task to a worker. All tasks of a given
// type must be completed before the coordinator will return a task of the next type.
func (c *Coordinator) RequestTask(args EmptyArgs, reply *TaskArgs) error {
	tasks := []*Tasks{&c.maps, &c.reduces}
	for _, task := range tasks {
		if task.Empty() {
			continue
		}
		if len(task.Pending()) == 0 {
			return nil
		}

		task := task.Request()
		if task.taskType == "" {
			return nil
		}
		reply.TaskType = task.taskType
		reply.Filenames = task.files
		reply.Number = task.taskNum
		reply.NReduce = c.nReduce
		reply.ReducePath = c.reducePath
		return nil
	}
	return nil
}

// CompleteTask is an RPC method that marks a task as complete.
// It shuts down the coordinator when all tasks are complete.
func (c *Coordinator) CompleteTask(task TaskArgs, reply *EmptyArgs) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch task.TaskType {
	case MapTask:
		c.maps.Complete(task.Number)
		c.reduces.Add(task.Filenames)
	case ReduceTask:
		c.reduces.Complete(task.Number)
	default:
		return fmt.Errorf("unknown task type: %s", task.TaskType)
	}

	if c.maps.Empty() && c.reduces.Empty() {
		c.closeOnce.Do(func() {
			close(c.Done)
		})
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":9090")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
}

// create a Coordinator.
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
		closeOnce:  sync.Once{},
		Done:       make(chan struct{}),
	}

	go c.MonitorPendingTasks()
	go c.server()

	return &c
}
