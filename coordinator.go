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
	Done       chan struct{}
	closeOnce  sync.Once
}

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

func (c *Coordinator) RequestTask(args EmptyArgs, reply *TaskArgs) error {
	if !c.maps.Empty() {
		if len(c.maps.Pending()) == 0 {
			return nil
		}

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

	if !c.reduces.Empty() {
		if len(c.reduces.Pending()) == 0 {
			return nil
		}

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

	return nil
}

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
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
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
		closeOnce:  sync.Once{},
		Done:       make(chan struct{}),
	}

	go c.MonitorPendingTasks()

	c.server()
	return &c
}
