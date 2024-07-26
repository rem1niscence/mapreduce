package mr

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

const (
	MapTask    = "map"
	ReduceTask = "reduce"
)

type Task struct {
	taskType string
	files    []string
	start    time.Time
	taskNum  int
}

type Tasks struct {
	pending  []string
	active   []Task
	taskType string
	mu       sync.Mutex
	Num      int
}

func (t *Tasks) Add(files []string) {
	t.mu.Lock()
	for _, file := range files {
		if slices.Index(t.pending, file) == -1 {
			t.pending = append(t.pending, file)
		}
	}
	t.mu.Unlock()
}

func (t *Tasks) Request() (Task, error) {
	if len(t.pending) == 0 {
		return Task{}, fmt.Errorf("no tasks available")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	file := t.pending[len(t.pending)-1]
	t.pending = t.pending[:len(t.pending)-1]
	task := Task{
		taskType: t.taskType,
		files:    []string{file},
		start:    time.Now(),
		taskNum:  t.Num,
	}

	t.Num++
	t.active = append(t.active, task)

	return task, nil
}

func (t *Tasks) Complete(taskNum int) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i, active := range t.active {
		if active.taskNum == taskNum {
			t.active = append(t.active[:i], t.active[i+1:]...)
			return true
		}
	}
	return false
}

func (t *Tasks) Pending() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pending
}

func (t *Tasks) Active() []Task {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.active
}

func (t *Tasks) Empty() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending) == 0 && len(t.active) == 0
}
