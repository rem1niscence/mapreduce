package mr

import (
	"slices"
	"sync"
	"time"
)

const (
	MapTask    = "map"
	ReduceTask = "reduce"
)

// Task is a struct that represents a task that a worker can perform.
type Task struct {
	taskType string
	files    []string
	start    time.Time
	taskNum  int
}

// Tasks is a struct that represents a queue of tasks.
type Tasks struct {
	pending  []string
	active   []Task
	taskType string
	mu       sync.Mutex
	Num      int
}

// Add adds a file task to the pending queue.
func (t *Tasks) Add(files []string) {
	t.mu.Lock()
	for _, file := range files {
		if slices.Index(t.pending, file) == -1 {
			t.pending = append(t.pending, file)
		}
	}
	t.mu.Unlock()
}

// Request returns a task from the pending queue. Or an empty task if there are
// none available.
func (t *Tasks) Request() Task {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.pending) == 0 {
		return Task{}
	}

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

	return task
}

// Complete removes an active task from the active queue.
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

// Pending returns the pending tasks.
func (t *Tasks) Pending() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pending
}

// Active returns the active tasks.
func (t *Tasks) Active() []Task {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.active
}

// Empty returns whether there are no pending or active tasks.
func (t *Tasks) Empty() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending) == 0 && len(t.active) == 0
}
