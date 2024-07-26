package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"

	"golang.org/x/sys/unix"
)

type MapFunc func(string, string) []KeyValue
type ReduceFunc func(string, []string) string

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Worker struct {
	Mapper    MapFunc
	Reducer   ReduceFunc
	rpcClient *rpc.Client
}

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

func (w *Worker) PerformTask(task TaskArgs) error {
	// fmt.Printf("new task: [%v] %v #%d\n", task.TaskType, task.Filenames, task.Number)

	switch task.TaskType {
	case MapTask:
		keyValues, err := w.Map(task)
		if err != nil {
			return err
		}
		reduceFiles, err := CreateReduceTasks(keyValues, task.Number, task.ReducePath, task.NReduce)
		if err != nil {
			return err
		}
		err = w.rpcClient.Call("Coordinator.CompleteTask", &TaskArgs{
			TaskType:  task.TaskType,
			Number:    task.Number,
			Filenames: reduceFiles,
		}, &EmptyArgs{})
		if err != nil {
			return err
		}

	case ReduceTask:
		keyValues, err := w.Reduce(task)
		if err != nil {
			return fmt.Errorf("reducer: function: %v", err)
		}
		ofile, err := os.Create(fmt.Sprintf("mr-out-%d", task.Number))
		if err != nil {
			return fmt.Errorf("reducer: create file: %v", err)
		}
		defer ofile.Close()

		for _, kv := range keyValues {
			fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
		}

		err = w.rpcClient.Call("Coordinator.CompleteTask", &TaskArgs{
			TaskType:  task.TaskType,
			Number:    task.Number,
			Filenames: task.Filenames,
		}, &EmptyArgs{})
		if err != nil {
			return fmt.Errorf("reducer: signal complete task: %v", err)
		}
	default:
		return fmt.Errorf("reducer: unknown task type: %s", task.TaskType)
	}

	return nil
}

func (w *Worker) RequestTask() (TaskArgs, error) {
	task := TaskArgs{}
	err := w.rpcClient.Call("Coordinator.RequestTask", EmptyArgs{}, &task)
	if err != nil {
		return TaskArgs{}, err
	}

	return task, nil
}

// Map runs the plugin's Map function on the given file and content
// and sorts the output by key to be placed in separate buckets based on NReduce.
func (w *Worker) Map(task TaskArgs) ([]KeyValue, error) {
	file, err := os.Open(task.Filenames[0])
	if err != nil {
		return nil, err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	return w.Mapper(task.Filenames[0], string(content)), nil
}

func (w *Worker) Reduce(task TaskArgs) ([]KeyValue, error) {
	reducedValues := make([]KeyValue, 0)

	path := filepath.Join(task.ReducePath, task.Filenames[0])
	file, err := os.Open(path)
	if err != nil {
		return reducedValues, fmt.Errorf("open file %s: %v", path, err)
	}
	defer file.Close()

	intermediateValues := make([]KeyValue, 0)
	err = json.NewDecoder(file).Decode(&intermediateValues)
	if err != nil {
		return reducedValues, fmt.Errorf("decode file %s: %v", path, err)
	}

	// call Reduce on each distinct key in keyValues[],
	i := 0
	for i < len(intermediateValues) {
		j := i + 1
		for j < len(intermediateValues) && intermediateValues[j].Key == intermediateValues[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediateValues[k].Value)
		}
		out := w.Reducer(intermediateValues[i].Key, values)
		reducedValues = append(reducedValues, KeyValue{
			Key:   intermediateValues[i].Key,
			Value: out,
		})

		i = j
	}

	return reducedValues, nil
}

// Stop closes the rpc connection
// TODO: Signal application to stop
func (w *Worker) Stop() error {
	return w.rpcClient.Close()
}

// CreateReduceTasks writes the intermediate key-values to the reduce tasks files, sorted by key.
func CreateReduceTasks(values []KeyValue, taskNumber int, path string, nReduce int) ([]string, error) {
	// Create nReduce files
	files := make([]*os.File, nReduce)
	filenames := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf("mp-bucket-%d", i)

		ofile, err := os.OpenFile(filepath.Join(path, oname), os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, fmt.Errorf("open file %s: %v", oname, err)
		}
		defer ofile.Close()

		// Apply an exclusive lock to the file
		err = unix.Flock(int(ofile.Fd()), unix.LOCK_EX)
		if err != nil {
			return nil, fmt.Errorf("lock file %s: %v", oname, err)
		}
		// Unlock the file when done
		defer unix.Flock(int(ofile.Fd()), unix.LOCK_UN)

		files[i] = ofile
		filenames[i] = oname
	}

	// Separate file into buckets
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range values {
		bucket := ihash(kv.Key) % nReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}

	// Write to files
	for i, bucket := range buckets {
		i := i
		content, err := io.ReadAll(files[i])
		if err != nil {
			return nil, fmt.Errorf("read file %s: %v", filenames[i], err)
		}

		// Override file content if it exists
		if len(content) > 0 {
			fileKeyValues := make([]KeyValue, 0)

			if err := json.NewDecoder(bytes.NewReader(content)).Decode(&fileKeyValues); err != nil {
				return nil, fmt.Errorf("decode file %s: %v", filenames[i], err)
			}

			// Truncate the file to zero length to overwrite its content
			err = files[i].Truncate(0)
			if err != nil {
				return nil, fmt.Errorf("truncate file %s: %v", filenames[i], err)
			}

			// Move the file pointer to the beginning of the file
			_, err = files[i].Seek(0, 0)
			if err != nil {
				return nil, fmt.Errorf("seek file %s: %v", filenames[i], err)
			}

			bucket = append(bucket, fileKeyValues...)
		}

		// Sort by key so all values for a key are grouped in the same bucket
		sort.Sort(ByKey(bucket))

		err = json.NewEncoder(files[i]).Encode(bucket)
		if err != nil {
			return nil, fmt.Errorf("cannot encode bucket %d: %v", i, err)
		}
	}

	return filenames, nil
}

// ihash returns a hash of the key
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
