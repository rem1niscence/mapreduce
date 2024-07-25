package mr

import (
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
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

func (w *Worker) PerformJob() {
	// Get job from server
	job := JobArgs{}
	err := w.rpcClient.Call("Coordinator.RunJob", EmptyArgs{}, &job)
	if err != nil {
		log.Println("error getting job from coordinator", err)
	}

	switch job.JobType {
	case "map":
		fmt.Printf("new job: %s #%d\n", job.Filenames[0], job.TaskNumber)
		_, err := w.Map(job)
		if err != nil {
			log.Println("map job failed:", err)
		}
	}
}

// Map runs the plugin's Map function on the given file and content
// and sorts the output by key to be placed in separate buckets based on NReduce.
func (w *Worker) Map(job JobArgs) ([]string, error) {
	file, err := os.Open(job.Filenames[0])
	if err != nil {
		return nil, err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	keyValues := w.Mapper(job.Filenames[0], string(content))

	return CreateReduceTasks(keyValues, job.TaskNumber, job.ReducePath, job.NReduce)
}

// CreateReduceTasks writes the intermediate key-values to the reduce tasks files, sorted by key.
func CreateReduceTasks(values []KeyValue, taskNumber int, path string, nReduce int) ([]string, error) {
	// Sort by key so all values for a key are grouped in the same bucket
	sort.Sort(ByKey(values))

	// Create nReduce files
	files := make([]*os.File, nReduce)
	filenames := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf("mr-out-%d-%d", taskNumber, i)
		ofile, err := os.Create(filepath.Join(path, oname))
		if err != nil {
			return nil, fmt.Errorf("cannot create file %s: %v", oname, err)
		}
		files[i] = ofile
		filenames[i] = oname

		defer ofile.Close()
	}

	// Separate file into buckets
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range values {
		bucket := ihash(kv.Key) % nReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}

	// Write to files
	for i, bucket := range buckets {
		err := gob.NewEncoder(files[i]).Encode(bucket)
		if err != nil {
			return nil, fmt.Errorf("cannot encode bucket %d: %v", i, err)
		}
	}

	return filenames, nil
}

// Stop closes the rpc connection
// TODO: Signal application to stop
func (w *Worker) Stop() error {
	return w.rpcClient.Close()
}

// ihash returns a hash of the key
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
