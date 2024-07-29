package mr

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type EmptyArgs struct{}

type TaskArgs struct {
	TaskType   string
	Filenames  []string
	NReduce    int
	ReducePath string
	Number     int
}
