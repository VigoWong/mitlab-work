package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskType int

const (
	MapType TaskType = iota + 1
	ReduceType
	SleepType
	EndType
)

type WorkerState int

const (
	MapState WorkerState = iota + 1
	ReduceState
	InitState
)

// TaskArgs The msg from worker to acquire job
type TaskArgs struct {
	ID    int         // Task ID finished
	State WorkerState // 1 - map | 2 - reduce | 3 - Init
	PID   int         // Process ID
}

// TaskReply The reply msg from coordinator, including detail of task
type TaskReply struct {
	Type TaskType // 1 - map job | 2 - reduce job | 3 - sleep job

	/*
		Task ID, it has special meaning. For reduce job, it means the final output file ID. E.g: mr-out-<ID>
		For map job, it represent the id of map out file mr. E.g:
				mr-X-Y, where X is the Map task number, and Y is the reduce task number.
	*/
	ID       int
	FileName string // it's meaningful for map job only
	NReduce  int    // number of reduce job
	NMap     int    // number of map job
}

// Cook up a unique-ish UNIX-domain socket name in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
