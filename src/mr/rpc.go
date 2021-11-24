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

// DistrubuteTaskArgs Add your RPC definitions here.
type DistrubuteTaskArgs struct {
}

type DistrubuteTaskReply struct {
	TaskType               int
	TaskNumber             int
	IntermediateFilePrefix string
	MT                     *MapTask
	RT                     *ReduceTask
}

type TaskError struct {
	info string
}

func (d TaskError) Error() string {
	return d.info
}

type FinishTaskArgs struct {
	TaskType   int
	TaskNumber int
}

type FinishTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
