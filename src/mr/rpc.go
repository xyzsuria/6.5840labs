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

type MapArgs struct{
}

type MapReply struct{
	// 如果没有在运行 map，运行 reduce
	RunMap bool
}

// 定义 task 的状态
const (
	TaskMap = 0
	TaskReduce = 1
	TaskWait = 2
	TaskEnd = 3
)

type TaskInfo struct{
	State int
	FileName string
	// 执行到第几个 file
	FileIndex int
	PartIndex int

	NReduce int
	NFiles int
}


// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
