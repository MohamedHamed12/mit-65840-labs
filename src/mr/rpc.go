package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

// Add your RPC definitions here.

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type TaskType int

const (
	MapTask TaskType = iota
	WaitTask
	ReduceTask
	ExitTask
)

type RequestTaskArgs struct{}

type RequestTaskReply struct {
	Type     TaskType
	TaskID   int
	Filename string
	NReduce  int
	NMap     int
	Attempt  int
}

type ReportTaskArgs struct {
	Type    TaskType
	TaskID  int
	Attempt int
}

type ReportTaskReply struct{}
