package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type Task struct {
	Filename  string
	Status    TaskStatus
	StartedAt time.Time
	Attempt   int
}

type Coordinator struct {
	mu       sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	nReduce     int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.Type = WaitTask
	reply.NReduce = c.nReduce
	reply.NMap = len(c.mapTasks)

	if !allCompleted(c.mapTasks) {
		c.assignTask(c.mapTasks, MapTask, reply)
		return nil
	}

	if !allCompleted(c.reduceTasks) {
		c.assignTask(c.reduceTasks, ReduceTask, reply)
		return nil
	}

	reply.Type = ExitTask
	return nil
}

func allCompleted(tasks []Task) bool {
	for _, task := range tasks {
		if task.Status != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) assignTask(tasks []Task, taskType TaskType, reply *RequestTaskReply) {
	for id := range tasks {
		task := &tasks[id]
		if task.Status != Idle && (task.Status != InProgress || time.Since(task.StartedAt) < 10*time.Second) {
			continue
		}

		task.Status = InProgress
		task.StartedAt = time.Now()
		task.Attempt++

		reply.Type = taskType
		reply.TaskID = id
		reply.Filename = task.Filename
		reply.Attempt = task.Attempt
		return
	}
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var tasks []Task
	switch args.Type {
	case MapTask:
		tasks = c.mapTasks
	case ReduceTask:
		tasks = c.reduceTasks
	default:
		return nil
	}

	if args.TaskID < 0 || args.TaskID >= len(tasks) {
		return nil
	}
	task := &tasks[args.TaskID]
	if task.Status == InProgress && task.Attempt == args.Attempt {
		task.Status = Completed
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return allCompleted(c.mapTasks) && allCompleted(c.reduceTasks)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{nReduce: nReduce}

	for _, filename := range files {
		c.mapTasks = append(c.mapTasks, Task{Filename: filename, Status: Idle})
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, Task{Status: Idle})
	}

	c.server(sockname)
	return &c
}
