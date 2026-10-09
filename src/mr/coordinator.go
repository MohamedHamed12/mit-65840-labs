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
	mapTasks []Task
	nReduce  int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.Type = WaitTask
	reply.NReduce = c.nReduce

	for id := range c.mapTasks {
		task := &c.mapTasks[id]
		if task.Status != Idle {
			continue
		}

		task.Status = InProgress
		task.StartedAt = time.Now()
		task.Attempt++

		reply.Type = MapTask
		reply.TaskID = id
		reply.Filename = task.Filename
		return nil
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
	ret := false

	// Your code here.


	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{nReduce: nReduce}

	for _, filename := range files {
		c.mapTasks = append(c.mapTasks, Task{Filename: filename, Status: Idle})
	}

	c.server(sockname)
	return &c
}
