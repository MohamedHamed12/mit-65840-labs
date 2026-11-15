package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "encoding/json"
import "io"
import "sort"
import "time"


// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator


// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		reply := RequestTaskReply{}
		if !call("Coordinator.RequestTask", &RequestTaskArgs{}, &reply) {
			return
		}

		var err error
		switch reply.Type {
		case MapTask:
			err = runMapTask(reply, mapf)
		case ReduceTask:
			err = runReduceTask(reply, reducef)
		case WaitTask:
			time.Sleep(200 * time.Millisecond)
			continue
		case ExitTask:
			return
		default:
			log.Printf("unknown task type: %d", reply.Type)
			return
		}

		if err != nil {
			log.Printf("task %d failed: %v", reply.TaskID, err)
			return
		}

		args := ReportTaskArgs{Type: reply.Type, TaskID: reply.TaskID, Attempt: reply.Attempt}
		if !call("Coordinator.ReportTask", &args, &ReportTaskReply{}) {
			return
		}
	}
}

func runMapTask(task RequestTaskReply, mapf func(string, string) []KeyValue) error {
	content, err := os.ReadFile(task.Filename)
	if err != nil {
		return err
	}
	if task.NReduce <= 0 {
		return fmt.Errorf("invalid reduce count: %d", task.NReduce)
	}

	partitions := make([][]KeyValue, task.NReduce)
	for _, kv := range mapf(task.Filename, string(content)) {
		bucket := ihash(kv.Key) % task.NReduce
		partitions[bucket] = append(partitions[bucket], kv)
	}

	for bucket, pairs := range partitions {
		file, err := os.CreateTemp(".", "mr-map-*")
		if err != nil {
			return err
		}
		name := file.Name()
		encoder := json.NewEncoder(file)
		for _, kv := range pairs {
			if err = encoder.Encode(&kv); err != nil {
				break
			}
		}
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
		if err == nil {
			err = os.Rename(name, fmt.Sprintf("mr-%d-%d-%d", task.TaskID, task.Attempt, bucket))
		}
		if err != nil {
			os.Remove(name)
			return err
		}
	}
	return nil
}

func runReduceTask(task RequestTaskReply, reducef func(string, []string) string) error {
	if len(task.MapAttempts) != task.NMap {
		return fmt.Errorf("expected %d map attempts, got %d", task.NMap, len(task.MapAttempts))
	}
	var intermediate []KeyValue
	for mapID := 0; mapID < task.NMap; mapID++ {
		file, err := os.Open(fmt.Sprintf("mr-%d-%d-%d", mapID, task.MapAttempts[mapID], task.TaskID))
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = decoder.Decode(&kv)
			if err == io.EOF {
				break
			}
			if err != nil {
				file.Close()
				return err
			}
			intermediate = append(intermediate, kv)
		}
		if err := file.Close(); err != nil {
			return err
		}
	}

	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	file, err := os.CreateTemp(".", "mr-reduce-*")
	if err != nil {
		return err
	}
	name := file.Name()
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := make([]string, 0, j-i)
		for _, kv := range intermediate[i:j] {
			values = append(values, kv.Value)
		}
		if _, err = fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, reducef(intermediate[i].Key, values)); err != nil {
			break
		}
		i = j
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(name, fmt.Sprintf("mr-out-%d-attempt-%d", task.TaskID, task.Attempt))
	}
	if err != nil {
		os.Remove(name)
	}
	return err
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call the
	// Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Printf("dialing: %v", err)
		return false
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
