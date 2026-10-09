package mr

import (
	"sync"
	"testing"
)

func TestRequestTask(t *testing.T) {
	c := &Coordinator{
		mapTasks: []Task{
			{Filename: "first.txt", Status: Idle},
			{Filename: "second.txt", Status: Idle},
		},
		nReduce: 3,
	}

	var wg sync.WaitGroup
	replies := make(chan RequestTaskReply, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var reply RequestTaskReply
			if err := c.RequestTask(&RequestTaskArgs{}, &reply); err != nil {
				t.Errorf("RequestTask failed: %v", err)
				return
			}
			replies <- reply
		}()
	}
	wg.Wait()
	close(replies)

	seen := make(map[int]bool)
	for reply := range replies {
		if reply.Type != MapTask || reply.NReduce != 3 {
			t.Fatalf("unexpected task reply: %+v", reply)
		}
		if reply.TaskID < 0 || reply.TaskID >= len(c.mapTasks) {
			t.Fatalf("invalid task ID: %d", reply.TaskID)
		}
		if seen[reply.TaskID] {
			t.Fatalf("task %d assigned twice", reply.TaskID)
		}
		seen[reply.TaskID] = true
		if reply.Filename != c.mapTasks[reply.TaskID].Filename {
			t.Fatalf("wrong filename for task %d", reply.TaskID)
		}
	}

	for i, task := range c.mapTasks {
		if task.Status != InProgress || task.StartedAt.IsZero() || task.Attempt != 1 {
			t.Fatalf("task %d not correctly assigned: %+v", i, task)
		}
	}

	var reply RequestTaskReply
	if err := c.RequestTask(&RequestTaskArgs{}, &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Type != WaitTask {
		t.Fatalf("expected WaitTask, got %v", reply.Type)
	}
}
