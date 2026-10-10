package mr

import (
	"testing"
	"time"
)

func TestCoordinatorMapReduceLifecycle(t *testing.T) {
	c := Coordinator{
		mapTasks:    []Task{{Filename: "a.txt"}, {Filename: "b.txt"}},
		reduceTasks: make([]Task, 2),
		nReduce:     2,
	}

	request := func() RequestTaskReply {
		t.Helper()
		reply := RequestTaskReply{}
		if err := c.RequestTask(&RequestTaskArgs{}, &reply); err != nil {
			t.Fatal(err)
		}
		return reply
	}
	report := func(task RequestTaskReply) {
		t.Helper()
		if err := c.ReportTask(&ReportTaskArgs{Type: task.Type, TaskID: task.TaskID, Attempt: task.Attempt}, &ReportTaskReply{}); err != nil {
			t.Fatal(err)
		}
	}

	first, second := request(), request()
	if first.Type != MapTask || second.Type != MapTask || first.TaskID == second.TaskID {
		t.Fatalf("expected distinct map tasks, got %+v and %+v", first, second)
	}
	if request().Type != WaitTask {
		t.Fatal("expected to wait while maps are in progress")
	}
	report(first)
	if request().Type != WaitTask {
		t.Fatal("reduce must wait for all maps")
	}
	report(second)

	reduce0, reduce1 := request(), request()
	if reduce0.Type != ReduceTask || reduce1.Type != ReduceTask || reduce0.TaskID == reduce1.TaskID {
		t.Fatalf("expected distinct reduce tasks, got %+v and %+v", reduce0, reduce1)
	}
	if reduce0.NMap != 2 || reduce1.NReduce != 2 {
		t.Fatal("missing map/reduce counts")
	}
	if c.Done() || request().Type != WaitTask {
		t.Fatal("job must wait for reducers")
	}
	report(reduce0)
	report(reduce1)
	if !c.Done() || request().Type != ExitTask {
		t.Fatal("expected completed job and exit task")
	}
}

func TestCoordinatorTimeoutRejectsStaleAttempt(t *testing.T) {
	c := Coordinator{mapTasks: []Task{{Filename: "a.txt"}}, reduceTasks: []Task{{}}, nReduce: 1}

	first := RequestTaskReply{}
	c.RequestTask(&RequestTaskArgs{}, &first)
	c.mapTasks[0].StartedAt = time.Now().Add(-11 * time.Second)

	retry := RequestTaskReply{}
	c.RequestTask(&RequestTaskArgs{}, &retry)
	if retry.Type != MapTask || retry.TaskID != first.TaskID || retry.Attempt != first.Attempt+1 {
		t.Fatalf("expected reassigned map, got %+v", retry)
	}
	c.ReportTask(&ReportTaskArgs{Type: first.Type, TaskID: first.TaskID, Attempt: first.Attempt}, &ReportTaskReply{})
	if c.mapTasks[0].Status != InProgress {
		t.Fatal("stale report completed reassigned task")
	}
	c.ReportTask(&ReportTaskArgs{Type: retry.Type, TaskID: retry.TaskID, Attempt: retry.Attempt}, &ReportTaskReply{})
	reduce := RequestTaskReply{}
	c.RequestTask(&RequestTaskArgs{}, &reduce)
	if reduce.Type != ReduceTask {
		t.Fatalf("expected reduce, got %+v", reduce)
	}
	c.reduceTasks[0].StartedAt = time.Now().Add(-11 * time.Second)
	reduceRetry := RequestTaskReply{}
	c.RequestTask(&RequestTaskArgs{}, &reduceRetry)
	if reduceRetry.Type != ReduceTask || reduceRetry.Attempt != reduce.Attempt+1 {
		t.Fatalf("expected reassigned reduce, got %+v", reduceRetry)
	}
	c.ReportTask(&ReportTaskArgs{Type: reduce.Type, TaskID: reduce.TaskID, Attempt: reduce.Attempt}, &ReportTaskReply{})
	if c.Done() {
		t.Fatal("stale reduce report completed job")
	}
	c.ReportTask(&ReportTaskArgs{Type: reduceRetry.Type, TaskID: reduceRetry.TaskID, Attempt: reduceRetry.Attempt}, &ReportTaskReply{})
	if !c.Done() {
		t.Fatal("valid retry did not complete job")
	}
}
