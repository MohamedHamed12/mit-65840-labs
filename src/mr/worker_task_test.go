package mr

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkerMapAndReduce(t *testing.T) {
	dir := t.TempDir()
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(old)

	if err := os.WriteFile("input.txt", []byte("go go map"), 0600); err != nil {
		t.Fatal(err)
	}
	mapf := func(_ string, content string) []KeyValue {
		pairs := []KeyValue{}
		for _, word := range strings.Fields(content) {
			pairs = append(pairs, KeyValue{Key: word, Value: "1"})
		}
		return pairs
	}
	if err := runMapTask(RequestTaskReply{TaskID: 0, Filename: "input.txt", NReduce: 1}, mapf); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "mr-0-0")); err != nil {
		t.Fatal(err)
	}

	reducef := func(_ string, values []string) string {
		return string(rune('0' + len(values)))
	}
	if err := runReduceTask(RequestTaskReply{TaskID: 0, NMap: 1}, reducef); err != nil {
		t.Fatal(err)
	}
	output, err := os.ReadFile("mr-out-0")
	if err != nil {
		t.Fatal(err)
	}
	if string(output) != "go 2\nmap 1\n" {
		t.Fatalf("unexpected output: %q", output)
	}
}
