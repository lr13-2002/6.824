package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
const (
	wait int = iota
	run
	done
)

type Job struct {
	FileName  string
	StartTime int64
	TaskId    int
	PTaskId   int
	Status    int
}

type Req struct {
	Flag string //"GET" "PUT"
	Job
}

type Rep struct {
	NReduce int
	Flag    string //"MAP" "REDUCE" "NONE"
	MaxId   int
	Job
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
