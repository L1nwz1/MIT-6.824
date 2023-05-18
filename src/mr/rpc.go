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

type TaskRequest struct {
}

type TaskResponse struct {
	XTask 				Task // map tasks 
	NumMapTasks 		int // number of map tasks
	NumReduceTasks 		int // number of reduce tasks
	Id 					int // task id
	MapDone 			chan bool // map tasks
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

func AskForTask() *Task {
	args := ExampleArgs{}
	reply := ExampleReply{}
	call("Coordinator.Example", &args, &reply)
	return nil
}
