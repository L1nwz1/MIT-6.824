package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Task struct {
	FileName string
	IdMap int
	IdReduce int
}

type Coordinator struct {
	// Your definitions here.
	State 				int // 0: start 1: map, 2: reduce
	NumMapTasks 		int // number of map tasks
	NumReduceTasks 		int // number of reduce tasks
	MapTasks 			chan Task // map tasks
	ReduceTasks 		chan Task // reduce tasks
	MapTaskFin 			chan bool // map tasks
	ReduceTaskFin 		chan bool // reduce tasks
}


// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }
func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskResponse) error {
	if len(c.MapTaskFin) != c.NumMapTasks {
		MapTask, ok := <- c.MapTasks
		if ok {
			reply.XTask = MapTask
		}
	} else {
		reduceTask, ok := <- c.ReduceTasks
		if ok {
			reply.XTask = reduceTask
		}
	}

	reply.NumMapTasks = c.NumMapTasks
	reply.NumReduceTasks = c.NumReduceTasks

	return nil
}

func (c *Coordinator) TaskFin(args *ExampleArgs, reply *ExampleReply) error {
	if len(c.MapTaskFin) != c.NumMapTasks {
		c.MapTaskFin <- true
	} else {
		c.ReduceTaskFin <- true
	}

	return nil
}





//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	if len(c.ReduceTaskFin) == c.NumReduceTasks {
		ret = true
	}
	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		State : 					0,
		NumMapTasks : 				len(files),
		NumReduceTasks : 			nReduce,
		MapTasks : 					make(chan Task, len(files)),
		ReduceTasks : 				make(chan Task, nReduce),
		MapTaskFin : 				make(chan bool, len(files)),
		ReduceTaskFin : 			make(chan bool, nReduce),
	}

	// Start Map
	for id, file := range files {
		c.MapTasks <- Task{FileName: file, IdMap: id}
	}

	// Start Reduce
	for id := 0; id < nReduce; id++ {
		c.ReduceTasks <- Task{IdReduce: id}
	}



	c.server()
	return &c
}
