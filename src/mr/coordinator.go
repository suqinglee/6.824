package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"

	"6.824/models"
)

type Coordinator struct {
	// Your definitions here.
	Tasks    chan models.Task
	InMap    map[int]bool
	InReduce map[int]bool
	DoneMap int
	DoneReduce int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) AskTask(args *Args, reply *Reply) error {
	var ok bool
	if reply.TaskInfo, ok = <-c.Tasks; ok {
		reply.TaskInfo.StartTime = time.Now().Unix()
		c.InMap[reply.TaskInfo.X] = true
	} else {
		reply.TaskInfo.Type = models.DONE
	}
	return nil
}

func (c *Coordinator) SubmitTask(args *Args, reply *Reply) error {
	if args.TaskInfo.Type == models. {
		c.Tasks <- models.Task{
			FileName: args.TaskInfo,
			X:        i,
			M:        len(files),
			R:        nReduce,
			Type:     models.MAP,
		}
	}
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
		Tasks: make(chan models.Task, len(files)),
	}

	// Your code here.
	for i, file := range files {
		c.Tasks <- models.Task{
			FileName: file,
			X:        i,
			M:        len(files),
			R:        nReduce,
			Type:     models.MAP,
		}
	}

	c.server()
	close(c.Tasks)
	return &c
}
