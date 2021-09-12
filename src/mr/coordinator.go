package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"

	"6.824/models"
)

type Coordinator struct {
	// Your definitions here.
	Tasks chan models.Task
	M     int
	R     int
	mwg   sync.WaitGroup
	rwg   sync.WaitGroup
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) AskTask(args *Args, reply *Reply) error {
	var ok bool
	if reply.Task, ok = <-c.Tasks; !ok {
		// fmt.Println("channel closed.")
		reply.Type = models.END
	} else {
		if reply.Type == models.REDUCE {
			// fmt.Println("wait map done ...")
			c.mwg.Wait()
			// fmt.Printf("alloc reduce task %v.\n", reply.XY)
		} else {
			// fmt.Printf("alloc map task %v.\n", reply.XY)
		}
	}
	return nil
}

func (c *Coordinator) SubmitTask(args *Args, reply *Reply) error {
	if args.Type == models.MAP {
		// fmt.Printf("submit map task %v\n", args.XY)
		c.mwg.Done()
	} else if args.Type == models.REDUCE {
		// fmt.Printf("submit reduce task %v\n", args.XY)
		c.rwg.Done()
	} else {
		log.Fatalf("unknown args type %v.", args.Type)
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
	// Your code here.
	// fmt.Println("done?")
	c.rwg.Wait()
	close(c.Tasks)
	// fmt.Println("done!")
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		M:     len(files),
		R:     nReduce,
		Tasks: make(chan models.Task, len(files)+nReduce),
	}

	// Your code here.
	for x, file := range files {
		c.Tasks <- models.Task{
			Files: []string{file},
			XY:    x,
			M:     c.M,
			R:     c.R,
			Type:  models.MAP,
		}
		c.mwg.Add(1)
	}

	for y := 0; y < c.R; y++ {
		intermediates := make([]string, c.M)
		for x := 0; x < c.M; x++ {
			intermediates[x] = fmt.Sprintf("mr-%v-%v", x, y)
		}
		c.Tasks <- models.Task{
			Files: intermediates,
			XY:    y,
			M:     c.M,
			R:     c.R,
			Type:  models.REDUCE,
		}
		c.rwg.Add(1)
	}

	c.server()
	return &c
}
