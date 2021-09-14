package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"6.824/models"
)

type Coordinator struct {
	// Your definitions here.
	Tasks   chan models.Task
	M       int
	R       int
	mwg     sync.WaitGroup
	rwg     sync.WaitGroup
	Running sync.Map
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) AskTask(args *Args, reply *Reply) error {
	var ok bool
	for {
		select {
		case reply.Task, ok = <-c.Tasks:
			if !ok {
				// fmt.Printf("channal closed, worker %v exit.\n", args.Worker)
				reply.Type = models.END
			} else {
				reply.Worker = args.Worker
				if reply.Type == models.MAP {
					// fmt.Printf("send task map %v to worker %v\n", reply.XY, reply.Worker)
				} else if reply.Type == models.REDUCE {
					// fmt.Printf("send task reduce %v to worker %v\n", reply.XY, reply.Worker)
				}
				c.Running.Store(reply.ToString(), time.Now().Unix())
			}
			return nil
		default:
			c.Running.Range(func(k, v interface{}) bool {
				taskString, startTime := k.(string), v.(int64)
				if startTime != -1 && time.Now().Unix()-startTime > 10 {
					// fmt.Printf("redo: %v\n", taskString)
					c.Running.Store(taskString, int64(-1))
					c.Tasks <- models.FromString(taskString)
				}
				return true
			})
		}
	}
}

func (c *Coordinator) SubmitTask(args *Args, reply *Reply) error {
	if startTime, ok := c.Running.Load(args.ToString()); ok && startTime == -1 {
		// fmt.Printf("submit fail: %v\n", args.ToString())
		return nil
	}
	c.Running.Delete(args.ToString())
	if args.Type == models.MAP {
		c.mwg.Done()
		// fmt.Printf("worker %v submit map %v\n", args.Worker, args.XY)
		for y, filename := range args.TempFiles {
			os.Rename(filename, fmt.Sprintf("mr-%v-%v", args.XY, y))
		}
	} else if args.Type == models.REDUCE {
		c.rwg.Done()
		// fmt.Printf("worker %v submit reduce %v\n", args.Worker, args.XY)
		for _, filename := range args.TempFiles {
			os.Rename(filename, fmt.Sprintf("mr-out-%v", args.XY))
		}
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
	c.mwg.Wait()
	c.rwg.Wait()
	close(c.Tasks)
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

	c.mwg.Add(c.M)
	c.rwg.Add(c.R)

	go func(c *Coordinator) {
		for x, file := range files {
			c.Tasks <- models.Task{
				Files: []string{file},
				XY:    x,
				M:     c.M,
				R:     c.R,
				Type:  models.MAP,
			}
			// fmt.Printf("add map %v\n", x)
		}

		c.mwg.Wait()

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
			// fmt.Printf("add reduce %v\n", y)
		}
	}(&c)

	c.server()
	return &c
}
