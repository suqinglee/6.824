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
	mutex      sync.Mutex
	M          int
	R          int
	Tasks      chan models.Task
	DeadWorker map[int]bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) AskTask(args *Args, reply *Reply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for {
		var ok bool
		if reply.TaskInfo, ok = <-c.Tasks; !ok {
			fmt.Println("chan empty")
			goto end
		}
		switch reply.TaskInfo.Status {
		case models.READY:
			fmt.Println("ready...")
			reply.TaskInfo.StartTime = time.Now().Unix()
			reply.TaskInfo.Status = models.DOING
			reply.TaskInfo.Worker = args.TaskInfo.Worker
			goto end
		case models.DOING:
			fmt.Println("doing...")
			if time.Now().Unix()-reply.TaskInfo.StartTime > 10 {
				reply.TaskInfo.Status = models.READY
				c.Tasks <- reply.TaskInfo
				c.DeadWorker[reply.TaskInfo.Worker] = true
			}
		case models.DONE:
			fmt.Println("done...")
			if reply.TaskInfo.Type == models.MAP {
				c.M--
				fmt.Printf("c.M = %v\n", c.M)
				if c.M == 0 {
					for i := 0; i < c.R; i++ {
						fmt.Printf("i = %v\n", i)
						c.Tasks <- models.Task{
							XY:     i,
							M:      reply.TaskInfo.M,
							R:      reply.TaskInfo.R,
							Type:   models.REDUCE,
							Status: models.READY,
						}
					}
				}
			} else if reply.TaskInfo.Type == models.REDUCE {
				c.R--
				if c.R == 0 {
					c.Tasks <- models.Task{
						Type:   models.END,
						Status: models.READY,
					}
					// close(c.Tasks)
					goto end
				}
			}
		default:
			log.Fatalf("unknown task type")
		}
	}

end:
	return nil
}

func (c *Coordinator) SubmitTask(args *Args, reply *Reply) error {
	fmt.Println("submit...")
	if _, ok := c.DeadWorker[args.TaskInfo.Worker]; !ok {
		args.TaskInfo.Status = models.DONE
		c.Tasks <- args.TaskInfo
	} else {
		delete(c.DeadWorker, args.TaskInfo.Worker)
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	fmt.Println("server ...")
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	ret := false

	// Your code here.
	if c.M == 0 && c.R == 0 {
		close(c.Tasks)
		ret = true
	}
	return ret
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
	for i, file := range files {
		fmt.Println(i)
		c.Tasks <- models.Task{
			FileName: file,
			XY:       i,
			M:        c.M,
			R:        c.R,
			Type:     models.MAP,
			Status:   models.READY,
		}
	}

	c.server()
	return &c
}
