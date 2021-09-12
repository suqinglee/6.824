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
	mutex  sync.Mutex
	M      int
	R      int
	Tasks  chan models.Task
	Status map[string]int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) AskTask(args *Args, reply *Reply) error {
	fmt.Println("ask task lock")
	c.mutex.Lock()
	defer c.mutex.Unlock()
	fmt.Println("ask task get lock")
	for {
		var ok bool
		select {
		case reply.TaskInfo, ok = <-c.Tasks:
			if !ok {
				reply.TaskInfo.Type = models.END
				goto end
			}
		default:
			fmt.Println("chan empty")
			goto end
		}
		if reply.TaskInfo, ok = <-c.Tasks; !ok {
			fmt.Printf("Worker %v quit\n", args.TaskInfo.Worker)
			reply.TaskInfo.Type = models.END
			goto end
		}
		switch reply.TaskInfo.Status {
		case models.READY:
			fmt.Printf("ready... type=%v xy=%v pid=%v\n", reply.TaskInfo.Type, reply.TaskInfo.XY, args.TaskInfo.Worker)
			reply.TaskInfo.StartTime = time.Now().Unix()
			reply.TaskInfo.Status = models.DOING
			reply.TaskInfo.Worker = args.TaskInfo.Worker
			c.Status[reply.TaskInfo.Unique()] = models.INPROGRESS
			c.Tasks <- reply.TaskInfo
			goto end
		case models.DOING:
			fmt.Printf("doing... type=%v xy=%v pid=%v\n", reply.TaskInfo.Type, reply.TaskInfo.XY, reply.TaskInfo.Worker)
			if a, ok := c.Status[reply.TaskInfo.Unique()]; ok {
				fmt.Printf("exists %v\n", a)
			} else {
				fmt.Println("not exists")
			}
			if time.Now().Unix()-reply.TaskInfo.StartTime > 1000 && c.Status[reply.TaskInfo.Unique()] != models.FINISH {
				fmt.Println("timeout")
				c.Status[reply.TaskInfo.Unique()] = models.HANG
				reply.TaskInfo.Status = models.READY
				fmt.Println("redo")
				c.Tasks <- reply.TaskInfo
				fmt.Println("redo over")
			}
			fmt.Println("?????")
		case models.DONE:
			fmt.Printf("done... type=%v xy=%v pid=%v\n", reply.TaskInfo.Type, reply.TaskInfo.XY, reply.TaskInfo.Worker)
			c.Status[reply.TaskInfo.Unique()] = models.FINISH
			if reply.TaskInfo.Type == models.MAP {
				c.M--
				fmt.Printf("c.M = %v\n", c.M)
				if c.M == 0 {
					for i := 0; i < c.R; i++ {
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
				fmt.Printf("c.R = %v\n", c.R)
				if c.R == 0 {
					close(c.Tasks)
				}
			}
		default:
			log.Fatalf("unknown task type")
		}
	}

end:
	fmt.Println("ask task unlock")
	return nil
}

func (c *Coordinator) SubmitTask(args *Args, reply *Reply) error {
	fmt.Println("submit task lock")
	c.mutex.Lock()
	defer c.mutex.Unlock()
	fmt.Println("submit task get lock")
	fmt.Printf("submit... type=%v xy=%v pid=%v\n", args.TaskInfo.Type, args.TaskInfo.XY, args.TaskInfo.Worker)
	if c.Status[args.TaskInfo.Unique()] == models.INPROGRESS {
		c.Status[args.TaskInfo.Unique()] = models.FINISH
		args.TaskInfo.Status = models.DONE
		c.Tasks <- args.TaskInfo
	} else if c.Status[args.TaskInfo.Unique()] == models.HANG {
		delete(c.Status, args.TaskInfo.Unique())
	} else {
		log.Fatalf("unknown status")
	}
	fmt.Println("submit task unlock")
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
	fmt.Println("done lock")
	c.mutex.Lock()
	defer c.mutex.Unlock()
	fmt.Println("done get lock")
	ret := false

	// Your code here.
	if c.M == 0 && c.R == 0 {
		// close(c.Tasks)
		ret = true
	}
	fmt.Println("done unlock")
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		M:      len(files),
		R:      nReduce,
		Tasks:  make(chan models.Task, len(files)+nReduce+50),
		Status: make(map[string]int),
	}

	// Your code here.
	for i, file := range files {
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
