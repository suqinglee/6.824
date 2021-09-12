package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"

	"6.824/models"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doMap(task *models.Task, mapf func(string, string) []KeyValue) (err error) {
	intermediates := make([]*os.File, task.R)
	encs := make([]*json.Encoder, task.R)
	for y := 0; y < task.R; y++ {
		intermediates[y], err = ioutil.TempFile("./", fmt.Sprintf("tmp_mr-%v-%v", task.XY, y))
		if err != nil {
			log.Fatalf("create tempfile fail.")
		}
		encs[y] = json.NewEncoder(intermediates[y])
	}

	for _, filename := range task.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("doMap %v", err)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("doMap %v", err)
		}
		file.Close()

		kva := mapf(filename, string(content))

		for _, kv := range kva {
			y := ihash(kv.Key) % task.R
			if encs[y].Encode(&kv) != nil {
				log.Fatalf("doMap %v", err)
			}
		}
	}

	for y := 0; y < task.R; y++ {
		os.Rename(intermediates[y].Name(), fmt.Sprintf("mr-%v-%v", task.XY, y))
		intermediates[y].Close()
	}
	return nil
}

func doReduce(task *models.Task, reducef func(string, []string) string) (err error) {
	kva := []KeyValue{}
	for _, filename := range task.Files {
		intermediate, err := os.Open(filename)
		if err != nil {
			log.Fatalf("doReduce %v", err)
		}
		dec := json.NewDecoder(intermediate)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		intermediate.Close()
	}

	sort.Sort(ByKey(kva))

	file, _ := ioutil.TempFile("./", fmt.Sprintf("tmp_mr-out-%v", task.XY))
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)

		i = j
	}
	os.Rename(file.Name(), fmt.Sprintf("mr-out-%v", task.XY))
	file.Close()

	return nil
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for {
		task := AskTask()
		switch task.Type {
		case models.MAP:
			fmt.Printf("get map task %v\n", task.XY)
			doMap(&task, mapf)
		case models.REDUCE:
			fmt.Printf("get reduce task %v\n", task.XY)
			doReduce(&task, reducef)
		case models.END:
			return
		default:
			log.Fatalf("unknown task type")
		}
		SubmitTask(task)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func AskTask() models.Task {

	// declare an argument structure.
	args := Args{
		models.Task{
			Worker: os.Getpid(),
		},
	}

	// send the RPC request, wait for the reply.
	reply := Reply{}
	call("Coordinator.AskTask", &args, &reply)
	return reply.Task
}

func SubmitTask(task models.Task) {
	call("Coordinator.SubmitTask", &Args{task}, nil)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
