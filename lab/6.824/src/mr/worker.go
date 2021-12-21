package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type WorkerProcess struct {
	PID int
	arg *TaskArgs
}

const SleepDuration = time.Second
const ExitingDuration = 5

// use ihash(key) % NReduce to choose the reduce. Task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	worker := &WorkerProcess{
		PID: os.Getpid(),
		arg: &TaskArgs{
			ID:    0,
			State: 3,
			PID:   os.Getpid(),
		},
	}

	for true {
		// Ask for a task
		taskReply := askForJob(worker.arg)

		// Identify if is a map or reduce job, call the right func or go to sleep
		if taskReply.Type == MapType {
			log.Printf("Get a map job: %v\n", taskReply)
			worker.doMap(taskReply, mapf)
		} else if taskReply.Type == ReduceType {
			log.Printf("Get a reduce job: %v\n", taskReply)
			worker.doReduce(taskReply, reducef)
		} else if taskReply.Type == SleepType{
			log.Printf("Currently nothing to do, sleep a second: %v\n", taskReply)
			time.Sleep(SleepDuration)
		} else {
			break
		}
	}

	for i := 0; i < ExitingDuration; i++ {
		log.Printf("Finish all job, exiting %v s later ...\n", ExitingDuration-i)
		time.Sleep(SleepDuration)
	}

	log.Printf("Done!\n")
}

func writeMapToFile(filename string, payload []KeyValue) {
	file, err := os.Create(filename)
	if err != nil {
		panic("111")
	}

	enc := json.NewEncoder(file)
	for _, kv := range payload {
		err := enc.Encode(&kv)
		if err != nil {
			panic("111")
		}
	}
}

func (w *WorkerProcess) doMap(task *TaskReply, mapf func(string, string) []KeyValue) {
	intermediate := make(map[int][]KeyValue)
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
	}

	// here got the content of the file, loading it into memory
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}

	if err = file.Close(); err != nil {
		log.Fatalf("cannot close %v", task.FileName)
	}

	// apply the map function and get the kv  list
	kva := mapf(task.FileName, string(content))

	// Comparing to serialized worker, kv set should be store in different buckets
	// that are divided by hash key
	for _, kv := range kva {
		intermediate[ihash(kv.Key)%task.NReduce] = append(
			intermediate[ihash(kv.Key)%task.NReduce], kv,
		)
	}

	for i := 0; i < task.NReduce; i++ {
		/*
			Here actually not need to sort, because reduce workers should read these output from multiple file,
			which means it would be disordered later in reduce process
		*/
		// sort it first
		//sort.Sort(ByKey(intermediate[i]))

		// write to file
		writeMapToFile(TempMapOutFileName(task.ID, i, w.PID), intermediate[i])
	}

	w.arg.State = MapState
	w.arg.ID = task.ID
	fmt.Println("Finish the map job", task.ID)

}

func (w *WorkerProcess) doReduce(task *TaskReply, reducef func(string, []string) string) {
	var intermediate []KeyValue
	// read the map file set, load it to memory
	for i := 0; i < task.NMap; i++ {
		filename := MapOutFileName(i, task.ID)

		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		// load the kv set into memory
		dec := json.NewDecoder(file)
		var kv KeyValue
		for {
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	// sort the intermediate
	sort.Sort(ByKey(intermediate))

	outputFileName := TempReduceOutFileName(task.ID, w.PID)
	ofile, _ := os.Create(outputFileName)

	// call Reduce on each distinct key in intermediate[],
	// and print the result to reduce out file
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// apply reducef on them
		output := reducef(intermediate[i].Key, values)

		// write it to a file
		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			log.Fatalf("err occur during writing to file %v", outputFileName)
		}

		i = j
	}

	ofile.Close()

	w.arg.State = ReduceState
	w.arg.ID = task.ID
	fmt.Println("Finish the reduce job", task.ID)
}

func askForJob(arg *TaskArgs) *TaskReply {
	// declare a reply structure.
	reply := &TaskReply{}
	// send the RPC request, wait for the reply.
	call("Coordinator.AssignTask", arg, reply)
	return reply
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

// Tip:
//mr-X-Y, where X is the Map task number, and Y is the reduce task number.

func MapOutFileName(mapID int, reduceID int) string {
	return fmt.Sprintf("mr-%d-%d", mapID, reduceID)
}

func TempMapOutFileName(mapID int, reduceID int, pid int) string {
	return fmt.Sprintf("map-%d-%d-%d", mapID, reduceID, pid)
}

func TempReduceOutFileName(reduceID int, workerID int) string {
	return fmt.Sprintf("out-%d-%d", reduceID, workerID)
}

func ReduceOutFileName(reduceID int) string {
	return fmt.Sprintf("mr-out-%d", reduceID)
}
