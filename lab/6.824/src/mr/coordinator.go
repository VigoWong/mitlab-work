package mr

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type CoState int

const (
	Map CoState = iota + 1
	Reduce
	End
)

type Coordinator struct {
	State     CoState // Current state of coordinator
	nReduce   int     // Number of reduce job
	nMap      int     // Number of map job
	TaskQueue chan *Task
	TaskMap   map[int]*Task // TaskID - PID
	Lock      sync.Mutex    // global lock
}

type Task struct {
	FileName  string
	TaskID    int
	TaskType  TaskType
	StartTime time.Time
	PID       int
}

// AssignTask the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	fmt.Println("Got a request from", args.PID)
	switch args.State {
	case InitState:
		c.processMap(args, reply)
	case MapState:
		c.processMap(args, reply)
	case ReduceState:
		c.processReduce(args, reply)
	}
	return nil
}

func (c *Coordinator) processMap(args *TaskArgs, reply *TaskReply) error {
	if args.State != InitState && c.TaskMap[args.ID] != nil && c.TaskMap[args.ID].PID == args.PID {
		// process the finish task
		// rename file to final output TODO: verify here
		for i := 0; i < c.nReduce; i++ {
			err := os.Rename(TempMapOutFileName(args.ID, i, args.PID), MapOutFileName(args.ID, i))
			if err != nil {
				return err
			}
		}

		// mark it finished
		fmt.Println("delete ID", args.ID, len(c.TaskMap))
		fmt.Println("queue length", len(c.TaskQueue))
		fmt.Println("map length", len(c.TaskMap))
		delete(c.TaskMap, args.ID)
	}

	// transit to next state？
	if args.State != InitState && len(c.TaskMap) == 0 {
		c.transit()
	}

	if len(c.TaskQueue) == 0 && c.State != End{
		fmt.Println("sleep a while")
		reply.Type = SleepType
		return nil
	}
	
	// assign the next task
	task := <-c.TaskQueue
	reply.Type = task.TaskType
	reply.ID = task.TaskID
	reply.FileName = task.FileName
	reply.NReduce = c.nReduce
	reply.NMap = c.nMap

	// mark it assigned to the worker
	task.StartTime = time.Now()
	task.PID = args.PID
	c.TaskMap[task.TaskID] = task
	return nil
}

func (c *Coordinator) processReduce(args *TaskArgs, reply *TaskReply) error {
	if c.TaskMap[args.ID] != nil && c.TaskMap[args.ID].PID == args.PID {
		// process the finish task
		// rename file to final output TODO: verify here
		err := os.Rename(TempReduceOutFileName(args.ID, args.PID), ReduceOutFileName(args.ID))
		if err != nil {
			return err
		}
		for i := 0; i < c.nMap; i++ {
			os.Remove(MapOutFileName(i, args.ID))
		}

		// mark it finished
		fmt.Println("delete ID", args.ID)
		delete(c.TaskMap, args.ID)
	}

	// transit to next state？
	if len(c.TaskMap) == 0 {
		c.transit()
	}

	if len(c.TaskQueue) == 0 && c.State != End{
		fmt.Println("sleep a while")
		reply.Type = SleepType
		return nil
	}

	// assign the next task
	task := <-c.TaskQueue
	reply.Type = task.TaskType
	reply.ID = task.TaskID
	reply.FileName = task.FileName
	reply.NReduce = c.nReduce
	reply.NMap = c.nMap

	// mark it assigned to the worker
	task.StartTime = time.Now()
	task.PID = args.PID
	c.TaskMap[task.TaskID] = task
	return nil
}

func (c *Coordinator) transit() {
	switch c.State {
	case Map:
		fmt.Println("transit to reduce")
		c.State = Reduce
		for i := 0; i < c.nReduce; i++ {
			c.TaskQueue <- &Task{
				TaskID:   i,
				TaskType: ReduceType,
			}
			c.TaskMap[i] = nil
		}
	case Reduce:
		fmt.Println("transit to end")
		c.State = End
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.State == End
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		State:     Map,
		nReduce:   nReduce,
		nMap:      len(files),
		TaskQueue: make(chan *Task, int(math.Max(float64(nReduce), float64(len(files))))+1),
		TaskMap:   make(map[int]*Task),
		Lock:      sync.Mutex{},
	}

	// 添加任务
	for i, file := range files {
		c.TaskQueue <- &Task{
			FileName: file,
			TaskID:   i,
			TaskType: MapType,
		}
		c.TaskMap[i] = nil
	}
	fmt.Println("queue length", len(c.TaskQueue))

	// TODO: 回收任务
	go func() {
		for {
			c.recycle()
			time.Sleep(time.Second * 2)
		}
	}()

	c.server()
	return &c
}

func (c *Coordinator) recycle() {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	for i, task := range c.TaskMap {
		if task != nil {
			if time.Now().After(task.StartTime.Add(time.Second * 10)) {
				// mark it
				c.TaskMap[i] = nil
				c.TaskQueue <- task
			}
		}
	}

}
