package mr

import (
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

var taskQueue chan Task
var waitMap sync.WaitGroup
var intermediateFileList []string
var iflMu sync.RWMutex
var DONE_EXECUTION bool = false

type Coordinator struct {
	// Your definitions here.
	WorkerInfo map[uuid.UUID]*WorkerInfo
	NReduce    int
}

// start a thread that listens for RPCs from worker.go
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
	return DONE_EXECUTION
}

func (c *Coordinator) InitialiseWorkerInfo(args Empty, info *uuid.UUID) error {
	wInfo := WorkerInfo{}
	wInfo.ID = uuid.New()
	wInfo.state = IDLE
	wInfo.done = make(chan bool)
	log.Tracef("Generating uuid: %v\n", wInfo.ID)
	c.WorkerInfo[wInfo.ID] = &wInfo
	*info = wInfo.ID
	return nil
}

func (c *Coordinator) waitForWorker(id uuid.UUID) {
	select {
	case <-c.WorkerInfo[id].done:
		waitMap.Done()
		return
	case <-time.After(10 * time.Second):
		c.WorkerInfo[id].state = FAILED
		log.Warnf("Worker %v failed!\n Reassigning task %v to new worker", id.String(), c.WorkerInfo[id].task)
		taskQueue <- c.WorkerInfo[id].task
	}
}

func (c *Coordinator) AssignTask(workerId uuid.UUID, task *Task) error {
	log.Infof("Request for task received from worker: %v\n", workerId)
	*task = <-taskQueue
	log.Infof("Sending task %v %v to worker %v\n", task.Name, task.InputFile, workerId)
	c.WorkerInfo[workerId].state = INPROGRESS
	c.WorkerInfo[workerId].task = *task
	log.Trace("Waiting for worker")
	if task.Name != "exit" {
		go c.waitForWorker(workerId)
	} else {
		taskQueue <- Task{"exit", "", -1}
	}
	return nil
}

func (c *Coordinator) FinishMapTask(report TaskReport, empty *Empty) error {
	log.Infof("Worker %v finished task num", report.ID)
	iflMu.Lock()
	intermediateFileList = append(intermediateFileList, report.FileList...)
	iflMu.Unlock()
	c.WorkerInfo[report.ID].state = IDLE
	c.WorkerInfo[report.ID].done <- true
	return nil
}

func (c *Coordinator) GetNReduce(empty Empty, nRed *int) error {
	*nRed = c.NReduce
	return nil
}

func (c *Coordinator) GetIntermediateFileList(empty Empty, list *[]string) error {
	*list = intermediateFileList
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.SetLevel(log.WarnLevel)
	taskQueue = make(chan Task, len(files))
	c := Coordinator{}
	c.NReduce = nReduce
	c.WorkerInfo = make(map[uuid.UUID]*WorkerInfo)
	go c.server()
	for ind, i := range files {
		log.Trace("Adding task to channel")
		waitMap.Add(1)
		taskQueue <- Task{"map", i, ind}
	}
	waitMap.Wait()
	log.Info("Finished map tasks\n")
	for i := 0; i < nReduce; i++ {
		taskQueue <- Task{"reduce", "", i}
		waitMap.Add(1)
	}
	waitMap.Wait()
	taskQueue <- Task{"exit", "", -1}
	DONE_EXECUTION = true
	// Your code here.
	return &c
}
