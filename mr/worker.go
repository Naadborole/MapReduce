package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"net/rpc"
	"os"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

var EXIT bool = false
var ID uuid.UUID
var fileList []string
var bucket map[int][]KeyValue
var NReduce int

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func writeToIntermediateFiles(kva []KeyValue, NReduce int, taskNum int) {
	sortIntoBucket(kva, NReduce)
	for i, kvlist := range bucket {
		fileName := fmt.Sprintf("mr-%v-%v", taskNum, i)
		log.Tracef("The filename is : %v", fileName)
		fileList = append(fileList, fileName)
		file, err := os.Create(fileName)
		if err != nil {
			log.Panic("Creation of file failed")
		}
		writer := bufio.NewWriter(file)
		for _, kv := range kvlist {
			_, err = writer.WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value))
			if err != nil {
				log.Panic("Failed to write to file")
			}
		}
		file.Close()
	}
}

func sortIntoBucket(kva []KeyValue, NReduce int) {
	for _, kv := range kva {
		bn := ihash(kv.Key) % NReduce
		bucket[bn] = append(bucket[bn], kv)
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.SetLevel(log.TraceLevel)
	bucket = make(map[int][]KeyValue)
	ID = getInfo()
	log.Infof("Worker initialised with ID: %v\n", ID)
	NReduce = getNReduce()
	for !EXIT {
		var task Task = getTask()
		if task.Name == "map" {
			log.Infof("Executing map task with %v file\n", task.InputFile)
			content, err := os.ReadFile(task.InputFile)
			if err != nil {
				log.Fatal(err)
			}
			kva := mapf(task.InputFile, string(content))
			writeToIntermediateFiles(kva, NReduce, task.Num)
			done()
		} else if task.Name == "reduce" {
			log.Infof("Executing reduce task with %v file\n", task.InputFile)
		} else if task.Name == "exit" {
			EXIT = true
		} else {
			log.Errorf("Invalid task name : %v\n", task.Name)
			return
		}
	}

}

func done() {
	taskrep := TaskReport{ID, fileList}
	ok := call("Coordinator.FinishMapTask", &taskrep, &Empty{})
	if !ok {
		log.Panicf("call failed!\n")
	}
}

func getInfo() uuid.UUID {
	var info uuid.UUID
	ok := call("Coordinator.InitialiseWorkerInfo", &Empty{}, &info)
	if ok {
		log.Tracef("Worker ID received from is : %v\n", info.String())
	} else {
		log.Panicf("call failed!\n")
	}
	return info
}

func getNReduce() int {
	var nReduce int
	ok := call("Coordinator.GetNReduce", &Empty{}, &nReduce)
	if !ok {
		log.Error("Cannot get NReduce")
	}
	return nReduce
}

func getTask() Task {
	log.Info("Sending request for Task")
	task := Task{}
	ok := call("Coordinator.AssignTask", &ID, &task)
	if ok {
		log.Tracef("Worker received task : %v on %v\n", task.Name, task.InputFile)
	} else {
		log.Panicf("call failed!\n")
	}
	return task
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
