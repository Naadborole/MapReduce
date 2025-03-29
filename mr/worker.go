package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/rpc"
	"os"
	"sort"
	"strings"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

var EXIT bool = false
var ID uuid.UUID
var fileNameList []string
var NReduce int

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func writeJsonToFile(kva []KeyValue, fileName string) {
	jsonData, err := json.MarshalIndent(kva, "", "    ")
	if err != nil {
		panic(err)
	}
	err = os.WriteFile(fileName+".json", jsonData, 0644) // 0644 sets file permissions
	if err != nil {
		panic(err)
	}
}

func readJsonFromFile(fileName string) []KeyValue {
	fileData, err := os.ReadFile(fileName + ".json")
	if err != nil {
		fmt.Println(err)
		return []KeyValue{}
	}

	// Unmarshal JSON data back to struct
	var kva []KeyValue
	err = json.Unmarshal(fileData, &kva)
	if err != nil {
		fmt.Println(err)
		return []KeyValue{}
	}
	return kva
}

func writeToIntermediateFiles(kva []KeyValue, NReduce int, taskNum int, bucket map[int][]KeyValue) {
	sortIntoBucket(kva, NReduce, bucket)
	for i, kvlist := range bucket {
		fileName := fmt.Sprintf("mr-%v-%v", taskNum, i)
		log.Tracef("The filename is : %v", fileName)
		fileNameList = append(fileNameList, fileName)
		writeJsonToFile(kvlist, fileName)
	}
}

func sortIntoBucket(kva []KeyValue, NReduce int, bucket map[int][]KeyValue) {
	for _, kv := range kva {
		bn := ihash(kv.Key) % NReduce
		bucket[bn] = append(bucket[bn], kv)
	}
}

func getKV(line string) KeyValue {
	t := strings.Split(line, " ")
	if len(t) == 1 {
		return KeyValue{t[0], "0"}
	}
	return KeyValue{t[0], t[1]}
}

func addWords(fileName string, wordList map[string][]string) {
	kva := readJsonFromFile(fileName)
	for _, kv := range kva {
		wordList[kv.Key] = append(wordList[kv.Key], kv.Value)
	}
}
func writeOutput(fileName string, out []KeyValue) {
	sort.Sort(ByKey(out))
	file, err := os.Create(fileName)
	if err != nil {
		log.Panic("Creation of file failed")
	}
	for _, kv := range out {
		fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
	}
	file.Close()
}

func performReduce(reducef func(string, []string) string, taskNum int) {
	fileList := getFileList()
	wordList := make(map[string][]string)
	for _, fileName := range fileList {
		if strings.HasSuffix(fileName, fmt.Sprintf("-%v", taskNum)) {
			addWords(fileName, wordList)
		}
	}
	out := []KeyValue{}
	for k, v := range wordList {
		out = append(out, KeyValue{k, reducef(k, v)})
	}
	writeOutput(fmt.Sprintf("mr-out-%v", taskNum), out)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.SetLevel(log.WarnLevel)
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
			var bucket map[int][]KeyValue = make(map[int][]KeyValue)
			writeToIntermediateFiles(kva, NReduce, task.Num, bucket)
			done(fileNameList)
			fileNameList = []string{}
		} else if task.Name == "reduce" {
			log.Infof("Executing reduce task %v", task.Num)
			performReduce(reducef, task.Num)
			done([]string{})
		} else if task.Name == "exit" {
			EXIT = true
		} else {
			log.Errorf("Invalid task name : %v\n", task.Name)
			return
		}
	}

}

func getFileList() []string {
	var fileList []string
	ok := call("Coordinator.GetIntermediateFileList", &Empty{}, &fileList)
	if !ok {
		log.Panicf("call failed!\n")
	}
	return fileList
}

func done(fileList []string) {
	taskrep := TaskReport{ID, fileList}
	log.Tracef("Calling Done with file list: %v", fileList)
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
