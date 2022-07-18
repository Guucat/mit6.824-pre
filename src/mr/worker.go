package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

//
// main/mrworker.go calls this function.
//The worker implementation should put the output of the X'th reduce task in the file mr-out-X.
//

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	//循环请求map
	for {
		request := RequestArgs{
			TaskType:    1,
			TaskId:      -1,
			WorkerId:    -1,
			ReduceFiles: make(map[int]string),
		}
		reply := ReplyArgs{}
		call("Coordinator.Task", &request, &reply)
		fmt.Println("完成map任务请求")
		if reply.Ok {
			fmt.Println("map任务完成，退出map循环RPC")
			break
		}
		if reply.Wait {
			fmt.Println("sleep 2s to waiting all maps done...")
			time.Sleep(2 * time.Second)
			continue
		}

		//处理得到中间键值对, 并分区储存在内存中
		//读取文件前的预处理
		headName := "mr-"
		taskIdName := strconv.Itoa(reply.TaskId) + "-"
		reduceFiles := map[int]string{}
		filePoint := map[int]*os.File{}
		mapKV := map[int][]KeyValue{}
		for i := 0; i < reply.NReduce; i++ {
			midFile := headName + taskIdName + strconv.Itoa(i)
			file, _ := os.OpenFile(midFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
			reduceFiles[i] = midFile
			filePoint[i] = file
			mapKV[i] = []KeyValue{}
		}

		for _, filename := range reply.Files {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("map cannot open %v", filename)
			}
			//将整个文本内容读进 content 字节数组
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			kva := mapf(filename, string(content))
			for _, kv := range kva {
				hash := ihash(kv.Key) % reply.NReduce
				mapKV[hash] = append(mapKV[hash], kv)
			}
			//intermediate = append(intermediate, kva...)
		}

		//将分区键值对持久化到文件
		fmt.Println("开始分区键值对持久化...")
		for i := 0; i < reply.NReduce; i++ {
			midFile := reduceFiles[i]
			file := filePoint[i]
			enc := json.NewEncoder(file)
			//将整个文件Json格式化
			err := enc.Encode(mapKV[i])
			if err != nil {
				log.Fatalf("Can not write into file names %v", midFile)
			}
		}
		//关闭文件符
		for _, file := range filePoint {
			file.Close()
		}

		//map任务回复
		request.TaskType = 1
		request.WorkerId = reply.WorkerId
		request.TaskId = reply.TaskId
		request.ReduceFiles = reduceFiles
		call("Coordinator.Task", &request, &reply)
		fmt.Println("完成map任务回复")
	}

	//循环请求reduce任务
	for {
		request := RequestArgs{
			TaskType: 2,
			TaskId:   -1,
			WorkerId: -1,
		}
		reply := ReplyArgs{}
		call("Coordinator.Task", &request, &reply)
		fmt.Println("reduce任务请求完成")
		if reply.Ok {
			fmt.Println("reduce完成,循环退出")
			break
		}
		if reply.Wait {
			fmt.Println("sleep 2s to wait all reduce worker done...")
			time.Sleep(2 * time.Second)
			continue
		}

		files := reply.Files
		intermediate := []KeyValue{}
		fmt.Println("开始读取键值对中间文件...")
		for _, filename := range files {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("reduce cannot open %v", filename)
			}
			dec := json.NewDecoder(file)
			//将整个文件Json解码
			inter := []KeyValue{}
			if err := dec.Decode(&inter); err != nil {
				break
			}
			intermediate = append(intermediate, inter...)
			file.Close()
		}

		fmt.Println("sorting")
		sort.Sort(ByKey(intermediate))
		oName := "mr-out-" + strconv.Itoa(reply.TaskId)
		ofile, _ := os.Create(oName)

		fmt.Println("outing")
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
			output := reducef(intermediate[i].Key, values)
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
			i = j
		}
		ofile.Close()

		request.TaskType = 2
		request.TaskId = reply.TaskId
		request.WorkerId = reply.WorkerId
		call("Coordinator.Task", &request, &reply)
		fmt.Println("一次reduce任务OK")
	}
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
