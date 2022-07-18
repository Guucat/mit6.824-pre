package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.

	//map任务列表 例如:任务1:文件名
	mapFile map[int]string
	//map任务列表及状态 例如:任务1 (1: 未开始,  2: 执行中,	 3: 完成)
	mapState map[int]int
	//reduce任务分区, 例如:分区1包含的files
	reduceFile map[int][]string
	//reduce的分区任务状态, 例如:分区1  (1: 未开始,  2: 执行中,	 3: 完成)
	reduceState map[int]int
	//work序列号, 初始为-1,每次机器的id为-1则为新worker请求,当前workId为此worker的id,workerId+1,否则判断worker状态...
	workerId int
	//workers状态: 正常1   故障3
	workerState map[int]int
	//reduce 分区数
	nReduce int
	//锁
	mutex sync.Mutex

	//workers任务类型: 例如: 机器1执行的map任务   1:map  2:reduce
	//workerType map[int]int
	//worker执行的任务内容 例如: 机器1:任务1
	//workerContent map[int]int
	//errorMap    map[int]int
	//errorReduce map[int]int
	t1 time.Time
}

//is RPC handlers concurrent when the worker calling ? yes
// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Task(request *RequestArgs, reply *ReplyArgs) error {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	//rpc为任务请求
	if request.TaskId == -1 {
		//为新worker分配id, 状态
		reply.WorkerId = c.workerId
		c.workerState[c.workerId] = 1
		c.workerId++

		//请求 map 任务
		if request.TaskType == 1 {
			hasProcess := false
			for i, state := range c.mapState {
				if state == 2 {
					hasProcess = true
				}
				if state == 1 {
					//存在空闲map任务
					fmt.Println("存在空闲map任务 ID:", i, c.mapFile[i])
					c.mapState[i] = 2
					reply.TaskId = i
					reply.NReduce = c.nReduce
					reply.Files = append(reply.Files, c.mapFile[i])
					reply.Ok = false
					reply.Wait = false
					go c.timedTask(request.TaskType, i, request.WorkerId)
					return nil
				}
			}
			//map未全部完成, 让空闲worker等待一下
			if hasProcess {
				reply.Ok = false
				reply.Wait = true
				return nil
			}
			//map任务全部完成
			reply.Ok = true
			return nil

			//请求 reduce 任务
		} else {
			hasPro := false
			for i, state := range c.reduceState {
				if state == 2 {
					hasPro = true
				}
				//存在空闲reduce任务
				if state == 1 {
					c.reduceState[i] = 2
					reply.TaskId = i
					reply.NReduce = c.nReduce
					reply.Files = c.reduceFile[i]
					reply.Ok = false
					reply.Wait = false
					go c.timedTask(request.TaskType, i, request.WorkerId)
					return nil
				}
			}
			//存在处理中的任务
			if hasPro {
				reply.Ok = false
				reply.Wait = true
				return nil
			}
			//map任务全部完成
			reply.Ok = true
			return nil
		}

		//PRC为任务回复
	} else {
		//排除故障worker任务回复
		if c.workerState[request.WorkerId] == 3 {
			//reply.Out = true
			return nil
		}
		//map任务回复
		if request.TaskType == 1 {
			c.mapState[request.TaskId] = 3
			for i, file := range request.ReduceFiles {
				c.reduceFile[i] = append(c.reduceFile[i], file)
			}
			return nil

			//回复为reduce任务
		} else {
			c.reduceState[request.TaskId] = 3
			return nil
		}
	}

}

//定时任务
//10s后如果任务未完成 State != 3, 则将此任务状态置1(未开始状态), 将此机器状态置3(故障状态)
func (c *Coordinator) timedTask(taskType int, taskId int, workerId int) {
	fmt.Println("开始定时,任务ID:", taskId)
	time.Sleep(10 * time.Second)
	//t1 := time.Tick(10 * time.Second)
	//select {
	//case <-t1:
	fmt.Println("时间到, 获取锁...")
	c.mutex.Lock()
	fmt.Println("得到锁")
	defer c.mutex.Unlock()
	if taskType == 1 {
		if c.mapState[taskId] != 3 {
			c.mapState[taskId] = 1
			c.workerState[workerId] = 3
			fmt.Println("map任务重新调度, taskId:", taskId)
		}
	} else {
		if c.reduceState[taskId] != 3 {
			c.reduceState[taskId] = 1
			c.workerState[workerId] = 3
			fmt.Println("reduce任务重新调度, taskId", taskId)
		}
	}
	//}

}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	ret := false
	//reduce中存在未完成的任务,即表示Job未完成
	for _, state := range c.reduceState {
		if state != 3 {
			return false
		}
	}
	ret = true
	fmt.Println("Job done, Coordinator Exit")
	fmt.Println(time.Now().Sub(c.t1))
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{
		mapFile:     make(map[int]string),
		mapState:    make(map[int]int),
		reduceFile:  make(map[int][]string),
		reduceState: make(map[int]int),
		workerState: make(map[int]int),
		t1:          time.Now(),
	}
	c.workerId = 0
	c.nReduce = nReduce
	for i, file := range files {
		c.mapFile[i] = file
		c.mapState[i] = 1
	}
	for i := 0; i < nReduce; i++ {
		c.reduceState[i] = 1
	}
	c.server()
	return &c
}
