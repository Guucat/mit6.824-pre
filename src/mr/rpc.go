package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type RequestArgs struct {
	//请求的任务类型 1:map 2:reduce
	TaskType int
	//任务id -1:任务请求 其它:任务回复
	TaskId int
	//机器序号 -1:新机器,需要协调器分配id
	WorkerId int
	//储存分区后的中间文件名
	ReduceFiles map[int]string
}

type ReplyArgs struct {
	TaskId   int
	WorkerId int
	//reduce任务数
	NReduce int
	//任务文件名
	Files []string
	//任务完成标识 true:任务完成,worker结束循环RPC请求 false:任务未完成
	Ok bool
	//true:需要等待所有worker完成当前类型的任务 false:不需要等待,已全部完成
	Wait bool
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
