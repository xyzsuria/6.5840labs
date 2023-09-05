package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "fmt"


type TaskStat struct{
	beginTime time.Time
	fileName string 
	fileIndex int 
	partIndex int 
	nReduce int 
	nFiles int 
}

// 定义方法的接口
type TaskStatInterface interface{
	GenerateTaskInfo() TaskInfo
	OutOfTime() bool
	GetFileIndex() int 
	GetPartIndex() int
	SetNow()
}

type MapTaskStat struct{
	TaskStat
}

type ReduceTaskStat struct{
	TaskStat
}

func (this *MapTaskStat) GenerateTaskInfo() TaskInfo{
	return TaskInfo{
		State: TaskMap,
		FileName: this.fileName,
		FileIndex: this.fileIndex,
		PartIndex:this.partIndex,
		NReduce: this.nReduce,
		NFiles:this.nFiles,
	}
}

func (this *ReduceTaskStat) GenerateTaskInfo() TaskInfo{
	return TaskInfo{
		State: TaskReduce,
		FileName: this.fileName,
		FileIndex: this.fileIndex,
		PartIndex:this.partIndex,
		NReduce: this.nReduce,
		NFiles:this.nFiles,
	}	
}

func (this *TaskStat) OutOfTime()bool{
	return time.Now().Sub(this.beginTime)>time.Duration(time.Second*60)
}

func (this *TaskStat) SetNow(){
	this.beginTime = time.Now()
}

func (this *TaskStat) GetFileIndex()int{
	return this.fileIndex
}

func (this *TaskStat) GetPartIndex()int{
	return this.partIndex
}

// 把任务方法接口组织成序列，任务序列对应的方法
type TaskStatQueue struct{
	taskArray []TaskStatInterface
	mutex sync.Mutex
}

func (this *TaskStatQueue) lock(){
	this.mutex.Lock()
}

func (this *TaskStatQueue) unlock(){
	this.mutex.Unlock()
}

func (this *TaskStatQueue) Size()int{
	return len(this.taskArray)
}

// 取出任务要加锁和解锁
func (this *TaskStatQueue) Pop() TaskStatInterface{
	this.lock()
	arrayLength := len(this.taskArray)
	if arrayLength == 0{
		this.unlock()
		return nil
	}
	ret := this.taskArray[arrayLength-1]
	this.taskArray = this.taskArray[:arrayLength-1]
	this.unlock()
	return ret
}

// 添加任务
func (this * TaskStatQueue)Push(taskStat TaskStatInterface){
	this.lock()
	// 判断参数是否为空
	if taskStat == nil{
		this.unlock()
		return
	}
	this.taskArray = append(this.taskArray, taskStat)
	this.unlock()
}

// 获得超时的任务队列
func (this *TaskStatQueue) TimeOutQueue() []TaskStatInterface{
	outArray := make([]TaskStatInterface,0)
	this.lock()
	for taskIndex :=0;taskIndex<len(this.taskArray);{
		t := this.taskArray[taskIndex]
		if t.OutOfTime(){
			outArray = append(outArray,t)
			this.taskArray = append(this.taskArray[:taskIndex],this.taskArray[taskIndex+1:]...)
		}else{
			taskIndex ++
		}
	}
	this.unlock()
	return outArray
}
// 拼接列表
func (this *TaskStatQueue) MoveAppend(rhs []TaskStatInterface){
	this.lock()
	this.taskArray = append(this.taskArray,rhs...)
	rhs = make([]TaskStatInterface,0)
	this.unlock()
}
// 移除 taskArray 中的 task
func (this *TaskStatQueue) RemoveTask(fileIndex int, partIndex int){
	this.lock()
	for index:=0;index<len(this.taskArray);{
		t := this.taskArray[index]
		if t.GetFileIndex() == fileIndex && t.GetPartIndex() == partIndex{
			this.taskArray = append(this.taskArray[:index],this.taskArray[index+1:]...)
		}else{
			index ++
		}
	}
	this.unlock()
}



// 分配给请求的 worker，如果某个 work 没有完成，则分配给别的worker
type Coordinator struct {
	// Your definitions here.
	filenames []string
	// reduce 的任务队列
	reduceTaskWaiting TaskStatQueue
	reduceTaskRunning TaskStatQueue
	
	// map 的任务队列
	mapTaskWaiting TaskStatQueue
	mapTaskRunning TaskStatQueue

	// 任务是否结束
	isDone bool
	nReduce int
}

func (this *Coordinator) AskTask(args *ExampleArgs, reply *TaskInfo) error{
	if this.isDone{
		reply.State = TaskEnd
		return nil
	}
	// 检查 Waiting 队列中的 reduce 任务
	reduceTask := this.reduceTaskWaiting.Pop()
	if reduceTask != nil{
		// 有可以执行的 reduce
		reduceTask.SetNow()
		// 移动到 running 队列
		this.reduceTaskRunning.Push(reduceTask)
		// 设置 reply
		*reply = reduceTask.GenerateTaskInfo()
		fmt.Printf("Distributing reduce task on part %v %vth file %v\n",reply.PartIndex,reply.FileIndex,reply.FileName)
		return nil
	}
	// 检查 map waiting 队列
	mapTask := this.mapTaskWaiting.Pop()
	if mapTask != nil{
		mapTask.SetNow()
		this.mapTaskRunning.Push(mapTask)
		*reply = mapTask.GenerateTaskInfo()
		fmt.Printf("Distributing reduce task on part %v %vth file %v\n",reply.PartIndex,reply.FileIndex,reply.FileName)
		return nil		
	}
	// 所有的任务都被分配了
	if this.mapTaskRunning.Size()>0 || this.reduceTaskRunning.Size()>0{
		reply.State = TaskWait
		return nil
	}
	// 所有的任务都完成了
	reply.State = TaskEnd
	this.isDone = true
	return nil
}
// 分配 nReduce 个 reduce任务
func (this *Coordinator)distributeReduce(){
	reduceTask := ReduceTaskStat{
		TaskStat{
			fileIndex:0,
			partIndex:0,
			nReduce:this.nReduce,
			nFiles:len(this.filenames),
		},
	}
	for reduceIndex:=0;reduceIndex<this.nReduce;reduceIndex++{
		task := reduceTask
		task.partIndex = reduceIndex
		this.reduceTaskWaiting.Push(&task)
	}
} 

func (this *Coordinator) TaskDone(args *TaskInfo, reply *ExampleReply)error{
	switch args.State{
	case TaskMap:
		fmt.Printf("Map task on %vth file %v complete\n",args.FileIndex,args.FileName)
		this.mapTaskRunning.RemoveTask(args.FileIndex,args.PartIndex)
		if this.mapTaskRunning.Size()==0&& this.mapTaskWaiting.Size()==0{
			// 所有的map任务都完成了，分配reduce 任务
			this.distributeReduce()
		}
		break
	case TaskReduce:
		fmt.Printf("Reduce task on %vth part complete\n",args.PartIndex)
		this.reduceTaskRunning.RemoveTask(args.FileIndex,args.PartIndex)
		break
	default:
		panic("Task Done error")
	}
	return nil
}

// Your code here -- RPC handlers for the worker to call.

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
func (this *Coordinator) Done() bool {
	return this.isDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// 分配 map 任务
	mapArray := make([]TaskStatInterface,0)
	// 一个是数据类型，一个是接口类型
	for fileIndex,filename := range files{
		mapTask:=MapTaskStat{
			TaskStat{
				fileName:filename,
				fileIndex:fileIndex,
				partIndex:0,
				nReduce:nReduce,
				nFiles:len(files),
			},
		}
		mapArray = append(mapArray,&mapTask)
	}
	m := Coordinator{
		mapTaskWaiting:TaskStatQueue{taskArray:mapArray},
		nReduce:nReduce,
		filenames:files,
	}
	// 创建中间文件夹
	if _,err := os.Stat("mr-tmp");os.IsNotExist(err){
		err = os.Mkdir("mr-tmp",os.ModePerm)
		if err != nil{
			fmt.Printf("Create tmp directory fail... Error:%v\n",err)
			panic("Create tmp directory failed...")
		}
	}
	// 开启一个收集超时任务的协程
	go m.collectOutOfTime()

	m.server()
	return &m
}

func (this *Coordinator) collectOutOfTime(){
	for {
		time.Sleep(time.Duration(time.Second*5))
		timeouts := this.reduceTaskRunning.TimeOutQueue()
		if len(timeouts)>0{
			this.reduceTaskWaiting.MoveAppend(timeouts)
		}
		timeouts = this.mapTaskRunning.TimeOutQueue()
		if len(timeouts)>0{
			this.mapTaskWaiting.MoveAppend(timeouts)
		}
	}
}