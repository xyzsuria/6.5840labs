package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sort"
	"strconv"
	"time"
	"os"
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

type ByKey []KeyValue

func (a ByKey) Len() int {return len(a)}
func (a ByKey) Swap(i,j int) {a[i],a[j]=a[j],a[i]}
func (a ByKey) Less(i,j int)bool {return a[i].Key<a[j].Key}
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
//
func Worker(mapf func(string, string) []KeyValue,
		reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		taskInfo := CallAskTask()
		switch taskInfo.State{
		case TaskMap:
			workerMap(mapf,taskInfo)
			break
		case TaskReduce:
			workerReduce(reducef,taskInfo)
			break
		case TaskWait:
			// 等待 5 秒再次请求
			time.Sleep(time.Duration(time.Second*5))
			break
		case TaskEnd:
			fmt.Println("Master all tasks complete. Nothing to do...")
			return
		default:
			panic("无效输入")
		}
	}

}


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

func CallAskTask() *TaskInfo{
	args := ExampleArgs{}
	reply := TaskInfo{}
	call("Coordinator.AskTask",&args,&reply)
	return &reply
}

func CallTaskDone(taskInfo *TaskInfo){
	reply := ExampleReply{}
	call("Coordinator.TaskDone",taskInfo,&reply)
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

func workerMap(mapf func(string,string)[]KeyValue, taskInfo *TaskInfo){
	fmt.Printf("Got assigned map task on %vth file %v\n",taskInfo.FileIndex,taskInfo.FileName)
	intermediate := []KeyValue{}
	file,err := os.Open(taskInfo.FileName)
	if err != nil{
		log.Fatalf("cannot open %v",taskInfo.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil{
		log.Fatalf("cannot read %v", taskInfo.FileName)
	}
	file.Close()
	kva := mapf(taskInfo.FileName,string(content))
	intermediate = append(intermediate,kva...)

	nReduce := taskInfo.NReduce
	outprefix := "mr-tmp/mr-"
	outprefix += strconv.Itoa(taskInfo.FileIndex)
	outprefix += "-"
	outFiles := make([]*os.File,nReduce)
	fileEncs := make([]*json.Encoder,nReduce)
	for outindex := 0;outindex<nReduce;outindex++{
		outFiles[outindex],_ = ioutil.TempFile("mr-tmp","mr-tmp-*")
		fileEncs[outindex] = json.NewEncoder(outFiles[outindex])
	}
	for _,kv := range intermediate{
		outindex := ihash(kv.Key)%nReduce
		file = outFiles[outindex]
		enc := fileEncs[outindex]
		err := enc.Encode(&kv)
		if err != nil{
			fmt.Printf("File %v Key %v Value %v Error:%v\n",taskInfo.FileName,kv.Key,kv.Value,err)
			panic("Json encode failed")
		}
	}
	// 替换文件名，并关闭文件
	// 文件名为 mr-fileIndex-outindex
	for outindex, file := range outFiles{
		outname := outprefix+strconv.Itoa(outindex)
		oldpath := filepath.Join(file.Name())
		os.Rename(oldpath,outname)
		file.Close()
	}
	CallTaskDone(taskInfo)
}

func workerReduce(reducef func(string,[]string)string, taskInfo *TaskInfo){
	fmt.Printf("Got assigned reduce task on part %v\n", taskInfo.PartIndex)
	outname := "mr-out-"+strconv.Itoa(taskInfo.PartIndex)
	innameprefix := "mr-tmp/mr-"
	innamesuffix := "-"+strconv.Itoa(taskInfo.PartIndex)
	// 读取全部的 files
	intermediate := []KeyValue{}
	for index:=0;index<taskInfo.NFiles;index++{
		inname := innameprefix+strconv.Itoa(index)+innamesuffix
		file,err := os.Open(inname)
		if err != nil{
			fmt.Printf("Open intermediate file %v failed:%v \n", inname,err)
			panic("Open file error")
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv);err != nil{
				break
			}
			intermediate = append(intermediate,kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	ofile, err := ioutil.TempFile("mr-tmp","mr-*")
	if err != nil{
		fmt.Printf("Create output file %v failed %v\n",outname,err)
		panic("Create file error")
	}
	i := 0
	for i < len(intermediate){
		j := i+1
		for j<len(intermediate)&&intermediate[j].Key == intermediate[i].Key{
			j ++
		}
		values := []string{}
		for k:=i;k<j;k++{
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key,output)
		i = j
	}
	os.Rename(filepath.Join(ofile.Name()),outname)
	ofile.Close()
	CallTaskDone(taskInfo)
}