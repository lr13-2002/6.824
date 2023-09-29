package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	filelog, err := os.OpenFile("worklogfile"+strconv.Itoa(os.Getegid()), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	log.SetOutput(filelog)
	// Your worker implementation here.
	for {
		arg, err := Call_task("GET", Job{})

		if err != nil {
			panic(err)
		}

		if arg.Flag == "MAP" {
			log.Println(arg)
			err = MapWork(mapf, arg)
			if err != nil {
				panic(err)
			}

			_, err = Call_task("PUT", arg.Job)
			if err != nil {
				panic(err)
			}

		} else if arg.Flag == "REDUCE" {
			log.Println(arg)
			err = ReduceWork(reducef, arg)
			if err != nil {
				panic(err)
			}

			_, err = Call_task("PUT", arg.Job)
			if err != nil {
				panic(err)
			}
		} else if arg.Flag == "NONE" {
			log.Println(arg)
			time.Sleep(time.Second)
		} else {
			log.Println(arg)
			break
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func MapWork(mapf func(string, string) []KeyValue, args Rep) error {
	file, err := os.Open(args.FileName)
	if err != nil {
		log.Println(err, args.FileName)
		return err
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	file.Close()

	kv := mapf(args.FileName, string(content))

	kvs := make([][]KeyValue, args.NReduce)
	for _, kv_ := range kv {
		kvs[ihash(kv_.Key)%args.NReduce] = append(kvs[ihash(kv_.Key)%args.NReduce], kv_)
	}

	for i := 0; i < args.NReduce; i++ {
		kv_json, err := json.Marshal(kvs[i])
		if err != nil {
			return err
		}
		file, err = os.Create("mpr-tmp-" + strconv.Itoa(i) + "-" + strconv.Itoa(args.TaskId))
		if err != nil {
			return err
		}

		_, err = file.Write(kv_json)
		if err != nil {
			return err
		}

		file.Close()
	}

	return nil
}

func ReduceWork(reducef func(string, []string) string, args Rep) error {

	kv := make([]KeyValue, 0)
	for i := 0; i < args.MaxId; i++ {
		file, err := os.Open(args.FileName + strconv.Itoa(i))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}

		content, err := ioutil.ReadAll(file)
		if err != nil {
			return err
		}

		var kv_ []KeyValue
		err = json.Unmarshal(content, &kv_)
		if err != nil {
			return err
		}

		kv = append(kv, kv_...)
		file.Close()
	}

	sort.Sort(ByKey(kv))

	file, err := os.Create("mpr-out-" + strconv.Itoa(args.TaskId) + "-" + strconv.Itoa(args.PTaskId))
	if err != nil {
		return err
	}

	i := 0
	for i < len(kv) {
		j := i + 1
		for j < len(kv) && kv[j].Key == kv[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kv[k].Value)
		}
		output := reducef(kv[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", kv[i].Key, output)

		i = j
	}
	file.Close()
	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func Call_task(flag string, work Job) (Rep, error) {

	// declare an argument structure.
	args := Req{}
	args.Flag = flag
	args.Job = work
	// fill in the argument(s).

	// declare a reply structure.
	reply := Rep{}

	// send the RPC request, wait for the reply.
	f := call("Coordinator.Get_task", &args, &reply)
	if !f {
		return Rep{}, errors.New("call failed")
	}

	return reply, nil
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
