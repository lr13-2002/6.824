package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	CMutex     sync.Mutex //临界区资源保护
	NReduce    int        //拆分成多少文件
	MapWork    []Job
	ReduceWork []Job
	Status     string //"MAP" "REDUCE" "DONE"
	TaskId     int
	PTaskId    int
}

const MaxTime = 10

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Get_task(args *Req, reply *Rep) error {
	c.CMutex.Lock()
	defer c.CMutex.Unlock()
	reply.Flag = "NONE"
	log.Println(c.Status)
	if args.Flag == "GET" {
		if c.Status == "MAP" {
			for i, k := range c.MapWork {
				log.Println(k, " ", int64(MaxTime), time.Now().Unix())
				if k.Status == run && k.StartTime+MaxTime < time.Now().Unix() {
					k.Status = wait
					c.MapWork[i].Status = wait
				}

				if k.Status == wait {
					c.MapWork[i].Status = run
					c.MapWork[i].TaskId = c.TaskId
					c.MapWork[i].StartTime = time.Now().Unix()
					reply.Flag = "MAP"
					reply.Job = c.MapWork[i]
					reply.NReduce = c.NReduce
					c.TaskId++
					return nil
				}
			}
		} else if c.Status == "REDUCE" {
			for i, k := range c.ReduceWork {
				log.Println(k, " ", MaxTime, time.Now().Unix())
				if k.Status == run && k.StartTime+MaxTime < time.Now().Unix() {
					k.Status = wait
					c.ReduceWork[i].Status = wait
				}

				if k.Status == wait {
					c.ReduceWork[i].Status = run
					c.ReduceWork[i].StartTime = time.Now().Unix()
					c.ReduceWork[i].PTaskId = c.PTaskId
					reply.MaxId = c.TaskId
					reply.Flag = "REDUCE"
					reply.Job = c.ReduceWork[i]
					reply.NReduce = c.NReduce
					c.PTaskId++
					return nil
				}
			}
		} else {
			reply.Flag = "DONE"
		}
	} else {
		if c.Status == "MAP" {
			for i, k := range c.MapWork {
				if k == args.Job {
					c.MapWork[i].Status = done
					for j := 0; j < c.NReduce; j++ {
						os.Rename("mpr-tmp-"+strconv.Itoa(j)+"-"+strconv.Itoa(args.TaskId), "mr-tmp-"+strconv.Itoa(j)+"-"+strconv.Itoa(args.TaskId))
					}
					break
				}
			}
			for _, k := range c.MapWork {
				if k.Status != done {
					return nil
				}
			}
			c.Status = "REDUCE"
		} else if c.Status == "REDUCE" {
			log.Println("PUT", args)
			for i, k := range c.ReduceWork {
				if k == args.Job {
					c.ReduceWork[i].Status = done
					os.Rename("mpr-out-"+strconv.Itoa(args.TaskId)+"-"+strconv.Itoa(args.PTaskId), "mr-out-"+strconv.Itoa(args.TaskId))
					break
				}
			}
			for _, k := range c.ReduceWork {
				if k.Status != done {
					return nil
				}
			}
			c.Status = "DONE"
		}
	}
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
	c.CMutex.Lock()
	defer c.CMutex.Unlock()
	return c.Status == "DONE"
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		CMutex:     sync.Mutex{},
		NReduce:    10,
		MapWork:    make([]Job, 0),
		ReduceWork: make([]Job, 0),
		Status:     "MAP",
		TaskId:     0,
		PTaskId:    0,
	}
	filelog, err := os.OpenFile("masterlogfile"+strconv.Itoa(os.Getegid()), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	log.SetOutput(filelog)
	for _, filename := range files {
		c.MapWork = append(c.MapWork, Job{
			FileName: filename,
			Status:   wait,
		})
	}
	// Your code here.
	for i := 0; i < nReduce; i++ {
		c.ReduceWork = append(c.ReduceWork, Job{
			FileName: "mr-tmp-" + strconv.Itoa(i) + "-",
			TaskId:   i,
			Status:   wait,
		})
	}
	c.server()
	return &c
}
