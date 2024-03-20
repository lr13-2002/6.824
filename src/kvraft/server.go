package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if raft.Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	PutOp    = "Put"
	AppendOp = "Append"
	GetOp    = "Get"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType  string
	ReqId   int
	Key     string
	Value   string
	ClerkId string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	waitChan map[int]chan Op
	data     map[string]string
	reqId    map[string]int
	lastSnapShot int
}

func (kv *KVServer) IsPastReq(clerkId string, nowReqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reqId, ok := kv.reqId[clerkId]
	return ok && reqId >= nowReqId
}

func (kv *KVServer) GetWaitChan(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitChan[index]
	if !exist {
		kv.waitChan[index] = make(chan Op, 1)
		ch = kv.waitChan[index]
	}
	return ch
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
	}
	DPrintf("%v 接收到Get: Key:%v, ReqId:%v, ClerkId:%v", kv.me, args.Key, args.ReqId, args.ClerkId)
	defer DPrintf("%v 发送Get给 %v,reply:%v", kv.me, args.ClerkId, reply)
	reply.Err = OK
	index, _, isLeader := kv.rf.Start(Op{OpType: "Get", ReqId: args.ReqId, Key: args.Key, ClerkId: args.ClerkId})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := kv.GetWaitChan(index)
	select {
	case <-time.After(100 * time.Millisecond):
		reply.Err = ErrTimeOut
		return
	case command := <-ch:
		if command.ClerkId != args.ClerkId || command.ReqId != args.ReqId {
			reply.Err = ErrWrongLeader
			return
		}
		kv.mu.Lock()
		if value, ok := kv.data[args.Key]; ok {
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
	}
	defer DPrintf("%v 发送给%v: %v", kv.me, args.ClerkId, reply)
	reply.Err = OK
	index, _, isLeader := kv.rf.Start(Op{OpType: args.Op, ReqId: args.ReqId, Key: args.Key, Value: args.Value, ClerkId: args.ClerkId})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("%v 接收到PA: Key:%v, Value:%v, Op:%v, ReqId:%v, ClerkId:%v index:%v", kv.me, args.Key, args.Value, args.Op, args.ReqId, args.ClerkId, index)

	ch := kv.GetWaitChan(index)
	select {
	case <-time.After(100 * time.Millisecond):
		reply.Err = ErrTimeOut
		return
	case command := <-ch:
		if command.ClerkId != args.ClerkId || command.ReqId != args.ReqId {
			reply.Err = ErrWrongLeader
			return
		}
	}
}

func (kv *KVServer) ListenApply() {
	for !kv.killed() {
		select {
		case val := <-kv.applyCh:
			if val.CommandValid {
				if val.CommandIndex <= kv.lastSnapShot {
					continue
				}
				ch := kv.GetWaitChan(val.CommandIndex)
				args := val.Command.(Op)
				nowReqId, ok := kv.reqId[args.ClerkId]
				if !ok {
					nowReqId = -1
				}
				DPrintf("ListenApply 接收到 kv_id:%v, 来自 %v 的请求:Key:%v, Value:%v, Op:%v, ReqId:%v , nowReqId:%v, index:%v", kv.me, args.ClerkId, args.Key, args.Value, args.OpType, args.ReqId, nowReqId, val.CommandIndex)
				if !kv.IsPastReq(args.ClerkId, args.ReqId) {
					kv.mu.Lock()
					if args.OpType == PutOp {
						kv.data[args.Key] = args.Value
					} else if args.OpType == AppendOp {
						kv.data[args.Key] += args.Value
					}
					DPrintf("%v 已经 PutAppend 成功: %v:%v", kv.me, args.Key, kv.data[args.Key])
					kv.reqId[args.ClerkId] = args.ReqId
					kv.mu.Unlock()
				}
				if kv.rf.GetRaftSize() > kv.maxraftstate && kv.maxraftstate != -1 {
					DPrintf("%v 发送快照 CommandIndex:%v, loglen:%v", kv.me, val.CommandIndex, kv.rf.LockGetLogLen())
					kv.rf.Snapshot(val.CommandIndex, kv.PersistSnapShot())
					kv.mu.Lock()
					kv.lastSnapShot = val.CommandIndex
					kv.mu.Unlock()
				}
				DPrintf("%v 写入 %v", kv.me, val.Command.(Op))
				if len(ch) == 1 {
					DPrintf("ch 中已有元素,可能会发生死锁")
				}
				ch <- args
				DPrintf("%v 写入成功 %v", kv.me, val.Command.(Op))
			} else if val.SnapshotValid {
				kv.mu.Lock()
				DPrintf("%v 接收到快照 val.SnapshotIndex:%v, kv.lastSnapShot:%v", kv.me, val.SnapshotIndex, kv.lastSnapShot)
				if val.SnapshotIndex > kv.lastSnapShot {
					kv.lastSnapShot = val.SnapshotIndex
					kv.DecodeSnapShot(val.Snapshot)
				}
				kv.mu.Unlock()
			}
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (kv *KVServer) InstallSnapshot() {
	for !kv.killed() {
		kv.mu.Lock()
		lastSnapShot := kv.lastSnapShot
		kv.mu.Unlock()
		if kv.rf.GetRaftSize() > kv.maxraftstate && kv.maxraftstate != -1  {
			DPrintf("%v 发送快照 CommandIndex:%v, loglen:%v", kv.me, lastSnapShot, kv.rf.LockGetLogLen())
			kv.rf.Snapshot(lastSnapShot, kv.PersistSnapShot())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	DPrintf("%v killed", kv.me)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	DPrintf("%v 快照之前", kv.me)
	DPrintf("data:")
	for key, value := range kv.data {
		DPrintf("%v~%v:%v", kv.me, key, value)
	}
	DPrintf("reqId:")
	for key, value := range kv.reqId {
		DPrintf("%v~%v:%v", kv.me, key, value)
	}
	var kvPersist map[string]string
	var seqMap map[string]int
	
	if d.Decode(&kvPersist) == nil && d.Decode(&seqMap) == nil {
		DPrintf("%v 快照之后", kv.me)
		DPrintf("data:")
		for key, value := range kvPersist {
			DPrintf("%v~%v:%v", kv.me, key, value)
		}
		kv.data = kvPersist
		DPrintf("reqId:")
		for key, value := range seqMap {
			DPrintf("%v~%v:%v", kv.me, key, value)
		}
		kv.reqId = seqMap
	} else {
		DPrintf("[Server(%v)] Failed to decode snapshot!!!", kv.me)
	}
}

func (kv *KVServer) PersistSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.reqId)
	data := w.Bytes()
	return data
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	atomic.StoreInt32(&kv.dead, 0)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.reqId = make(map[string]int)
	kv.data = make(map[string]string)
	kv.waitChan = make(map[int]chan Op)
	kv.DecodeSnapShot(persister.ReadSnapshot())
	kv.lastSnapShot = -1
	// You may need initialization code here.
	go kv.ListenApply()
	if kv.maxraftstate != -1 {
		go kv.InstallSnapshot()
	}
	return kv
}
