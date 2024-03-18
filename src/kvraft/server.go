package kvraft

import (
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
	waitChan sync.Map
	data     sync.Map
	repId    sync.Map
}

func (kv *KVServer) IsPastReq(clerkId string, nowReqId int) bool {
	reqId, ok := kv.repId.Load(clerkId)
	return ok && reqId.(int) >= nowReqId
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
	}
	DPrintf("%v 接收到Get: Key:%v, ReqId:%v, ClerkId:%v", kv.me, args.Key, args.ReqId, args.ClerkId)
	defer DPrintf("%v 发送 %v", kv.me, reply)
	reply.Err = OK

	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(Op{OpType: "Get", ReqId: args.ReqId, Key: args.Key, ClerkId: args.ClerkId})
	kv.mu.Unlock()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	chAny, ok := kv.waitChan.Load(index)
	if !ok {
		chAny = make(chan Op, 1)
		kv.waitChan.Store(index, chAny)
	}
	ch, _ := chAny.(chan Op)
	select {
	case <-time.After(100 * time.Millisecond):
		reply.Err = ErrTimeOut
		return
	case command := <-ch:
		if command.ClerkId != args.ClerkId || command.ReqId != args.ReqId {
			reply.Err = ErrWrongLeader
			return
		}
		if value, ok := kv.data.Load(args.Key); ok {
			reply.Value = value.(string)
		} else {
			reply.Err = ErrNoKey
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
	}
	defer DPrintf("%v 发送给%v: %v", kv.me, args.ClerkId, reply)
	reply.Err = OK
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(Op{OpType: args.Op, ReqId: args.ReqId, Key: args.Key, Value: args.Value, ClerkId: args.ClerkId})
	kv.mu.Unlock()
	DPrintf("%v 接收到PA: Key:%v, Value:%v, Op:%v, ReqId:%v, ClerkId:%v index:%v", kv.me, args.Key, args.Value, args.Op, args.ReqId, args.ClerkId, index)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	chAny, ok := kv.waitChan.Load(index)
	if !ok {
		chAny = make(chan Op, 1)
		kv.waitChan.Store(index, chAny)
	}
	ch, ok := chAny.(chan Op)
	if !ok {
		panic("chAny transform error")
	}
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
		val := <-kv.applyCh
		if val.CommandValid {
			ch, ok := kv.waitChan.Load(val.CommandIndex)
			if !ok {
				ch = make(chan Op, 1)
				kv.waitChan.Store(val.CommandIndex, ch)
			}
			args := val.Command.(Op)
			ch.(chan Op) <- args
			DPrintf("ListenApply 接收到 kv_id:%v, 来自 %v 的请求:Key:%v, Value:%v, Op:%v, ReqId:%v index:%v", kv.me, args.ClerkId, args.Key, args.Value, args.OpType, args.ReqId, val.CommandIndex)
			if kv.IsPastReq(args.ClerkId, args.ReqId) {
				nowReqId, _ := kv.repId.Load(args.ClerkId)
				DPrintf("kv_id:%v, 来自 %v 的请求已过期:Key:%v, Value:%v, Op:%v, ReqId:%v, nowReqId:%v", kv.me, args.ClerkId, args.Key, args.Value, args.OpType, args.ReqId, nowReqId)
				continue
			}
			kv.repId.Store(args.ClerkId, args.ReqId)

			if args.OpType == PutOp {
				kv.mu.Lock()
				kv.data.Store(args.Key, args.Value)
				kv.mu.Unlock()
				DPrintf("%v 已经 PutAppend 成功: %v:%v", kv.me, args.Key, args.Value)
			} else if args.OpType == AppendOp {
				kv.mu.Lock()
				value, ok := kv.data.Load(args.Key)
				if !ok {
					value = ""
				}
				kv.data.Store(args.Key, value.(string)+args.Value)
				kv.mu.Unlock()
				DPrintf("%v 已经 PutAppend 成功: %v:%v", kv.me, args.Key, value.(string)+args.Value)
			}
			DPrintf("写入 %v", val.Command.(Op))
		}
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

	// You may need initialization code here.
	go kv.ListenApply()
	return kv
}
