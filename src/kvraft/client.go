package kvraft

import (
	"crypto/rand"
	"math/big"
	"strconv"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	clerkId  int
	reqId    int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkId = int(nrand())
	ck.leaderId = 0
	ck.reqId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.reqId++
	args := GetArgs{
		Key:     key,
		ReqId:   ck.reqId,
		ClerkId: strconv.Itoa(ck.clerkId),
	}
	for {
		reply := GetReply{}
		DPrintf("ClerkId:%v Get发送 %v: %v", ck.clerkId, ck.leaderId, args)
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok {
			time.Sleep(100 * time.Millisecond)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		DPrintf("%v 接收到 %v", ck.clerkId, reply)
		if reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			if reply.Err == ErrTimeOut {
				time.Sleep(100 * time.Millisecond)
			}
		} else if reply.Err == ErrNoKey {
			return ""
		} else {
			DPrintf("ClerkId:%v Get接收到 %v: %v", ck.clerkId, key, reply.Value)
			return reply.Value
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.reqId++
	args := PutAppendArgs{
		Key:     key,
		Value:   value,
		Op:      op,
		ReqId:   ck.reqId,
		ClerkId: strconv.Itoa(ck.clerkId),
	}
	for {
		
		reply := PutAppendReply{}
		DPrintf("ClerkId:%v %v发送 %v: Key:%v, Value:%v, ReqId:%v", ck.clerkId, op, ck.leaderId, args.Key, value, args.ReqId)
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			time.Sleep(100 * time.Millisecond)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		DPrintf("ClerkId:%v %v接收到 %v", ck.clerkId, op, reply)
		if reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			if reply.Err == ErrTimeOut {
				time.Sleep(100 * time.Millisecond)
			}
		} else {
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("%v PUT接收到 key:%v, value:%v", ck.clerkId, key, value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
