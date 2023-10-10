package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
const (
	follower = iota
	candidate
	leader
)

type LogEntry struct {
	Command interface{} //命令
	TermId  int         //任期
	Index   int         //下标
}
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state int //身份
	//所有机器需要持久化的状态
	currentTerm int        //当前任期
	votedFor    int        //投给谁了
	logEntries  []LogEntry //日志信息
	//所有机器的可变状态
	commitIndex int //将提交的日志的索引
	lastApplied int //已经被提交到状态机的最后一个日志的索引
	//leader 的可变状态
	nextIndex  []int //下条发送到该机器的日志索引
	matchIndex []int //将要复制给该机器日志的索引
	//计时器
	timerElect       *time.Timer //选举计时器
	timerHeartBeat   *time.Timer //心跳计时器
	timeoutHeartBeat int         //心跳频率/ms
	timeoutElect     int         //选举频率/ms
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == leader)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) setElectTimer() {
	time_ := rand.Intn(200) + rf.timeoutElect
	rf.timerElect.Reset(time.Millisecond * time.Duration(time_))
}

type AppendEntriesArgs struct {
	TermId   int //leader 任期
	LeaderId int //用来 follower 重定向到 leader
}

type AppendEntriesReply struct {
	TermId int  //当前任期，leader 用来更新自己
	Sucess bool //如果follower包含索引为 prevlogindex 和任期为 prevlogitem 的日志
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Println(rf.me, "get", args.LeaderId)
	defer func() {
		reply.TermId = rf.currentTerm
	}()
	reply.Sucess = true
	if args.TermId < rf.currentTerm {
		reply.Sucess = false
		return
	}
	rf.setElectTimer()
	rf.toFollower()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	TermId       int //候选者的任期
	CandidateId  int //候选者编号
	LastLogIndex int //候选者最后一条日志记录的索引
	LastLogTerm  int //候选者最后一条日志记录的索引的任期
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	TermId      int  //当前任期、候选者用来更新自己
	VoteGranted bool //如果候选者当选则为 true
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		reply.TermId = rf.currentTerm
	}()
	reply.VoteGranted = false
	if args.TermId < rf.currentTerm || (args.TermId == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		return
	}
	rf.currentTerm = args.TermId

	if args.TermId > rf.currentTerm {
		rf.votedFor = -1
		rf.toFollower()
		return
	}
	// lastIndex := len(rf.logEntries) - 1
	// if lastIndex == -1 || (rf.logEntries[lastIndex].TermId < args.TermId) ||
	// 	(rf.logEntries[lastIndex].Index <= args.LastLogIndex && rf.logEntries[lastIndex].TermId == args.TermId) {
	rf.setElectTimer()
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	// return
	// }
}

func (rf *Raft) Broadcast() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == leader {
		// log.Println(rf.me, "BroadCast")
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.appendEntriesHandler(i)
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) appendEntriesHandler(server int) {
	rf.mu.Lock()
	if rf.state != leader {
		return
	}
	rf.mu.Unlock()
	args := AppendEntriesArgs{
		TermId:   rf.currentTerm,
		LeaderId: rf.me,
	}
	reply := AppendEntriesReply{}
	rf.sendAppendEntries(server, &args, &reply)

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) toFollower() {
	log.Println(rf.me, "跟随者")
	rf.state = follower

}

func (rf *Raft) toCandidate() {
	if rf.state == candidate {
		return
	}
	log.Println(rf.me, "候选者")
	rf.currentTerm++
	rf.state = candidate
	rf.votedFor = rf.me
}

func (rf *Raft) toLeader() {
	log.Println(rf.me, "领导")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = leader

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logEntries)
	}
	rf.timerHeartBeat.Reset(0)
}
func (rf *Raft) startElection() {
	rf.toCandidate()
	voteNums := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := RequestVoteArgs{
			TermId:       rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.logEntries[len(rf.logEntries)-1].Index,
			LastLogTerm:  rf.logEntries[len(rf.logEntries)-1].TermId,
		}
		go func(id int) {
			reply := RequestVoteReply{}
			ff := rf.sendRequestVote(id, &args, &reply)
			if ff {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.TermId == rf.currentTerm && rf.state == candidate {
					if reply.VoteGranted {
						voteNums++
						if voteNums*2 > len(rf.peers) {
							go rf.toLeader()
						}
					}
				} else if reply.TermId > rf.currentTerm {
					rf.currentTerm = reply.TermId
					rf.toFollower()
					rf.votedFor = -1
				}
			}
		}(i)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// log.Println(rf.state, rf.votedFor, rf.currentTerm)
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.timerElect.C: //选举超时
			if rf.killed() {
				break
			}
			log.Println(rf.me, "准备重置选举")
			rf.mu.Lock()
			if rf.state != leader {
				log.Println(rf.me, " ", rf.currentTerm, "开始选举")
				rf.startElection()
			}
			//重置选举计时器
			rf.setElectTimer()
			rf.mu.Unlock()
		case <-rf.timerHeartBeat.C: //没有收到心跳信息
			if rf.killed() {
				break
			}
			log.Println(rf.me, "准备重置心跳")
			rf.mu.Lock()
			if rf.state == leader {
				go rf.Broadcast()
			}
			rf.timerHeartBeat.Reset(time.Duration(rf.timeoutHeartBeat) * time.Millisecond)
			rf.mu.Unlock()
		// default:
		// 	log.Println(rf.me, "等着呢")
		}
	}
	log.Println(rf.me, "DONE!!!!")
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = follower
	rf.logEntries = make([]LogEntry, 0)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.timeoutHeartBeat = 150
	rf.timeoutElect = 300
	rf.timerHeartBeat = time.NewTimer(time.Duration(rf.timeoutHeartBeat) * time.Millisecond)
	rf.timerElect = time.NewTimer(time.Duration(rf.timeoutElect+rand.Intn(1000)) * time.Millisecond)
	rf.logEntries = append(rf.logEntries, LogEntry{-1, 0, -1})
	// Your initialization code here (2A, 2B, 2C).
	// File, err := os.Open("2A.txt")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// log.SetOutput(File)
	// defer File.Close()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
