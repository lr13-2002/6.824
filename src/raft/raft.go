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

	"bytes"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
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
}
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	applyMu   sync.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg       // applied message
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
	nextIndex      []int //下条发送到该机器的日志索引
	matchIndex     []int //将要复制给该机器日志的索引
	sortMatchIndex []int //用来得到 commitId
	//计时器
	timerElect       *time.Timer //选举计时器
	timerHeartBeat   *time.Timer //心跳计时器
	timeoutHeartBeat int         //心跳频率/ms
	timeoutElect     int         //选举频率/ms
	//snapshot
	lastIncludedIndex int //快照包含的最后一个索引
	lastIncludedItem  int //快照包含的最后一个任期
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedItem)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedItem int
	var logEntries []LogEntry
	if d.Decode(&currentTerm) != nil {
		panic(d.Decode(&currentTerm))
	} else if d.Decode(&votedFor) != nil {
		panic(d.Decode(&votedFor))
	} else if d.Decode(&logEntries) != nil {
		panic(d.Decode(&logEntries))
	} else if d.Decode(&lastIncludedIndex) != nil {
		panic(d.Decode(&lastIncludedIndex))
	} else if d.Decode(&lastIncludedItem) != nil {
		panic(d.Decode(&lastIncludedItem))
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logEntries = logEntries
	rf.lastIncludedItem = lastIncludedItem
	rf.lastIncludedIndex = lastIncludedIndex
	if rf.lastIncludedIndex != -1 {
		rf.lastApplied = rf.lastIncludedIndex
	}
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
	rf.mu.Lock()
	DPrintf("%v 接收到快照: %v %v %v", rf.me, index, snapshot, rf.lastApplied)
	defer rf.mu.Unlock()
	if rf.lastIncludedIndex >= index || rf.lastApplied < index {
		return
	}
	rf.lastIncludedItem = rf.logEntries[rf.GetRealIndex(index)].TermId
	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{-1, 0})
	rf.logEntries = append(newLog, rf.logEntries[rf.GetRealIndex(index+1):]...)
	DPrintf("%v 快照之后的 log\n%v", rf.me, getNumberLog(rf.logEntries))
	rf.lastIncludedIndex = index
	rf.persist()
	raftState := rf.persister.ReadRaftState()
	rf.persister.SaveStateAndSnapshot(raftState, snapshot)
}

func (rf *Raft) setElectTimer() {
	time_ := rand.Intn(200) + rf.timeoutElect
	rf.timerElect.Reset(time.Millisecond * time.Duration(time_))
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
	defer func() {
		reply.TermId = rf.currentTerm
		rf.persist()
		rf.mu.Unlock()
	}()
	reply.VoteGranted = false
	if args.TermId < rf.currentTerm || (args.TermId == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		return
	}
	if rf.currentTerm < args.TermId {
		rf.toFollower()
		rf.votedFor = -1
	}
	rf.currentTerm = args.TermId
	DPrintf("candidate:%v %v\nfollower: %v %v", args.LastLogIndex, args.LastLogTerm, rf.GetLastIndex(), rf.GetLastItem())
	if rf.GetLastItem() > args.LastLogTerm || (rf.GetLastItem() == args.LastLogTerm && args.LastLogIndex < rf.GetLastIndex()) {
		return
	}
	rf.toFollower()
	rf.setElectTimer()
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	TermId            int    //当前任期
	LeaderId          int    //定位 leader
	LastIncludedIndex int    //快照包含的最后一个索引
	LastIncludedItem  int    //快照包含的最后一个任期
	Data              []byte //快照
}

type InstallSnapshotReply struct {
	TermId int //follower 的任期
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if args.TermId < rf.currentTerm || args.LastIncludedIndex <= rf.lastIncludedIndex {
		reply.TermId = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	rf.setElectTimer()
	DPrintf("%v 接收到 %v 的快照: %v", rf.me, args.LeaderId, args)
	if args.TermId > rf.currentTerm {
		rf.currentTerm = args.TermId
		rf.votedFor = -1
		rf.toFollower()
	}
	reply.TermId = rf.currentTerm
	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{-1, 0})
	for i := args.LastIncludedIndex + 1; i < rf.GetLogLen(); i++ {
		if rf.logEntries[rf.GetRealIndex(i)].TermId > args.LastIncludedItem {
			newLog = append(newLog, rf.logEntries[rf.GetRealIndex(i)])
		}
	}
	rf.logEntries = newLog
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedItem = args.LastIncludedItem
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}
	raftState := rf.persister.ReadRaftState()
	rf.persister.SaveStateAndSnapshot(raftState, args.Data)
	applyMsg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		SnapshotTerm:  rf.lastIncludedItem,
		SnapshotIndex: rf.lastIncludedIndex,
		Snapshot:      args.Data,
	}
	rf.mu.Unlock()
	rf.applyCh <- applyMsg
}

func (rf *Raft) SendInstallSnapshotToFollower(server int, args InstallSnapshotArgs, ch chan struct{}) {
	defer func() {
		if !rf.killed() {
			rf.mu.Unlock()
			ch <- struct{}{}
		}
	}()
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != leader || rf.currentTerm != args.TermId {
			return
		}
		rf.mu.Unlock()
		reply := InstallSnapshotReply{}
		ff := rf.sendInstallSnapshot(server, &args, &reply)
		if !ff {
			continue
		}
		rf.mu.Lock()
		if rf.currentTerm < reply.TermId {
			rf.currentTerm = reply.TermId
			rf.toFollower()
			rf.persist()
			break
		}
		if rf.nextIndex[server] < args.LastIncludedIndex+1 {
			rf.nextIndex[server] = args.LastIncludedIndex + 1
		}
		if rf.matchIndex[server] < args.LastIncludedIndex {
			rf.matchIndex[server] = args.LastIncludedIndex
		}
		break
	}
}

type AppendEntriesArgs struct {
	TermId         int        //leader 任期
	LeaderId       int        //用来 follower 重定向到 leader
	PrevLogIndex   int        //前继日志记录的索引
	PrevLogItem    int        //前继日志记录的任期
	LogEntry       []LogEntry //日志记录
	LeaderCommitId int        //leader 的 commitIndex
}

type AppendEntriesReply struct {
	TermId    int  //当前任期，leader 用来更新自己
	SkipIndex int  //优化日志复制
	SkipTerm  int  //优化日志复制
	Sucess    bool //如果follower包含索引为 prevlogindex 和任期为 prevlogitem 的日志
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("%v 收到 %v 的心跳\n%v:%v\n%v:%v\n%v Log: %v", rf.me, args.LeaderId, args.LeaderId, args.TermId, rf.me, rf.currentTerm, rf.me, getNumberLog(rf.logEntries))
	defer func() {
		reply.TermId = rf.currentTerm
		rf.persist()
		DPrintf("%v 回复 %v: %v", rf.me, args.LeaderId, reply)
		rf.mu.Unlock()
	}()
	reply.SkipTerm = rf.currentTerm
	reply.Sucess = true
	reply.SkipIndex = args.PrevLogIndex
	if args.TermId < rf.currentTerm {
		reply.Sucess = false
		return
	}
	rf.setElectTimer()
	if args.TermId > rf.currentTerm {
		rf.currentTerm = args.TermId
		rf.votedFor = -1
		rf.toFollower()
	}

	ff := false
	DPrintf("PrevLogIndex:%v  PrevLogItem:%v  LogEntry:%v  LeaderCommitId:%v\n %v:%v", args.PrevLogIndex, args.PrevLogItem, getNumberLog(args.LogEntry), args.LeaderCommitId, rf.me, getNumberLog(rf.logEntries))

	if args.PrevLogIndex < rf.GetLogLen() && rf.GetRealIndex(args.PrevLogIndex) >= 0 {
		if rf.logEntries[rf.GetRealIndex(args.PrevLogIndex)].TermId != args.PrevLogItem {
			DPrintf("PrevLogTerm: %v, TermId: %v", rf.logEntries[rf.GetRealIndex(args.PrevLogIndex)].TermId, args.PrevLogItem)
			reply.SkipTerm = rf.logEntries[rf.GetRealIndex(args.PrevLogIndex)].TermId
			for rf.logEntries[rf.GetRealIndex(reply.SkipIndex)].TermId > args.PrevLogItem {
				reply.SkipIndex--
			}
			reply.Sucess = false
			return
		}
		var i int
		for i = 0; i < len(args.LogEntry) && args.PrevLogIndex+i+1 < rf.GetLogLen(); i++ {
			if args.LogEntry[i] != rf.logEntries[rf.GetRealIndex(args.PrevLogIndex+i+1)] {
				ff = true
				break
			}
		}
		if i < len(args.LogEntry) {
			ff = true
		}
	} else {
		reply.SkipIndex = rf.GetLogLen()
		reply.Sucess = false
		return
	}
	if ff {
		rf.logEntries = append(rf.logEntries[:rf.GetRealIndex(args.PrevLogIndex+1)], args.LogEntry...)
	}
	DPrintf("%v: %v", rf.me, getNumberLog(rf.logEntries))
	nowCommitIndex := args.LeaderCommitId
	if nowCommitIndex > rf.GetLastIndex() {
		nowCommitIndex = rf.GetLastIndex()
	}

	DPrintf("%v: LastCommitIndex: %v NowCommitIndex: %v LastApplied: %v", rf.me, rf.commitIndex, nowCommitIndex, rf.lastApplied)
	if rf.commitIndex < nowCommitIndex {
		rf.commitIndex = nowCommitIndex
	}
	if rf.lastApplied < rf.commitIndex {
		go rf.Apply()
	}
}

func (rf *Raft) Broadcast() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == leader {
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.AppendEntriesToFollower(i)
		}
	}
}

func (rf *Raft) Apply() {
	ok := rf.applyMu.TryLock()
	if !ok {
		DPrintf("%v 已经开始 Apply", rf.me)
		return
	}
	DPrintf("%v 开始 apply", rf.me)
	for !rf.killed() {
		rf.mu.Lock()
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			break
		}
		rf.lastApplied++
		ApplyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logEntries[rf.GetRealIndex(rf.lastApplied)].Command,
			CommandIndex: rf.lastApplied,
		}
		DPrintf("%v 提交 %v %v", rf.me, hashToNumber(rf.logEntries[rf.GetRealIndex(rf.lastApplied)].Command), rf.lastApplied)
		rf.mu.Unlock()
		rf.applyCh <- ApplyMsg
	}
	DPrintf("%v 完成 apply", rf.me)
	rf.applyMu.Unlock()
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.

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
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()
	term = rf.currentTerm
	if rf.state != leader {
		isLeader = false
		return index, term, isLeader
	}
	DPrintf("%v 接收到 %v", rf.me, hashToNumber(command))
	index = rf.GetLogLen()
	newEntry := LogEntry{command, term}
	rf.logEntries = append(rf.logEntries, newEntry)
	rf.matchIndex[rf.me] = rf.GetLastIndex()
	return index, term, isLeader
}

func (rf *Raft) GetLog(index int) []LogEntry {
	newLog := make([]LogEntry, 0)
	for i := index; i < rf.GetLogLen(); i++ {
		newLog = append(newLog, rf.logEntries[rf.GetRealIndex(i)])
	}
	return newLog
}

func (rf *Raft) GetLastIndex() int {
	index := len(rf.logEntries) - 1
	if rf.lastIncludedIndex != -1 {
		if index == 0 {
			return rf.lastIncludedIndex
		} else {
			return index + rf.lastIncludedIndex
		}
	}
	return index
}

func (rf *Raft) GetLastItem() int {
	index := len(rf.logEntries) - 1
	if rf.lastIncludedIndex != -1 {
		if index == 0 {
			return rf.lastIncludedItem
		}
	}
	return rf.logEntries[index].TermId
}

func (rf *Raft) GetRealIndex(index int) int {
	if rf.lastIncludedIndex != -1 {
		index -= rf.lastIncludedIndex
	}
	return index
}

func (rf *Raft) GetLogLen() int {
	len := len(rf.logEntries)
	if rf.lastIncludedIndex != -1 {
		return len + rf.lastIncludedIndex
	}
	return len
}

func (rf *Raft) AppendEntriesToFollower(server int) {
	defer func() {
		if !rf.killed() {
			rf.mu.Unlock()
		}
	}()

	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != leader {
			return
		}
		DPrintf("%v 发送给 %v nextIndex: %v lastIncludedIndex: %v \nLogEntry: %v  ", rf.me, server, rf.nextIndex[server], rf.lastIncludedIndex, getNumberLog(rf.logEntries))
		lastNextIndex := rf.nextIndex[server]
		lastIndex := rf.GetLastIndex()
		if rf.GetRealIndex(lastNextIndex-1) < 0 {
			args := InstallSnapshotArgs{
				TermId:            rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedItem:  rf.lastIncludedItem,
				Data:              rf.persister.ReadSnapshot(),
			}
			ch := make(chan struct{})
			rf.mu.Unlock()
			go rf.SendInstallSnapshotToFollower(server, args, ch)
			<-ch
			continue
		}
		sendIndex := rf.nextIndex[server] - 1
		args := AppendEntriesArgs{
			TermId:         rf.currentTerm,
			LeaderId:       rf.me,
			PrevLogIndex:   rf.nextIndex[server] - 1,
			PrevLogItem:    rf.logEntries[rf.GetRealIndex(sendIndex)].TermId,
			LogEntry:       rf.GetLog(sendIndex + 1),
			LeaderCommitId: rf.commitIndex,
		}
		rf.mu.Unlock()
		reply := AppendEntriesReply{}
		ff := rf.sendAppendEntries(server, &args, &reply)
		if !ff {
			continue
		}
		rf.mu.Lock()
		if rf.state != leader || args.TermId != rf.currentTerm || sendIndex < rf.lastIncludedIndex {
			return
		}
		DPrintf("%v 收到 %v 心跳回复 reply: %v", rf.me, server, reply)
		if reply.TermId > rf.currentTerm {
			DPrintf("leader 被发现 Term 较低\n%v: %v\n%v:%v", rf.me, rf.currentTerm, server, reply.TermId)
			rf.votedFor = -1
			rf.currentTerm = reply.TermId
			rf.toFollower()
			rf.persist()
			return
		}
		if reply.Sucess {
			if rf.matchIndex[server] < lastIndex {
				rf.matchIndex[server] = lastIndex
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			}
			break
		} else if rf.nextIndex[server] == lastNextIndex {
			DPrintf("nextIndex rollback\nlog: %v\n term: %v nextIndex: %v", getNumberLog(rf.logEntries), reply.SkipTerm, rf.nextIndex[server])
			rf.nextIndex[server] = reply.SkipIndex
			for rf.GetRealIndex(rf.nextIndex[server]-1) >= 0 && rf.logEntries[rf.GetRealIndex(rf.nextIndex[server]-1)].TermId > reply.SkipTerm {
				rf.nextIndex[server] -= 1
			}
			DPrintf("优化发生 nextIndex: %v", rf.nextIndex[server])
		} else {
			return
		}
		rf.mu.Unlock()
	}
	if rf.killed() {
		return
	}
	copy(rf.sortMatchIndex, rf.matchIndex)
	sort.Slice(rf.sortMatchIndex, func(i, j int) bool {
		return rf.sortMatchIndex[i] < rf.sortMatchIndex[j]
	})
	minMatchIndex := rf.sortMatchIndex[len(rf.sortMatchIndex)/2]
	if minMatchIndex > rf.GetLogLen() {
		minMatchIndex = rf.GetLogLen()
	}
	DPrintf("%v 发送 %v 完毕, minMatchIndex: %v commitIndex: %v", rf.me, server, minMatchIndex, rf.commitIndex)
	if (rf.GetRealIndex(minMatchIndex) < 0 || rf.logEntries[rf.GetRealIndex(minMatchIndex)].TermId == rf.currentTerm) && minMatchIndex > rf.commitIndex {
		rf.commitIndex = minMatchIndex
	}
	if rf.lastApplied < rf.commitIndex {
		go rf.Apply()
	}
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
	DPrintf("%v 是跟随者\n", rf.me)
	rf.state = follower
}

func (rf *Raft) toCandidate() {
	rf.currentTerm++
	DPrintf("%v 是候选者\n", rf.me)
	rf.state = candidate
	rf.votedFor = rf.me
	rf.persist()
}

func hashToNumber(value interface{}) string {
	switch value_type := value.(type) {
	case int:
		{
			return strconv.Itoa(value_type)
		}
	case string:
		{
			return value.(string)
		}
	default:
		{
			panic("interface type is not string or int")
		}
	}

}

type NumberLog struct {
	Command string
	TermId  int
}

func getNumberLog(Log []LogEntry) []NumberLog {
	var returnLog []NumberLog
	// for _, i := range Log {
	// 	returnLog = append(returnLog, NumberLog{hashToNumber(i.Command), i.TermId})
	// }
	return returnLog
}

func (rf *Raft) toLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != candidate {
		return
	}
	DPrintf("%v 是领导 当前任期 %v len: %v\n", rf.me, rf.currentTerm, rf.GetLogLen())
	rf.state = leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.sortMatchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.GetLogLen()
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
		LastTermId := rf.currentTerm
		args := RequestVoteArgs{
			TermId:       rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.GetLastIndex(),
			LastLogTerm:  rf.GetLastItem(),
		}
		go func(id int) {
			reply := RequestVoteReply{}
			ff := rf.sendRequestVote(id, &args, &reply)
			if ff {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if LastTermId == rf.currentTerm && rf.state == candidate {
					DPrintf("%v 选举收到 %v 的回复 状态为 %v 当前有 %v 票", rf.me, id, reply.VoteGranted, voteNums)
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
					rf.persist()
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
			DPrintf("%v 准备重置选举\n", rf.me)
			rf.mu.Lock()
			if rf.state != leader {
				DPrintf("%v 开始选举 当前任期为 %v\n", rf.me, rf.currentTerm+1)
				rf.startElection()
			}
			//重置选举计时器
			rf.setElectTimer()
			rf.mu.Unlock()
		case <-rf.timerHeartBeat.C: //没有收到心跳信息
			if rf.killed() {
				break
			}
			rf.mu.Lock()
			DPrintf("%v 准备重置心跳 当前任期为 %v 投给了 %v\n", rf.me, rf.currentTerm, rf.votedFor)
			if rf.state == leader {
				DPrintf("%v 发送心跳\n", rf.me)
				go rf.Broadcast()
			}
			rf.timerHeartBeat.Reset(time.Duration(rf.timeoutHeartBeat) * time.Millisecond)
			DPrintf("%v 重置心跳 当前任期为 %v 投给了 %v\n", rf.me, rf.currentTerm, rf.votedFor)
			rf.mu.Unlock()
		}
	}
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
	rf.applyCh = applyCh
	rf.me = me
	rf.state = follower
	rf.logEntries = make([]LogEntry, 0)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.timeoutHeartBeat = 100
	rf.timeoutElect = 450
	rf.state = follower
	rf.commitIndex = 0
	rf.lastIncludedIndex = -1
	rf.lastIncludedItem = 0
	rf.lastApplied = 0
	rf.timerHeartBeat = time.NewTimer(time.Duration(rf.timeoutHeartBeat) * time.Millisecond)
	rf.timerElect = time.NewTimer(time.Duration(rf.timeoutElect+rand.Intn(1000)) * time.Millisecond)
	rf.logEntries = append(rf.logEntries, LogEntry{-1, 0})
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("当前的机器为 %v\n", len(peers))
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
