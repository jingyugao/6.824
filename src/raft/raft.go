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
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"labrpc"
	"math/rand"
	"sync/atomic"

	"sync"
	"time"

	"github.com/golang/glog"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	UseSnapshot  bool
	Snapshot     []byte
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//

type LogEntry struct {
	Index  int
	Term   int
	Leader int
	Data   interface{}
}

const (
	stateLeader    = 1
	stateCandidate = 2
	stateFollower  = 3

	chanSize = 100

	heaetBeatInterval = 50 * time.Millisecond
)

type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int

	Data []byte
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	spshot Snapshot

	followCh chan struct{}

	curTerm       int
	state         int32
	electDuration time.Duration
	winChan       chan struct{}

	voteFor   int
	voteCount int32

	logCh       chan LogEntry
	log         []LogEntry
	logMu       sync.Mutex
	applyCh     chan ApplyMsg
	commitCh    chan int
	commitIndex int
	lastApplied int // used for application
	// just for leader
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.curTerm, rf.state == stateLeader
}

func (rf *Raft) RaftStateSize() int {
	// Your code here (2A).
	return rf.persister.RaftStateSize()
}
func (rf *Raft) IsLeader() bool {
	// Your code here (2A).
	return rf.state == stateLeader
}
func (rf *Raft) Leader() int {
	// Your code here (2A).
	return rf.voteFor
}
func (rf *Raft) lastIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) lastTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) becomeFollower(term int) {
	rf.curTerm = term
	rf.state = stateFollower
	rf.voteFor = -1
	rf.voteCount = 0
	rf.persist()
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
	e := gob.NewEncoder(w)
	e.Encode(rf.curTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}
func (rf *Raft) persistAll() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.curTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()

	w2 := new(bytes.Buffer)
	e2 := gob.NewEncoder(w2)
	e2.Encode(rf.spshot)
	data2 := w2.Bytes()

	rf.persister.SaveStateAndSnapshot(data, data2)
}
func (rf *Raft) MakeSnapshot(lastIncludedIndex int, data []byte) {
	// dont contain the lastCmtIdx log
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.log[0].Index || lastIncludedIndex > rf.lastIndex() {
		return
	}

	idx0 := rf.log[0].Index

	lastIncludedTerm := rf.log[lastIncludedIndex-idx0].Term
	rf.log = append([]LogEntry{
		LogEntry{Index: lastIncludedIndex,
			Term: lastIncludedTerm},
	}, rf.log[lastIncludedIndex-idx0+1:]...)

	rf.spshot = Snapshot{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	}
	rf.persistAll()

	// fmt.Printf("MakeSnapshot:%+v\n", rf.log)
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
	d := gob.NewDecoder(r)

	err := d.Decode(&rf.curTerm)
	if err != nil {
		panic(err)
	}
	err = d.Decode(&rf.voteFor)
	if err != nil {
		panic(err)
	}
	err = d.Decode(&rf.log)
	if err != nil {
		panic(err)
	}
}
func (rf *Raft) readPersistSp(data []byte) {

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	err := d.Decode(&rf.spshot)
	if err != nil {
		panic(err)
	}
	lastIncludedIndex := rf.spshot.LastIncludedIndex
	lastIncludedTerm := rf.spshot.LastIncludedTerm
	rf.commitIndex = rf.spshot.LastIncludedIndex
	rf.lastApplied = rf.spshot.LastIncludedIndex
	idx0 := rf.log[0].Index
	rf.log = append([]LogEntry{
		LogEntry{Index: lastIncludedIndex,
			Term: lastIncludedTerm},
	}, rf.log[lastIncludedIndex-idx0+1:]...)
	// rf.log = truncateLog(rf.spshot.LastIncludedIndex, rf.spshot.LastIncludedTerm, rf.log)
	rf.applyCh <- ApplyMsg{
		CommandValid: false,
		UseSnapshot:  true,
		Snapshot:     rf.spshot.Data,
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
// currentTerm is equal lastLogTerm
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	fn := "RequestVote"
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false

	// If RPC request or response contains term T > currentTerm:
	//  set currentTerm = T, convert to follower
	if args.Term > rf.curTerm {
		rf.becomeFollower(args.Term)
	}

	// Reply false if term < currentTerm
	if args.Term < rf.curTerm {
		reply.Term = rf.curTerm
		return
	}

	reply.Term = rf.curTerm

	// If the logs have last entries with different terms,
	// then the log with the later term is more up-to-date.
	// If the logs end with the same term,
	// then whichever log is longer is more up-to-date.
	uptodate := false
	if args.LastLogTerm > rf.lastTerm() {
		uptodate = true
	}
	if args.LastLogTerm == rf.lastTerm() &&
		args.LastLogIndex >= rf.lastIndex() {
		uptodate = true
	}
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateID) && !uptodate {
		glog.Warningf("[%s]: %d recv voteReq from %+v but %+v", fn, rf.me, args, rf.log[len(rf.log)-1])
	}
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateID) && uptodate {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateID
		rf.persist()
		rf.followCh <- struct{}{}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) (ok bool) {

	ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		return
	}
	term := rf.curTerm

	// If RPC request or response contains term T > currentTerm:
	//  set currentTerm = T, convert to follower
	if reply.Term > term {
		rf.becomeFollower(args.Term)
		return
	}

	if reply.VoteGranted && args.Term == rf.curTerm &&
		rf.voteFor == rf.me && rf.state == stateCandidate {
		cnt := atomic.AddInt32(&rf.voteCount, 1)
		if int(cnt) > len(rf.peers)/2 {
			rf.winChan <- struct{}{}
		}
	}
	return
}

// AppendEntries
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// 1. Reply false if term < currentTerm
	if args.Term < rf.curTerm {
		reply.Term = rf.curTerm
		reply.NextIndex = rf.lastIndex() + 1
		return
	}

	fn := "AppendEntries"
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false

	// RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	// AppendEntries RPC received from new leader: convert to
	// folower
	if args.Term > rf.curTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = args.Term

	// receiving AppendEntries RPC from current leader
	if args.Term == rf.curTerm && args.LeaderID == rf.voteFor {
		rf.followCh <- struct{}{}
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex > rf.lastIndex() {
		reply.NextIndex = rf.lastIndex() + 1
		return
	}

	idx0 := rf.log[0].Index
	if args.PrevLogIndex < idx0 {
		glog.Warningf("[%s] %d recv out-of-date Entries from %d,args:%+v, ", fn, rf.me, args.LeaderID, args)
	}
	if args.PrevLogIndex > idx0 && rf.log[args.PrevLogIndex-idx0].Term != args.PrevLogTerm {
		for i := args.PrevLogIndex - 1 - idx0; i >= 0; i-- {
			if rf.log[i].Term != rf.log[args.PrevLogIndex-idx0].Term {
				reply.NextIndex = i + 1 + idx0
				return
			}
		}
		return
	}

	reply.Success = true

	if len(args.Entries) > 0 && args.Entries[0].Index <= rf.commitIndex {
		glog.Warningf("[%s] %d to %d overwrite committed log, %+v, %+v.\n", fn, args.LeaderID, rf.me, args.Entries, rf.log)
	}

	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	for i, e := range args.Entries {
		if e.Index < rf.lastIndex()+1 && e.Term == rf.log[e.Index-idx0].Term {
			continue
		}
		// 4. Append any new entries not already in the log
		rf.log = append(rf.log[:e.Index-idx0], args.Entries[i:]...)
	}

	reply.NextIndex = rf.lastIndex() + 1

	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitCh <- min(args.LeaderCommit, rf.lastIndex())
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (ok bool) {
	fn := "sendAppendEntries"
	ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	glog.Infof("[%s]: %d send %+v to %d, ok?%v , recv %+v", fn, rf.me, args, server, ok, reply)

	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// ignore out-of-date response
	if rf.state != stateLeader || args.Term != rf.curTerm {
		glog.Warningf("[%s]: %d recv out-of-date reply from %d", fn, rf.me, server)
		fmt.Println("rf.state != stateLeader || args.Term != rf.curTerm")
		return
	}

	// RPC request or response contains term T > currentTerm:
	//  set currentTerm = T
	if reply.Term > rf.curTerm {
		rf.becomeFollower(reply.Term)
		fmt.Println("rf.state != stateLeader || args.Term != rf.curTerm")
		return
	}

	if !reply.Success {
		rf.nextIndex[server] = reply.NextIndex
		return
	}

	if len(args.Entries) > 0 {
		rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}

	return
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	FirstLog          LogEntry
	// Offset            int
	Data []byte
	Size int
	Done bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1. Reply immediately if term < currentTerm
	if args.Term < rf.curTerm {
		reply.Term = rf.curTerm
		return
	}

	if args.Term > rf.curTerm {
		rf.becomeFollower(args.Term)
	}

	reply.Term = args.Term

	if args.Term == rf.curTerm && args.LeaderID == rf.voteFor {
		rf.followCh <- struct{}{}
	}

	idx0 := rf.log[0].Index
	if args.LastIncludedIndex < idx0 {
		glog.Warningf("[] %d install out-of-date snapshot to %d,args: %+v, reply:%+v.", args.LeaderID, rf.me, args, reply)
	}

	// leader.lastIncludeIndex >= leader.commitIndex >= follower.commitIndex >= follow.idx0
	rf.spshot = Snapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
	}
	newLog := []LogEntry{
		LogEntry{
			Index: args.LastIncludedIndex,
			Term:  args.LastIncludedTerm,
		},
	}
	// 6. If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply
	if args.LastIncludedIndex > idx0 && args.LastIncludedIndex <= rf.lastIndex() && rf.log[args.LastIncludedIndex-idx0].Term == args.LastIncludedTerm {
		rf.log = append(newLog, rf.log[args.LastIncludedIndex-idx0+1:]...)
	} else {
		rf.log = newLog
	}
	rf.persistAll()

	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	// rf.commitCh <- args.LastIncludedIndex
	// this is not leader, neednt response to client
	rf.applyCh <- ApplyMsg{
		UseSnapshot:  true,
		CommandValid: false,
		Snapshot:     args.Data,
		CommandIndex: args.LastIncludedIndex,
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) (ok bool) {
	fn := "sendInstallSnapshot"
	glog.Infof("[%s]: %d send %+v to %d, recv %+v", fn, rf.me, args, server, reply)

	ok = rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.curTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex

	return
}

func (rf *Raft) heartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for N := rf.lastIndex(); N > rf.commitIndex; N-- {
		agreeCnt := 1
		for i := range rf.peers {
			if rf.me != i && rf.matchIndex[i] >= N {
				agreeCnt++
			}
		}
		// a majority of matchIndex[i] ≥ N and log[N].term == currentTerm
		idx0 := rf.log[0].Index
		if agreeCnt > len(rf.peers)/2 && rf.log[N-idx0].Term == rf.curTerm {
			rf.commitCh <- N
			break
		}
	}

	for i := range rf.peers {
		if i != rf.me {
			idx0 := rf.log[0].Index
			plIdx := rf.nextIndex[i] - 1
			offset := plIdx - idx0
			if rf.nextIndex[i] > idx0 {
				args := AppendEntriesArgs{
					Term:         rf.curTerm,
					LeaderID:     rf.me,
					PrevLogIndex: plIdx,
					PrevLogTerm:  rf.log[offset].Term,
					Entries:      rf.log[offset+1:],
					LeaderCommit: rf.commitIndex,
				}
				go rf.sendAppendEntries(i, &args, &AppendEntriesReply{})
			} else {
				args := InstallSnapshotArgs{
					Term:              rf.curTerm,
					LeaderID:          rf.me,
					LastIncludedIndex: rf.spshot.LastIncludedIndex,
					LastIncludedTerm:  rf.spshot.LastIncludedTerm,
					Data:              rf.spshot.Data,
					Done:              true,
				}
				go rf.sendInstallSnapshot(i, &args, &InstallSnapshotReply{})
			}
		}
	}
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
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	// Your code here (2B).
	isLeader := (rf.state == stateLeader)
	if !isLeader {
		return 0, 0, false
	}

	idx := rf.lastIndex()
	log := LogEntry{
		Index:  idx + 1,
		Term:   rf.curTerm,
		Leader: rf.me,
		Data:   command,
	}

	rf.log = append(rf.log, log)

	return idx + 1, rf.curTerm, isLeader
}

// func (rf *Raft) broadcastAppendEntries(log) {

// }

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf.voteFor = -1
	rf.state = stateFollower
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.log = append(rf.log, LogEntry{})
	rf.logCh = make(chan LogEntry, chanSize)
	rf.followCh = make(chan struct{}, chanSize)
	rf.winChan = make(chan struct{}, chanSize)
	rf.commitCh = make(chan int, chanSize)
	rf.applyCh = applyCh
	rf.electDuration = time.Duration((rand.Intn(150) + 150)) * time.Millisecond
	// Your initialization code here (2A, 2B, 2C).
	rf.readPersist(persister.ReadRaftState())
	rf.readPersistSp(persister.ReadSnapshot())

	go func() {
		for {
			switch rf.state {
			case stateFollower:
				select {
				case <-rf.followCh:
					// Reset election timer
				case <-time.After(rf.electDuration):
					rf.state = stateCandidate
				}

			case stateCandidate:
				rf.mu.Lock()
				rf.curTerm++ // Increment currentTerm
				// Reset election timer
				rf.electDuration = time.Duration((rand.Intn(150) + 150)) * time.Millisecond
				rf.voteFor = rf.me
				rf.voteCount = 1
				rf.persist()
				arg := &RequestVoteArgs{
					Term:         rf.curTerm,
					CandidateID:  rf.me,
					LastLogIndex: rf.lastIndex(),
					LastLogTerm:  rf.lastTerm(),
				}
				// Send RequestVote RPCs to all other servers
				for i := range rf.peers {
					if rf.me == i {
						continue
					}
					go rf.sendRequestVote(i, arg, &RequestVoteReply{})
				}
				rf.mu.Unlock()

				select {
				case <-rf.followCh:
					// become follower
				case <-rf.winChan:
					rf.mu.Lock()
					// When a leader first comes to power,
					// it initializes all nextIndex values to
					// the index just after the last one in its log
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = rf.lastIndex() + 1
						rf.matchIndex[i] = 0
					}
					rf.matchIndex[me] = rf.lastIndex()
					rf.state = stateLeader
					rf.mu.Unlock()

					// election timeout elapses: start new election
				case <-time.After(rf.electDuration):
				}

			case stateLeader:
				// send initial empty AppendEntries RPCs (heartbeat) to each server
				go rf.heartBeat()
				// repeat during idle periods to prevent election timeouts
				time.Sleep(heaetBeatInterval)
			}
		}
	}()

	// commitedIndex and appliedIndex
	go func() {
		for {
			select {

			case cmtIdx := <-rf.commitCh:
				rf.mu.Lock()
				idx0 := rf.log[0].Index
				// rf.lastApplied >= rf.commitIndex >= idx0 >= Snapshot.LastIncludeIndex

				if rf.lastApplied < idx0 {
					panic("rf.lastApplied < idx0")
				}
				rf.commitIndex = cmtIdx

				if cmtIdx > rf.lastApplied {
					for i := rf.lastApplied + 1; i <= cmtIdx; i++ {

						applyCh <- ApplyMsg{
							UseSnapshot:  false,
							CommandValid: true,
							Command:      rf.log[i-idx0].Data,
							CommandIndex: rf.log[i-idx0].Index,
						}
					}
					rf.lastApplied = cmtIdx
				}

				// if rf.commitIndex-rf.log[0].Index > 10 {
				// 	go rf.MakeSnapshot(&Snapshot{
				// 		LastIncludedIndex: rf.commitIndex,
				// 		LastIncludedTerm:  rf.log[rf.commitIndex-rf.log[0].Index].Term,
				// 		data:              nil,
				// 	})
				// }
				rf.mu.Unlock()
			}
		}
	}()

	// initialize from state persisted before a crash

	return rf
}

// only restart your election timer if
// a) you get an AppendEntries RPC from the current leader (i.e.,
// 	if the term in the AppendEntries arguments is outdated,
// 	you should not reset your timer);
// b) you are starting an election; or
// c) you grant a vote to another peer.

func init() {
	if !flag.Parsed() {
		flag.Parse()
	}
}
