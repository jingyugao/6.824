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
	"math/rand"

	"labrpc"
	"sync/atomic"

	"sync"
	"time"
)

// import "bytes"
// import "labgob"

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

// is this up-to-date to that?
func (this *LogEntry) isUpToDate(that *LogEntry) bool {
	if this.Term > that.Term {
		return true
	}
	if this.Term == that.Term &&
		this.Index >= that.Index {
		return true
	}

	return false
}

const (
	stateLeader    = 1
	stateCandidate = 2
	stateFollower  = 3

	heaetBeatInterval = 50 * time.Millisecond
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	cmdCh         chan interface{}
	beatCh        chan struct{}
	voteCh        chan struct{}
	state         int32
	electDuration time.Duration
	winChan       chan struct{}
	curTerm       int
	voteFor       int
	voteCount     int32
	log           []LogEntry
	logMu         sync.Mutex
	commitCh      chan int
	commitIndex   int
	lastApplied   int // used for application
	// just for leader
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	DPrintf("%d peer, curTerm:%d, state:%d.\n", rf.me, rf.curTerm, rf.state)
	return rf.curTerm, rf.state == stateLeader
}

func (rf *Raft) lastIndex() int {

	return rf.log[len(rf.log)-1].Index
}
func (rf *Raft) lastTerm() int {

	return rf.log[len(rf.log)-1].Term
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
	e.Encode(rf.log)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
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
	d := gob.NewDecoder(r)

	d.Decode(&rf.curTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.log)

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
	// rf.curTerm >= lastLog.Term
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer func() {
	// 	DPrintf("%d recv ReqVote from %d and vote %v %d.\n", rf.me, args.CandidateID, reply.VoteGranted, reply.Term)
	// }()
	reply.VoteGranted = false
	// If RPC request or response contains term T > currentTerm:
	//  set currentTerm = T, convert to follower

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
	if !uptodate {
		return
	}

	if args.Term > rf.curTerm {
		rf.curTerm = args.Term
		rf.state = stateFollower
		rf.voteFor = -1
		rf.beatCh <- struct{}{}
	}
	reply.Term = rf.curTerm

	// Reply false if term < currentTerm
	if args.Term < rf.curTerm {
		reply.Term = rf.curTerm
		return
	}

	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateID) && uptodate {
		reply.VoteGranted = true
		rf.state = stateFollower
		rf.voteFor = args.CandidateID
		rf.voteCh <- struct{}{}
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
	// defer func() {
	// 	DPrintf("%d sendVote to %d,%v", rf.me, server, ok)
	// }()
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
		rf.curTerm = reply.Term
		rf.state = stateFollower
		rf.voteFor = -1
		rf.beatCh <- struct{}{}
	}

	// out-of-date response
	if rf.state != stateCandidate || args.Term != term {
		return
	}

	if reply.VoteGranted {
		cnt := atomic.AddInt32(&rf.voteCount, 1)
		// DPrintf("%d get voted from %d, now is %d.\n", rf.me, server, cnt)
		if rf.state == stateCandidate && int(cnt) > len(rf.peers)/2 {
			DPrintf("%d wins in this election", rf.me)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.curTerm
	reply.Success = false

	// RPC request or response contains term T > currentTerm:
	// set currentTerm = T
	if args.Term > rf.curTerm {
		rf.curTerm = args.Term
		rf.state = stateFollower
		rf.voteFor = -1
		rf.beatCh <- struct{}{}
	}

	// AppendEntries RPC received from new leader: convert to
	// folower
	if rf.state == stateCandidate {
		rf.state = stateFollower
		rf.voteFor = -1
		rf.curTerm = max(args.Term, rf.lastTerm())
		rf.beatCh <- struct{}{}
	}

	// receiving AppendEntries RPC from current leader
	if args.Term == rf.curTerm && args.LeaderID == rf.voteFor {
		rf.beatCh <- struct{}{}
	}

	// 1. Reply false if term < currentTerm
	if args.Term < rf.curTerm {
		return
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.log) ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	// DPrintf("%d recv Entrie form %d and match (%d = %d).\n", rf.me, args.LeaderID, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)

	reply.Success = true
	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	// 4. Append any new entries not already in the log

	old := rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	DPrintf("log of %d append \n%v with \n%v\n ret = %v .\n", rf.me, old, args, rf.log)

	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		DPrintf("update local cmtIdx of %d from %d to min(%d,%d) .\n", rf.me, rf.commitIndex, args.LeaderCommit, rf.lastIndex())
		old := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, rf.lastIndex())
		if rf.commitIndex > old {
			rf.commitCh <- rf.commitIndex
		}
	}
}
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (ok bool) {

	// DPrintf("%d sendAppend to %d,%v", rf.me, server, ok)

	ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// RPC request or response contains term T > currentTerm:
	//  set currentTerm = T
	if reply.Term > rf.curTerm {
		rf.curTerm = reply.Term
		rf.state = stateFollower
		rf.voteFor = -1
		rf.beatCh <- struct{}{}
	}

	// ignore out-of-date response
	if rf.state != stateLeader || args.Term != rf.curTerm {
		return
	}

	if len(args.Entries) == 0 {
		return
	}

	if !reply.Success {
		// rf.nextIndex[server]--
		// 这里不太明白
		idx := rf.nextIndex[server] - 1
		for idx > 1 && rf.log[idx].Term == args.PrevLogTerm {
			idx--
		}

		rf.nextIndex[server] = idx
		return
	}

	// update nextIndex and matchIndex
	DPrintf("%d update nextIndex of %d from %d to %d\n", rf.me, server, rf.nextIndex[server], args.Entries[len(args.Entries)-1].Index+1)
	rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
	rf.matchIndex[server] = rf.nextIndex[server] - 1

	N := rf.lastIndex()
	for N > rf.commitIndex {
		agreeCnt := 0
		// a majority of matchIndex[i] ≥ N
		for i := range rf.matchIndex {
			if rf.matchIndex[i] >= N || rf.me == i {
				agreeCnt++
			}
		}
		// and log[N].term == currentTerm
		if agreeCnt > len(rf.peers)/2 && rf.log[N].Term == rf.curTerm {
			DPrintf("commit %d because of (%d >,%d) and (%d = %d)\n", N, agreeCnt, len(rf.peers)/2, rf.log[N].Term, rf.curTerm)
			rf.commitIndex = N
			rf.commitCh <- N
			break
		} else {
			// DPrintf("cant commit %d because of (%d !>,%d) or (%d != %d)\n", N, agreeCnt, len(rf.peers)/2, rf.log[N].Term, rf.curTerm)
		}
		N--
	}

	return
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

	// Your code here (2B).
	isLeader := (rf.state == stateLeader)
	if !isLeader {
		return 0, 0, false
	}

	rf.mu.Lock()

	idx := rf.lastIndex()
	log := LogEntry{
		Index:  idx + 1,
		Term:   rf.curTerm,
		Leader: rf.me,
		Data:   command,
	}
	DPrintf("start a new log of idx=%d with %v.\n", idx+1, command)
	rf.log = append(rf.log, log)
	rf.mu.Unlock()

	// for i := 0; i < len(rf.peers); i++ {
	// 	if i == rf.me {
	// 		continue
	// 	}
	// 	go func(t int) {
	// 		args := &AppendEntriesArgs{}
	// 		args.Entries = rf.log[rf.nextIndex[t]:]
	// 		args.Term = rf.curTerm
	// 		args.LeaderID = rf.me
	// 		args.PrevLogIndex = rf.nextIndex[t] - 1
	// 		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	// 		args.LeaderCommit = rf.commitIndex
	// 		reply := &AppendEntriesReply{}
	// 		rf.sendAppendEntries(t, args, reply)
	// 	}(i)
	// }

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
	rf.cmdCh = make(chan interface{}, 100)
	rf.beatCh = make(chan struct{}, 100)
	rf.winChan = make(chan struct{}, 100)
	rf.voteCh = make(chan struct{}, 100)
	rf.commitCh = make(chan int, 100)
	rf.electDuration = time.Duration((rand.Intn(150) + 150)) * time.Millisecond
	// Your initialization code here (2A, 2B, 2C).
	rf.readPersist(persister.ReadRaftState())
	go func() {
		for {
			switch rf.state {
			case stateFollower:
				select {
				case <-rf.beatCh:
				case <-rf.voteCh:
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
				atomic.StoreInt32(&rf.voteCount, 1)

				// Send RequestVote RPCs to all other servers
				for i := range rf.peers {
					if rf.me == i {
						continue
					}
					reply := &RequestVoteReply{}
					arg := &RequestVoteArgs{
						Term:         rf.curTerm,
						CandidateID:  rf.me,
						LastLogIndex: rf.lastIndex(),
						LastLogTerm:  rf.lastTerm(),
					}
					go func(t int) {
						ok := rf.sendRequestVote(t, arg, reply)
						_ = ok
					}(i)
				}
				rf.mu.Unlock()

				select {
				case <-rf.beatCh:
					// become follower
					// DPrintf("%d become follwer of.\n", rf.me)
					// atomic.StoreInt32(&rf.state, stateFollower)
				case <-rf.winChan:
					rf.mu.Lock()

					DPrintf("%d become leader in term %d.\n", rf.me, rf.curTerm)
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = rf.lastIndex() + 1
						rf.matchIndex[i] = 0
					}
					rf.matchIndex[me] = len(rf.log) - 1
					rf.state = stateLeader
					rf.mu.Unlock()
					// rf.Start(nil)
					// atomic.StoreInt32(&rf.state, stateLeader)

					// election timeout elapses: start new election
				case <-time.After(rf.electDuration):
				}

			case stateLeader:
				// send initial empty AppendEntries RPCs (heartbeat) to each server
				// DPrintf("%d heart beat.\n", rf.me)

				for i := range rf.peers {
					if rf.me == i {
						continue
					}
					go func(t int) {
						reply := &AppendEntriesReply{}
						args := &AppendEntriesArgs{
							Term:         rf.curTerm,
							LeaderID:     rf.me,
							PrevLogIndex: rf.nextIndex[t] - 1,
							PrevLogTerm:  rf.log[rf.nextIndex[t]-1].Term,
							LeaderCommit: rf.commitIndex,
							Entries:      rf.log[rf.nextIndex[t]:],
						}
						ok := rf.sendAppendEntries(t, args, reply)
						_ = ok
					}(i)
				}
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
				DPrintf("commit idx:%d\n", cmtIdx)
				rf.mu.Lock()
				if cmtIdx > rf.lastApplied {
					for i := rf.lastApplied + 1; i <= cmtIdx; i++ {
						DPrintf("peer %d apply msg of %d, %+v", rf.me, i, rf.log[i].Data)
						applyCh <- ApplyMsg{
							CommandValid: true,
							Command:      rf.log[i].Data,
							CommandIndex: i,
						}
					}
					rf.lastApplied = cmtIdx
				}
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
