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

	"labgob"
	"labrpc"
	"sync/atomic"

	"math/rand"
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
	Data   []byte
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
	logChan       chan LogEntry
	leaderCh      chan struct{}
	voteCh        chan AppendEntriesReply
	state         int
	electDuration time.Duration
	winChan       chan struct{}
	curTerm       int
	voteFor       int
	voteCount     int32
	log           []LogEntry
	logMu         sync.Mutex

	commitIndex int
	// lastApplied int // used for application
	// just for leader
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	DPrintf("%d peer, curTerm:%d, state:%t.\n", rf.me, rf.curTerm, rf.state == stateLeader)
	return rf.curTerm, rf.state == stateLeader
}

func (rf *Raft) lastIndex() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Index
}
func (rf *Raft) lastTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
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
	e := labgob.NewEncoder(w)
	e.Encode(rf.peers)
	e.Encode(rf.me)
	e.Encode(rf.curTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	e.Encode(rf.nextIndex)
	e.Encode(rf.matchIndex)

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

	if d.Decode(&rf.peers) != nil ||
		d.Decode(&rf.me) != nil ||
		d.Decode(&rf.curTerm) != nil ||
		d.Decode(&rf.voteFor) != nil ||
		d.Decode(&rf.log) != nil ||
		d.Decode(&rf.nextIndex) != nil ||
		d.Decode(&rf.matchIndex) != nil {
		panic("decode err")
	} else {

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
	// rf.curTerm >= lastLog.Term
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false

	reply.Term = rf.curTerm

	if args.Term < rf.curTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.curTerm {
		rf.state = stateFollower
		rf.curTerm = args.Term
		// -1 or args.CandidateID
		rf.voteFor = args.CandidateID
	}

	if rf.voteFor == -1 || rf.voteFor == args.CandidateID {
		if args.LastLogIndex > rf.lastIndex() {
			reply.VoteGranted = true
		}
		if args.LastLogIndex == rf.lastIndex() &&
			args.LastLogTerm >= rf.lastTerm() {
			reply.VoteGranted = true
		}
	}

	if reply.VoteGranted {
		rf.state = stateFollower
		rf.voteFor = args.CandidateID
		rf.leaderCh <- struct{}{}
		DPrintf("%d leaderCh<-.\n", rf.me)
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
	if rf.state != stateCandidate {
		return
	}
	if args.Term != term {
		return
	}
	if reply.Term > term {
		rf.curTerm = reply.Term
		rf.state = stateFollower
		rf.voteFor = -1
	}
	if reply.VoteGranted {
		cnt := atomic.AddInt32(&rf.voteCount, 1)
		DPrintf("%d get voted from %d, now is %d.\n", rf.me, server, cnt)
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
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.curTerm
	if args.Term < rf.curTerm {
		reply.Success = false
		return
	}
	if args.PrevLogIndex < len(rf.log) &&
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	rf.leaderCh <- struct{}{}
	DPrintf("%d recv msg from leader %d.\n", rf.me, args.LeaderID)
	// now args.prevLogIndex < len(rf.log)
	for _, e := range args.Entries {
		idx := e.Index
		if idx >= len(rf.log) {
			break
		}
		l := rf.log[idx]
		if l.Term != e.Term {
			rf.log = rf.log[:idx]
			break
		}
	}
	rf.log = append(rf.log, args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
	}
}
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (ok bool) {
	ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}

	// ignore out-of-date info
	if rf.state != stateLeader {
		return
	}
	if args.Term != rf.curTerm {
		return
	}

	if reply.Term > rf.curTerm {
		rf.state = stateFollower
		rf.voteFor = -1
		return
	}

	if !reply.Success {
		rf.nextIndex[server]--
		return
	}

	if len(args.Entries) == 0 {
		return
	}
	// update nextIndex and matchIndex
	rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
	rf.matchIndex[server] = rf.nextIndex[server] - 1

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1

	// Your code here (2B).
	isLeader := (rf.state == stateLeader)
	if !isLeader {
		return 0, 0, false
	}

	args := &AppendEntriesArgs{}
	args.Entries = []LogEntry{
		LogEntry{},
	}
	args.LeaderID = rf.me

	reply := &AppendEntriesReply{}
	rf.AppendEntries(args, reply)
	return index, term, isLeader
}

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
	rf.electDuration = time.Duration((rand.Intn(150) + 150)) * time.Millisecond
	rf.leaderCh = make(chan struct{}, 100)
	rf.winChan = make(chan struct{}, 100)
	// Your initialization code here (2A, 2B, 2C).

	go func() {
		for {
			switch rf.state {
			case stateFollower:
				select {
				case <-rf.leaderCh:
				case <-time.After(rf.electDuration):
					rf.state = stateCandidate
				}
			case stateCandidate:
				rf.curTerm++
				rf.voteFor = rf.me
				atomic.StoreInt32(&rf.voteCount, 1)

				// Temporarily not retry, do it later
				// replied := make([]bool, len(rf.peers))
				// var wg sync.WaitGroup
				for i := range rf.peers {
					if rf.me == i {
						continue
					}
					// if replied[i] {
					// 	continue
					// }
					// wg.Add(1)
					go func(t int) {
						reply := &RequestVoteReply{}
						arg := &RequestVoteArgs{
							Term:         rf.curTerm,
							CandidateID:  rf.me,
							LastLogIndex: rf.lastIndex(),
							LastLogTerm:  rf.lastTerm(),
						}
						ok := rf.sendRequestVote(t, arg, reply)
						_ = ok
						// if ok {
						// 	replied[t] = true
						// }
						// wg.Done()
					}(i)
				}
				// wg.Wait()
				select {
				case <-rf.leaderCh:
					// become follower
					DPrintf("%d become follwer of.\n", rf.me)
					rf.state = stateFollower
				case <-rf.winChan:
					rf.state = stateLeader
					DPrintf("%d become leader.\n", rf.me)
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = rf.lastIndex() + 1
						rf.matchIndex[i] = 0
					}
				case <-time.After(rf.electDuration):
					// new election cycle
				}

			case stateLeader:
				DPrintf("leader %d heart beat", rf.me)
				for i := range rf.peers {
					if rf.me == i {
						continue
					}
					// if replied[i] {
					// 	continue
					// }
					// wg.Add(1)
					go func(t int) {
						reply := &AppendEntriesReply{}
						args := &AppendEntriesArgs{
							Term:     rf.curTerm,
							LeaderID: rf.me,
						}
						ok := rf.sendAppendEntries(t, args, reply)
						_ = ok
						// if ok {
						// 	replied[t] = true
						// }
						// wg.Done()
					}(i)
				}
				time.Sleep(heaetBeatInterval)
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// only restart your election timer if
// a) you get an AppendEntries RPC from the current leader (i.e.,
// 	if the term in the AppendEntries arguments is outdated,
// 	you should not reset your timer);
// b) you are starting an election; or
// c) you grant a vote to another peer.
