package raft

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"time"
)

// //
// // this is an outline of the API that raft must expose to
// // the service (or tester). see comments below for
// // each of these functions for more details.
// //
// // rf = Make(...)
// //   create a new Raft server.
// // rf.Start(command interface{}) (index, term, isleader)
// //   start agreement on a new log entry
// // rf.GetState() (term, isLeader)
// //   ask a Raft for its current term, and whether it thinks it is leader
// // ApplyMsg
// //   each time a new entry is committed to the log, each Raft peer
// //   should send an ApplyMsg to the service (or tester)
// //   in the same server.
// //

// import (
// 	//"fmt"
// 	"bytes"
// 	"encoding/gob"
// 	"labrpc"
// 	"math/rand"
// 	"sync"
// 	"time"
// )

// const (
// 	stateLeader = iota
// 	stateCandidate
// 	stateFollower

// 	HBINTERVAL = 50 * time.Millisecond // 50ms
// )

// //
// // as each Raft peer becomes aware that successive log entries are
// // committed, the peer should send an ApplyMsg to the service (or
// // tester) on the same server, via the applyCh passed to Make().
// //
// type ApplyMsg struct {
// 	CommandValID bool
// 	CommandIndex int
// 	Command      interface{}
// 	UseSnapshot  bool   // ignore for lab2; only used in lab3
// 	Snapshot     []byte // ignore for lab2; only used in lab3
// }

// type LogEntry struct {
// 	LogIndex int
// 	LogTerm  int
// 	LogComd  interface{}
// }

// //
// // A Go object implementing a single Raft peer.
// //
// type Raft struct {
// 	mu        sync.Mutex
// 	peers     []*labrpc.ClientEnd
// 	persister *Persister
// 	me        int // index into peers[]

// 	// Your data here.
// 	// Look at the paper's Figure 2 for a description of what
// 	// state a Raft server must maintain.

// 	//channel
// 	state         int
// 	voteCount     int
// 	chanCommit    chan bool
// 	beatCh chan bool
// 	chanGrantVote chan bool
// 	chanLeader    chan bool
// 	applyCh     chan ApplyMsg

// 	//persistent state on all server
// 	curTerm int
// 	voteFor     int
// 	log         []LogEntry

// 	//volatile state on all servers
// 	commitIndex int
// 	lastApplied int

// 	//volatile state on leader
// 	nextIndex  []int
// 	matchIndex []int
// }

// // return curTerm and whether this server
// // believes it is the leader.
// func (rf *Raft) GetState() (int, bool) {
// 	return rf.curTerm, rf.state == stateLeader
// }

// func (rf *Raft) lastIndex() int {
// 	return rf.log[len(rf.log)-1].Index
// }
// func (rf *Raft) lastTerm() int {
// 	return rf.log[len(rf.log)-1].Term
// }
// func (rf *Raft) IsLeader() bool {
// 	return rf.state == stateLeader
// }

// // save Raft's persistent state to stable storage,
// // where it can later be retrieved after a crash and restart.
// // see paper's Figure 2 for a description of what should be persistent.
// //
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.curTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readSnapshot(data []byte) {

	rf.readPersist(rf.persister.ReadRaftState())

	if len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var LastIncludedIndex int
	var LastIncludedTerm int

	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)

	rf.commitIndex = LastIncludedIndex
	rf.lastApplied = LastIncludedIndex

	rf.log = truncateLog(LastIncludedIndex, LastIncludedTerm, rf.log)

	msg := ApplyMsg{CommandValid: true, UseSnapshot: true, Snapshot: data}

	go func() {
		rf.applyCh <- msg
	}()
}

// //
// // restore previously persisted state.
// //
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.curTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.log)
}

// //
// // example RequestVote RPC arguments structure.
// //
// type RequestVoteArgs struct {
// 	// Your data here.
// 	Term         int
// 	CandidateID  int
// 	LastLogTerm  int
// 	LastLogIndex int
// }

// //
// // example RequestVote RPC reply structure.
// //
// type RequestVoteReply struct {
// 	// Your data here.
// 	Term        int
// 	VoteGranted bool
// }

// type AppendEntriesArgs struct {
// 	// Your data here.
// 	Term         int
// 	LeaderID     int
// 	PrevLogTerm  int
// 	PrevLogIndex int
// 	Entries      []LogEntry
// 	LeaderCommit int
// }

// type AppendEntriesReply struct {
// 	// Your data here.
// 	Term      int
// 	Success   bool
// 	NextIndex int
// }

// //
// // example RequestVote RPC handler.
// //
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false
	if args.Term < rf.curTerm {
		reply.Term = rf.curTerm
		//fmt.Printf("%v curTerm:%v vote reject for:%v term:%v",rf.me,rf.curTerm,args.CandidateID,args.Term)
		return
	}

	if args.Term > rf.curTerm {
		rf.curTerm = args.Term
		rf.state = stateFollower
		rf.voteFor = -1
	}
	reply.Term = rf.curTerm

	term := rf.lastTerm()
	index := rf.lastIndex()
	// := moreUpToDate(rf.lastIndex(), rf.lastTerm(), args.LastLogIndex, args.LastLogTerm)
	uptoDate := false

	if args.LastLogTerm > term {
		uptoDate = true
	}

	if args.LastLogTerm == term && args.LastLogIndex >= index { // at least up to date
		uptoDate = true
	}

	if (rf.voteFor == -1 || rf.voteFor == args.CandidateID) && uptoDate {
		rf.beatCh <- struct{}{}
		rf.state = stateFollower
		reply.VoteGranted = true
		rf.voteFor = args.CandidateID
		//fmt.Printf("%v curTerm:%v vote for:%v term:%v",rf.me,rf.curTerm,args.CandidateID,args.Term)
	}
	//fmt.Printf("%v curTerm:%v vote reject for:%v term:%v\n",rf.me,rf.curTerm,args.CandidateID,args.Term)
}

func (rf *Raft) AppendEntries2(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false
	if args.Term < rf.curTerm {
		reply.Term = rf.curTerm
		reply.NextIndex = rf.lastIndex() + 1
		//	fmt.Printf("%v curTerm: %v rejected %v:%v\n",rf.me,rf.curTerm,args.LeaderID,args.Term)
		return
	}
	rf.beatCh <- struct{}{}
	//fmt.Printf("%d respond for %v\n",rf.me,args.LeaderID)
	if args.Term > rf.curTerm {
		rf.curTerm = args.Term
		rf.state = stateFollower
		rf.voteFor = -1
	}
	reply.Term = args.Term

	if args.PrevLogIndex > rf.lastIndex() {
		reply.NextIndex = rf.lastIndex() + 1
		return
	}

	baseIndex := rf.log[0].Index

	if args.PrevLogIndex > baseIndex {
		term := rf.log[args.PrevLogIndex-baseIndex].Term
		if args.PrevLogTerm != term {
			for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
				if rf.log[i-baseIndex].Term != term {
					reply.NextIndex = i + 1
					break
				}
			}
			return
		}
	}
	/*else {
		//fmt.Printf("????? len:%v\n",len(args.Entries))
		last := rf.lastIndex()
		elen := len(args.Entries)
		for i := 0; i < elen ;i++ {
			if args.PrevLogIndex + i > last || rf.log[args.PrevLogIndex + i].Term != args.Entries[i].Term {
				rf.log = rf.log[: args.PrevLogIndex+1]
				rf.log = append(rf.log, args.Entries...)
				app = false
				fmt.Printf("?????\n")
				break
			}
		}
	}*/
	if args.PrevLogIndex < baseIndex {

	} else {
		rf.log = rf.log[:args.PrevLogIndex+1-baseIndex]
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true
		reply.NextIndex = rf.lastIndex() + 1
	}
	//println(rf.me,rf.lastIndex(),reply.NextIndex,rf.log)
	if args.LeaderCommit > rf.commitIndex {
		last := rf.lastIndex()
		if args.LeaderCommit > last {
			rf.commitIndex = last
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.commitCh <- rf.commitIndex
	}
	return
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		term := rf.curTerm
		if rf.state != stateCandidate {
			return ok
		}
		if args.Term != term {
			return ok
		}
		if reply.Term > term {
			rf.curTerm = reply.Term
			rf.state = stateFollower
			rf.voteFor = -1
			rf.persist()
		}
		if reply.VoteGranted {
			rf.voteCount++
			if rf.state == stateCandidate && rf.voteCount > len(rf.peers)/2 {
				rf.state = stateFollower
				rf.winChan <- struct{}{}
			}
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries2(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != stateLeader {
			return ok
		}
		if args.Term != rf.curTerm {
			return ok
		}

		if reply.Term > rf.curTerm {
			rf.curTerm = reply.Term
			rf.state = stateFollower
			rf.voteFor = -1
			rf.persist()
			return ok
		}
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
				//reply.NextIndex
				//rf.nextIndex[server] = reply.NextIndex
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			rf.nextIndex[server] = reply.NextIndex
		}
	}
	return ok
}

// ------------------------------------------------------

func (rf *Raft) MakeSnapshot(index int, snapshot []byte) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].Index
	lastIndex := rf.lastIndex()

	if index <= baseIndex || index > lastIndex {
		// in case having installed a snapshot from leader before snapshotting
		// second condition is a hack
		return
	}

	var newLogEntries []LogEntry

	newLogEntries = append(newLogEntries, LogEntry{Index: index, Term: rf.log[index-baseIndex].Term})

	for i := index + 1; i <= lastIndex; i++ {
		newLogEntries = append(newLogEntries, rf.log[i-baseIndex])
	}

	rf.log = newLogEntries

	rf.persist()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(newLogEntries[0].Index)
	e.Encode(newLogEntries[0].Term)

	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveSnapshot(data)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.curTerm {
		reply.Term = rf.curTerm
		return
	}
	rf.beatCh <- struct{}{}
	rf.state = stateFollower
	rf.curTerm = rf.curTerm

	rf.persister.SaveSnapshot(args.Data)

	rf.log = truncateLog(args.LastIncludedIndex, args.LastIncludedTerm, rf.log)

	msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}

	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex

	rf.persist()

	rf.applyCh <- msg
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok {
		if reply.Term > rf.curTerm {
			rf.curTerm = reply.Term
			rf.state = stateFollower
			rf.voteFor = -1
			return ok
		}

		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// // the first return value is the index that the command will appear at
// // if it's ever committed. the second return value is the current
// // term. the third return value is true if this server believes it is
// // the leader.
// //
// func (rf *Raft) Start(command interface{}) (int, int, bool) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	index := -1
// 	term := rf.curTerm
// 	isLeader := rf.state == stateLeader
// 	if isLeader {
// 		index = rf.lastIndex() + 1
// 		//fmt.Printf("raft:%d start\n",rf.me)
// 		rf.log = append(rf.log, LogEntry{Term: term, LogComd: command, Index: index}) // append new entry from client
// 		rf.persist()
// 	}
// 	return index, term, isLeader
// }

// //
// // the tester calls Kill() when a Raft instance won't
// // be needed again. you are not required to do anything
// // in Kill(), but it might be convenient to (for example)
// // turn off debug output from this instance.
// //
// func (rf *Raft) Kill() {
// 	// Your code here, if desired.
// }

// func (rf *Raft) boatcastRequestVote() {
// 	var args RequestVoteArgs
// 	rf.mu.Lock()
// 	args.Term = rf.curTerm
// 	args.CandidateID = rf.me
// 	args.LastLogTerm = rf.lastTerm()
// 	args.LastLogIndex = rf.lastIndex()
// 	rf.mu.Unlock()

// 	for i := range rf.peers {
// 		if i != rf.me && rf.state == stateCandidate {
// 			go func(i int) {
// 				var reply RequestVoteReply
// 				//fmt.Printf("%v RequestVote to %v\n",rf.me,i)
// 				rf.sendRequestVote(i, args, &reply)
// 			}(i)
// 		}
// 	}
// }

func (rf *Raft) boatcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	N := rf.commitIndex
	last := rf.lastIndex()
	baseIndex := rf.log[0].Index
	for i := rf.commitIndex + 1; i <= last; i++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i-baseIndex].Term == rf.curTerm {
				num++
			}
		}
		if 2*num > len(rf.peers) {
			N = i
		}
	}
	if N != rf.commitIndex {
		rf.commitIndex = N
		rf.commitCh <- N
	}

	for i := range rf.peers {
		if i != rf.me && rf.state == stateLeader {

			//copy(args.Entries, rf.log[args.PrevLogIndex + 1:])

			if rf.nextIndex[i] > baseIndex {
				var args AppendEntriesArgs
				args.Term = rf.curTerm
				args.LeaderID = rf.me
				args.PrevLogIndex = rf.nextIndex[i] - 1
				//	fmt.Printf("baseIndex:%d PrevIndex:%d\n",baseIndex,args.PrevLogIndex )
				args.PrevLogTerm = rf.log[args.PrevLogIndex-baseIndex].Term
				//args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex + 1:]))
				args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex+1-baseIndex:]))
				copy(args.Entries, rf.log[args.PrevLogIndex+1-baseIndex:])
				args.LeaderCommit = rf.commitIndex
				go func(i int, args AppendEntriesArgs) {
					var reply AppendEntriesReply
					rf.sendAppendEntries(i, &args, &reply)
				}(i, args)
			} else {
				var args InstallSnapshotArgs
				args.Term = rf.curTerm
				args.LeaderID = rf.me
				args.LastIncludedIndex = rf.log[0].Index
				args.LastIncludedTerm = rf.log[0].Term
				args.Data = rf.persister.snapshot
				go func(server int, args InstallSnapshotArgs) {
					reply := &InstallSnapshotReply{}
					rf.sendInstallSnapshot(server, &args, reply)
				}(i, args)
			}
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
	rf.me = me

	// Your initialization code here.
	rf.state = stateFollower
	rf.voteFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.curTerm = 0
	rf.commitCh = make(chan int, 100)
	rf.beatCh = make(chan struct{}, 100)

	rf.winChan = make(chan struct{}, 100)
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	go func() {
		for {
			switch rf.state {
			case stateFollower:
				select {
				case <-rf.beatCh:

				case <-time.After(time.Duration(rand.Int63()%333+550) * time.Millisecond):
					rf.state = stateCandidate
				}
			case stateLeader:
				//fmt.Printf("Leader:%v %v\n",rf.me,"boatcastAppendEntries	")
				rf.boatcastAppendEntries()
				time.Sleep(heaetBeatInterval)
			case stateCandidate:
				rf.mu.Lock()
				rf.curTerm++
				rf.voteFor = rf.me
				rf.voteCount = 1
				rf.persist()
				rf.mu.Unlock()
				go rf.boatcastRequestVote()
				//fmt.Printf("%v become CANDIDATE %v\n",rf.me,rf.curTerm)
				select {
				case <-time.After(time.Duration(rand.Int63()%333+550) * time.Millisecond):
				case <-rf.beatCh:
					rf.state = stateFollower
				//	fmt.Printf("CANDIDATE %v reveive beatCh\n",rf.me)
				case <-rf.winChan:
					rf.mu.Lock()
					rf.state = stateLeader
					//fmt.Printf("%v is Leader\n",rf.me)
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = rf.lastIndex() + 1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
					//rf.boatcastAppendEntries()
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-rf.commitCh:
				rf.mu.Lock()
				commitIndex := rf.commitIndex
				baseIndex := rf.log[0].Index
				for i := rf.lastApplied + 1; i <= commitIndex; i++ {
					msg := ApplyMsg{CommandValid: true, CommandIndex: i, Command: rf.log[i-baseIndex].Data}
					applyCh <- msg
					//fmt.Printf("me:%d %v\n",rf.me,msg)
					rf.lastApplied = i
				}
				rf.mu.Unlock()
			}
		}
	}()
	return rf
}
