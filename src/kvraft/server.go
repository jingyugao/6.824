package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Kind  string
	Key   string
	Value string
	ID    int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big
	waiterLock   map[int]*sync.Mutex
	waiter       map[int]*sync.Cond
	// Your definitions here.
	data map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()

	wal := &Op{Kind: "Get", Key: args.Key, ID: args.ID}
	idx, _, isLeader := kv.rf.Start(wal)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	kv.waiterLock[idx] = new(sync.Mutex)
	kv.waiter[idx] = sync.NewCond(kv.waiterLock[idx])
	kv.mu.Unlock()

	kv.waiterLock[idx].Lock()
	kv.waiter[idx].Wait()

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	wal := &Op{Kind: "Get", Key: args.Key, ID: args.ID}
	idx, _, isLeader := kv.rf.Start(wal)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	kv.waiterLock[idx] = new(sync.Mutex)
	kv.waiter[idx] = sync.NewCond(kv.waiterLock[idx])
	kv.mu.Unlock()

	kv.waiterLock[idx].Lock()
	kv.waiter[idx].Wait()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.waiter = make(map[int]*sync.Cond)
	kv.waiterLock = make(map[int]*sync.Locker)

	// You may need initialization code here.

	return kv
}
