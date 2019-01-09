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

type DB struct {
	mu   sync.RWMutex
	data map[string]string
}

func (db *DB) Put(k string, v string) {
	db.mu.Lock()
	db.data[k] = v
	db.mu.Unlock()
}

func (db *DB) Append(k string, v string) {
	db.mu.Lock()
	db.data[k] += v
	db.mu.Unlock()
}

func (db *DB) Get(k string) (val string, ok bool) {
	db.mu.RLock()
	val, ok = db.data[k]
	db.mu.RUnlock()

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

type pair struct {
	v  string
	ok bool
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	waiter map[int]chan interface{}
	db     *DB
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("KVServer %d Get req %+v\n", kv.me, args)
	defer DPrintf("KVServer %d Get reply %+v\n", kv.me, reply)

	kv.mu.Lock()

	wal := Op{Kind: "Get", Key: args.Key, ID: args.ID}
	idx, _, isLeader := kv.rf.Start(wal)
	if !isLeader {
		reply.WrongLeader = true
		leader := kv.rf.Leader()
		kv.mu.Unlock()

		DPrintf("KVServer %d Get %+v Leader not me but %d \n", kv.me, args, leader)
		return
	}

	ch := make(chan interface{})
	kv.waiter[idx] = ch
	kv.mu.Unlock()

	p, ok := (<-ch).(pair)
	if !ok {
		panic("err 	p, ok := (<-ch).(pair)")
	}

	reply.ID = args.ID
	if !p.ok {
		reply.Err = ErrNoKey
		return
	}
	reply.Value = p.v
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("KVServer %d PutAppend req %+v.\n", kv.me, args)
	defer DPrintf("KVServer %d PutAppend reply %+v.\n", kv.me, reply)

	kv.mu.Lock()

	wal := Op{Kind: args.Op, Key: args.Key, Value: args.Value, ID: args.ID}
	idx, _, isLeader := kv.rf.Start(wal)
	if !isLeader {
		reply.WrongLeader = true
		leader := kv.rf.Leader()
		kv.mu.Unlock()

		DPrintf("KVServer %d Get %+v Leader not me but %d \n", kv.me, args, leader)
		return
	}
	ch := make(chan interface{})
	kv.waiter[idx] = ch
	kv.mu.Unlock()

	<-ch
	reply.ID = args.ID
	return
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
	kv.db = &DB{data: make(map[string]string), mu: sync.RWMutex{}}
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.waiter = make(map[int]chan interface{})

	go func() {
		for {
			select {
			case applyMsg := <-kv.applyCh:
				DPrintf("get applyMsg of %+v\n", applyMsg)
				if applyMsg.CommandIndex == 0 {
					continue
				}
				// reply to client
				kv.mu.Lock()
				ch, waiting := kv.waiter[applyMsg.CommandIndex]
				kv.mu.Unlock()

				// apply for SM
				op, ok := applyMsg.Command.(Op)
				if !ok {
					continue
					// panic()
				}

				switch op.Kind {
				case "Get":
					v, ok := kv.db.Get(op.Key)
					if waiting {
						ch <- pair{v: v, ok: ok}
					}
				case "Put":
					kv.db.Put(op.Key, op.Value)
					if waiting {
						ch <- struct{}{}
					}
				case "Append":
					kv.db.Append(op.Key, op.Value)
					ch <- struct{}{}
				default:
					DPrintf("err op kind :%s\n", op.Kind)
					panic("err op")
				}

			}
		}
	}()

	return kv
}
