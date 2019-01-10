package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
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
	ReqID int64
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
	ack          map[int64]int64
	waiter       map[int]chan Op
	db           *DB
}

func (kv *KVServer) AppendEntryToLog(entry Op) bool {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	ch, ok := kv.waiter[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.waiter[index] = ch
	}
	kv.mu.Unlock()
	select {
	case op := <-ch:
		return op == entry
	case <-time.After(1000 * time.Millisecond):
		//log.Printf("timeout\n")
		return false
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("KVServer %d Get req %+v\n", kv.me, args)
	defer DPrintf("KVServer %d Get reply %+v\n", kv.me, reply)

	wal := Op{Kind: "Get", Key: args.Key, ID: args.ID, ReqID: args.ReqID}
	index, _, isLeader := kv.rf.Start(wal)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	ch, ok := kv.waiter[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.waiter[index] = ch
	}
	kv.mu.Unlock()
	select {
	case op := <-ch:
		if op != wal {
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value, _ = kv.db.Get(args.Key)
			kv.ack[args.ID] = args.ReqID
			kv.mu.Unlock()
		}
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrTimeout
		return
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	defer DPrintf("KVServer %d PutAppend reply %+v.\n", kv.me, reply)
	wal := Op{Kind: args.Op, Key: args.Key, Value: args.Value, ID: args.ID, ReqID: args.ReqID}
	index, _, isLeader := kv.rf.Start(wal)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	ch, ok := kv.waiter[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.waiter[index] = ch
	}
	kv.mu.Unlock()
	select {
	case op := <-ch:
		if op != wal {
			// log rewrite by other
			reply.WrongLeader = true
		} else {
			reply.Err = OK
		}
		return
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrTimeout
		return
	}

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
	kv.waiter = make(map[int]chan Op)
	kv.ack = make(map[int64]int64)
	go func() {
		for {
			select {
			case applyMsg := <-kv.applyCh:
				// DPrintf("get applyMsg of %+v\n", applyMsg)
				// // apply for SM
				op := applyMsg.Command.(Op)
				kv.mu.Lock()
				v, ok := kv.ack[op.ID]
				if !ok || v < op.ReqID {
					switch op.Kind {
					case "Get":
					case "Put":
						kv.db.Put(op.Key, op.Value)
					case "Append":
						kv.db.Append(op.Key, op.Value)
					default:
						DPrintf("err op kind :%s\n", op.Kind)
						panic("err op")
					}
					kv.ack[op.ID] = op.ReqID
				}

				if ch, ok := kv.waiter[applyMsg.CommandIndex]; !ok {
					kv.waiter[applyMsg.CommandIndex] = make(chan Op, 1)
				} else {
					select {
					case <-kv.waiter[applyMsg.CommandIndex]:
					default:
					}
					ch <- op
				}
				kv.mu.Unlock()
			}
		}
	}()

	return kv
}
