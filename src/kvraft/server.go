package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"net/http"
	_ "net/http/pprof"
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
	waiter       map[int64]chan interface{}
	db           *DB
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("KVServer %d Get req %+v\n", kv.me, args)
	defer DPrintf("KVServer %d Get reply %+v\n", kv.me, reply)
	kv.mu.Lock()
	wal := Op{Kind: "Get", Key: args.Key, ID: args.ID, ReqID: args.ReqID}
	idx, _, isLeader := kv.rf.Start(wal)
	DPrintf("KVServer %d Get req idx:%d, %+v\n", kv.me, idx, args)

	if !isLeader {
		reply.WrongLeader = true
		leader := kv.rf.Leader()
		kv.mu.Unlock()
		DPrintf("KVServer %d Get %+v Leader not me but %d \n", kv.me, args, leader)
		return
	}
	if _, ok := kv.waiter[args.ReqID]; !ok {
		kv.waiter[args.ReqID] = make(chan interface{}, 100)
	}
	ch := kv.waiter[args.ReqID]
	kv.mu.Unlock()
	reply.ID = args.ID
	reply.ReqID = args.ReqID
	select {
	case i := <-ch:
		p, ok := i.(pair)
		if !ok {
			reply.Err = ErrTimeout
			return
		}
		if !p.ok {
			reply.Err = ErrNoKey
			return
		}
		reply.Value = p.v
		return
	case <-time.After(1000 * time.Millisecond):
		DPrintf("timeout\n")
		reply.Err = ErrTimeout
		return
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	defer DPrintf("KVServer %d PutAppend reply %+v.\n", kv.me, reply)
	kv.mu.Lock()
	wal := Op{Kind: args.Op, Key: args.Key, Value: args.Value, ID: args.ID, ReqID: args.ReqID}
	idx, _, isLeader := kv.rf.Start(wal)
	DPrintf("KVServer %d PutAppend req idx:%d, %+v.\n", kv.me, idx, args)

	if !isLeader {
		reply.WrongLeader = true
		leader := kv.rf.Leader()
		kv.mu.Unlock()
		DPrintf("KVServer %d Get %+v Leader not me but %d \n", kv.me, args, leader)
		return
	}
	if _, ok := kv.waiter[args.ReqID]; !ok {
		kv.waiter[args.ReqID] = make(chan interface{}, 100)
	}
	ch := kv.waiter[args.ReqID]
	kv.mu.Unlock()
	reply.ID = args.ID
	reply.ReqID = args.ReqID
	select {
	case v := <-ch:
		if _, ok := v.(int); !ok {
			reply.Err = ErrTimeout
		}
		return
	case <-time.After(1000 * time.Millisecond):
		DPrintf("timeout\n")
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
	kv.waiter = make(map[int64]chan interface{})
	kv.ack = make(map[int64]int64)
	go func() {
		for {
			select {
			case applyMsg := <-kv.applyCh:
				DPrintf("get applyMsg of %+v\n", applyMsg)
				if applyMsg.CommandIndex == 0 {
					continue
				}
				// reply to client

				// apply for SM
				op, ok := applyMsg.Command.(Op)
				if !ok {
					continue
					// panic()
				}
				kv.mu.Lock()
				if _, ok := kv.waiter[op.ReqID]; !ok {
					kv.waiter[op.ReqID] = make(chan interface{}, 100)
				}
				ch := kv.waiter[op.ReqID]
				val, ok := kv.ack[op.ReqID]
				if ok && val <= op.ReqID {
					// go func() { ch <- false }()
					kv.mu.Unlock()
					continue
				}
				kv.ack[op.ReqID] = op.ReqID
				kv.mu.Unlock()

				switch op.Kind {
				case "Get":
					v, ok := kv.db.Get(op.Key)

					ch <- pair{ok: ok, v: v}

				case "Put":
					kv.db.Put(op.Key, op.Value)
					ch <- 1
				case "Append":
					kv.db.Append(op.Key, op.Value)
					ch <- 1
				default:
					DPrintf("err op kind :%s\n", op.Kind)
					panic("err op")
				}

			}
		}
	}()

	return kv
}

func init() {
	go http.ListenAndServe("0.0.0.0:6060", nil)

}
