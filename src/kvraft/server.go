package raftkv

import (
	"bytes"
	"encoding/gob"
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
func (db *DB) Load(e *gob.Decoder) (err error) {
	db.mu.Lock()
	e.Decode(&db.data)
	db.mu.Unlock()
	return
}
func (db *DB) Store(e *gob.Encoder) (err error) {
	db.mu.RLock()
	err = e.Encode(db.data)
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
	// db           *DB
	db map[string]string
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
func (kv *KVServer) MakeSnapshot() (sp []byte) {

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.ack)
	e.Encode(kv.db)
	sp = w.Bytes()
	return
}
func (kv *KVServer) CheckDup(id int64, reqid int64) bool {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	v, ok := kv.ack[id]
	if ok {
		return v >= reqid
	}
	return false
}
func (kv *KVServer) LoadSnapshot(sp []byte) {
	if sp == nil || len(sp) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(sp)
	d := gob.NewDecoder(r)
	d.Decode(&kv.ack)
	d.Decode(&kv.db)
	return
}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("KVServer %d Get req %+v\n", kv.me, args)
	defer DPrintf("KVServer %d Get reply %+v\n", kv.me, reply)

	wal := Op{Kind: "Get", Key: args.Key, ID: args.ID, ReqID: args.ReqID}
	ok := kv.AppendEntryToLog(wal)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.WrongLeader = false

		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.db[args.Key]
		kv.ack[args.ID] = args.ReqID
		//log.Printf("%d get:%v value:%s\n",kv.me,entry,reply.Value)
		kv.mu.Unlock()
	}
	return
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
			reply.Value, _ = kv.db[args.Key]
			kv.ack[args.ID] = args.ReqID
			kv.mu.Unlock()
		}
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrTimeout
		return
	}

}
func (kv *KVServer) Apply(args Op) {
	switch args.Kind {
	case "Put":
		kv.db[args.Key] = args.Value
	case "Append":
		kv.db[args.Key] += args.Value
	}
	kv.ack[args.ID] = args.ReqID
}
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{Kind: args.Op, Key: args.Key, Value: args.Value, ID: args.ID, ReqID: args.ReqID}
	ok := kv.AppendEntryToLog(entry)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
}

func (kv *KVServer) PutAppend2(args *PutAppendArgs, reply *PutAppendReply) {
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
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.db = make(map[string]string)
	kv.ack = make(map[int64]int64)
	kv.waiter = make(map[int]chan Op)
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func() {
		for {
			msg := <-kv.applyCh
			if msg.UseSnapshot {
				var LastIncludedIndex int
				var LastIncludedTerm int

				r := bytes.NewBuffer(msg.Snapshot)
				d := gob.NewDecoder(r)

				kv.mu.Lock()
				d.Decode(&LastIncludedIndex)
				d.Decode(&LastIncludedTerm)
				kv.db = make(map[string]string)
				kv.ack = make(map[int64]int64)
				d.Decode(&kv.db)
				d.Decode(&kv.ack)
				kv.mu.Unlock()
			} else {
				op := msg.Command.(Op)
				kv.mu.Lock()
				if !kv.CheckDup(op.ID, op.ReqID) {
					kv.Apply(op)
				}

				ch, ok := kv.waiter[msg.CommandIndex]
				if ok {
					select {
					case <-kv.waiter[msg.CommandIndex]:
					default:
					}
					ch <- op
				} else {
					kv.waiter[msg.CommandIndex] = make(chan Op, 1)
				}

				//need snapshot
				if maxraftstate != -1 && kv.rf.RaftStateSize() > maxraftstate {
					w := new(bytes.Buffer)
					e := gob.NewEncoder(w)
					e.Encode(kv.db)
					e.Encode(kv.ack)
					data := w.Bytes()
					go kv.rf.MakeSnapshot(msg.CommandIndex, data)
				}
				kv.mu.Unlock()
			}
		}
	}()

	return kv
}
func StartKVServer2(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.waiter = make(map[int]chan Op)
	kv.ack = make(map[int64]int64)
	go func() {
		for {
			select {
			case applyMsg := <-kv.applyCh:
				// DPrintf("get applyMsg of %+v\n", applyMsg)

				if !applyMsg.UseSnapshot {
					// // apply for SM
					op := applyMsg.Command.(Op)
					kv.mu.Lock()
					if !kv.CheckDup(op.ID, op.ReqID) {
						kv.Apply(op)
					}
					ch, ok := kv.waiter[applyMsg.CommandIndex]
					if ok {
						select {
						case <-kv.waiter[applyMsg.CommandIndex]:
						default:
						}
						ch <- op
					} else {
						kv.waiter[applyMsg.CommandIndex] = make(chan Op, 1)
					}

					//need snapshot
					if maxraftstate != -1 && kv.rf.RaftStateSize() > maxraftstate {
						w := new(bytes.Buffer)
						e := gob.NewEncoder(w)
						e.Encode(kv.db)
						e.Encode(kv.ack)
						data := w.Bytes()
						go kv.rf.MakeSnapshot(applyMsg.CommandIndex, data)
					}
					kv.mu.Unlock()

					// v, ok := kv.ack[op.ID]

					// if !ok || v < op.ReqID {
					// 	switch op.Kind {
					// 	case "Get":
					// 	case "Put":
					// 		kv.db[op.Key] = op.Value
					// 	case "Append":
					// 		kv.db[op.Key] += op.Value
					// 	default:
					// 		DPrintf("err op kind :%s\n", op.Kind)
					// 		panic("err op")
					// 	}
					// 	kv.ack[op.ID] = op.ReqID
					// }

					// if ch, ok := kv.waiter[applyMsg.CommandIndex]; !ok {
					// 	kv.waiter[applyMsg.CommandIndex] = make(chan Op, 1)
					// } else {
					// 	select {
					// 	case <-kv.waiter[applyMsg.CommandIndex]:
					// 	default:
					// 	}
					// 	ch <- op
					// }
					// if kv.maxraftstate != -1 && kv.rf.RaftStateSize() > kv.maxraftstate {
					// 	sp := kv.MakeSnapshot()
					// 	go kv.rf.MakeSnapshot(applyMsg.CommandIndex, sp)
					// }
				} else {
					var LastIncludedIndex int
					var LastIncludedTerm int

					r := bytes.NewBuffer(applyMsg.Snapshot)
					d := gob.NewDecoder(r)

					kv.mu.Lock()
					d.Decode(&LastIncludedIndex)
					d.Decode(&LastIncludedTerm)
					kv.db = make(map[string]string)
					kv.ack = make(map[int64]int64)
					d.Decode(&kv.db)
					d.Decode(&kv.ack)
					kv.mu.Unlock()
					// kv.LoadSnapshot(applyMsg.Snapshot)
				}
			}
		}
	}()

	return kv
}

func init() {
	go http.ListenAndServe("localhost:6060", nil)
}
