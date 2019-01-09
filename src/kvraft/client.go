package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"raft"
	"sync/atomic"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	kvs   []*KVServer
	ReqID int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.kvs = make([]*KVServer, len(servers))
	// You'll have to add code here.
	for i := range servers {
		ck.kvs[i] = StartKVServer(servers, i, &raft.Persister{}, -1)
	}
	time.Sleep(3 * time.Second)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	for i := 0; ; {
		DPrintf("%d Get key %s \n", i, key)
		i = (i + 1) % len(ck.servers)
		args := &GetArgs{
			Key: key,
			ID:  atomic.AddInt64(&ck.ReqID, 1),
		}
		reply := &GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", args, reply)
		if ok && reply.Err == "" && reply.WrongLeader == false {
			return reply.Value
		}
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	DPrintf("PutAppend [%s, %s, %s].\n", key, value, op)

	for i := 0; ; {
		DPrintf("%d Get key %s \n", i, key)
		i = (i + 1) % len(ck.servers)
		args := &PutAppendArgs{
			Key:   key,
			Value: value,
			Op:    op,
			ID:    atomic.AddInt64(&ck.ReqID, 1),
		}
		reply := &PutAppendReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", args, reply)

		if ok && reply.Err == "" && reply.WrongLeader == false {
			DPrintf("PutAppend resp.\n")
			return
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
