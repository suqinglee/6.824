package kvraft

import (
	crand "crypto/rand"
	"math/big"
	mrand "math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu       sync.Mutex
	leaderId int
	cid      string
	seq      int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.cid = strconv.FormatInt(mrand.Int63(), 16) + strconv.FormatInt(time.Now().UnixNano(), 16)
	ck.seq = 0
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
	args := GetArgs{
		Key: key,
		Cid: ck.cid,
		Seq: atomic.LoadInt64(&ck.seq),
	}
	reply := GetReply{}

	ck.mu.Lock()
	defer ck.mu.Unlock()

	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err != OK {
			if reply.Err == ErrWrongLeader {
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			}
			continue
		}
		return reply.Value
	}

	// You will have to modify this function.
	// return ""
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
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Cid:   ck.cid,
		Seq:   atomic.AddInt64(&ck.seq, 1),
	}
	reply := PutAppendReply{}

	ck.mu.Lock()
	defer ck.mu.Unlock()

	// DPrintf("putappend %v %v", key, value)

	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err != OK {
			if reply.Err == ErrWrongLeader {
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			}
			continue
		}
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
