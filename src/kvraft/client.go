package kvraft

import (
	crand "crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	mu      sync.Mutex
	lid     int
	cid     int64
	seq     int64
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
	ck.cid = nrand()
	return ck
}

func (ck *Clerk) Get(key string) string {
	return ck.Request(key, "", GET)
}

func (ck *Clerk) Put(key string, value string) {
	ck.Request(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.Request(key, value, APPEND)
}

func (ck *Clerk) Request(key string, value string, act string) string {
	args := Args{
		Key:   key,
		Value: value,
		Act:   act,
		Cid:   ck.cid,
		Seq:   atomic.AddInt64(&ck.seq, 1),
	}
	reply := Reply{}

	for {
		// DPrintf("key=%v, value=%v", key, value)
		ok := ck.servers[ck.lid].Call("KVServer.Request", &args, &reply)
		if ok && reply.Err == OK {
			break
		} else if !ok || reply.Err == ErrWrongLeader {
			ck.mu.Lock()
			ck.lid = (ck.lid + 1) % len(ck.servers)
			ck.mu.Unlock()
		}
	}

	return reply.Value
}
