package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	mu  sync.Mutex
	lid int
	cid int64
	seq int64
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
	// Your code here.
	ck.cid = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	return ck.Request(nil, num, nil, 0, Query)
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.Request(servers, 0, nil, 0, Join)
}

func (ck *Clerk) Leave(gids []int) {
	ck.Request(nil, 0, gids, 0, Leave)
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.Request(nil, 0, []int{gid}, shard, Move)
}

func (ck *Clerk) Request(servers map[int][]string, num int, gids []int, shard int, act string) Config {
	args := Args{
		Act:     act,
		Cid:     ck.cid,
		Seq:     atomic.AddInt64(&ck.seq, 1),
		Servers: servers,
		Num:     num,
		GIDs:    gids,
		Shard:   shard,
	}
	reply := Reply{}

	// for {
	// 	ok := ck.servers[ck.lid].Call("ShardCtrler.Request", &args, &reply)
	// 	if ok && reply.Err == OK {
	// 		break
	// 	} else if !ok || reply.Err == ErrWrongLeader {
	// 		ck.mu.Lock()
	// 		ck.lid = (ck.lid + 1) % len(ck.servers)
	// 		ck.mu.Unlock()
	// 	}
	// }

	for {
		for _, srv := range ck.servers {
			ok := srv.Call("ShardCtrler.Request", &args, &reply)
			if ok && reply.Err == OK {
				return reply.Config
			}
		}
	}

	return reply.Config
}
