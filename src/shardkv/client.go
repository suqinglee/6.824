package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	cid int64
	seq int64
}

func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.cid = nrand()
	return ck
}

func (ck *Clerk) Get(key string) string {
	return ck.Request(Args {
		Key: key,
		Act: GET,
		// Shard: key2shard(key),
	})
}

func (ck *Clerk) Put(key string, value string) {
	ck.Request(Args {
		Key: key,
		Value: value,
		Act: PUT,
		// Shard: key2shard(key),
	})
}
func (ck *Clerk) Append(key string, value string) {
	ck.Request(Args {
		Key: key,
		Value: value,
		Act: APPEND,
		// Shard: key2shard(key),
	})
}

func (ck *Clerk) Request(args Args) string {
	args.Cid = ck.cid
	args.Seq = atomic.AddInt64(&ck.seq, 1)
	// DPrintf("Request %v", args)

	for {
		shard := key2shard(args.Key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for i := 0; i < len(servers); i++ {
				srv := ck.make_end(servers[i])
				reply := Reply{}
				ok := srv.Call("ShardKV.Request", &args, &reply)
				if ok && reply.Err == OK {
					return reply.Value
				} else if ok && reply.Err == ErrWrongGroup {
					break
				}
			}
		}
		time.Sleep(1000 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}
