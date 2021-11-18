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

	"6.824/labrpc"
	"6.824/shardctrler"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
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

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.cid = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
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
	DPrintf("%v %v %v", act, key, value)

	args := Args{
		Key:   key,
		Value: value,
		Act:   act,
		Cid:   ck.cid,
		Seq:   atomic.AddInt64(&ck.seq, 1),
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.Gid = gid
		servers, ok := ck.config.Groups[gid]
		if !ok {
			ck.config = ck.sm.Query(-1)
			continue
		}

		for i := 0; i < len(servers); i++ {
			srv := ck.make_end(servers[i])
			reply := Reply{}
			ok := srv.Call("ShardKV.Request", &args, &reply)
			DPrintf("%v %v", ok, reply.Err)
			if ok && reply.Err == OK {
				return reply.Value
			} else if ok && reply.Err == ErrWrongGroup {
				ck.config = ck.sm.Query(-1)
				break
			}
		}
	}
}
