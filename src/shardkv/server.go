package shardkv

import (
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data [shardctrler.NShards]map[string]string
	mseq map[int64]int64
	recv map[int]chan Args
	
	clerk *Clerk
	config shardctrler.Config
	migrating map[int]bool
}

func (kv *ShardKV) Request(args *Args, reply *Reply) {
	_, leader := kv.rf.GetState()
	if !leader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if args.Gid != kv.gid || args.Num != kv.config.Num {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if args.Act != MIGRATE && kv.migrating[args.Shard] {
		reply.Err = ErrRetry
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan Args)
	kv.recv[index] = ch
	kv.mu.Unlock()

	select {
	case op := <-ch:
		if op.Cid != args.Cid || op.Seq != args.Seq {
			reply.Err = ErrRetry
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.data[key2shard(op.Key)][op.Key]
			kv.mu.Unlock()
		}

	case <-time.After(1 * time.Second):
		reply.Err = ErrRetry
	}

	kv.mu.Lock()
	close(kv.recv[index])
	delete(kv.recv, index)
	kv.mu.Unlock()
}

func (kv *ShardKV) Update() {
	for {
		msg := <-kv.applyCh
		if !msg.CommandValid {
			kv.SnapshotHandler(msg.Snapshot, msg.SnapshotIndex, msg.SnapshotTerm)
			kv.Compaction(msg.SnapshotIndex)
		} else {
			kv.CommandHandler(msg.Command.(Args), msg.CommandIndex)
			kv.Compaction(msg.CommandIndex)
		}
	}
}

func (kv *ShardKV) Pull() {
	for {
		time.Sleep(100 * time.Millisecond)
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			continue
		}
		config := kv.clerk.sm.Query(-1)
		if config.Num == kv.config.Num {
			continue
		}
		kv.mu.Lock()
		DPrintf("me:%v gid:%v detect config change %v to %v",kv.me, kv.gid, kv.config.Num, config.Num)
		DPrintf("me:%v gid:%v old config %v",kv.me, kv.gid, kv.config)
		DPrintf("me:%v gid:%v new config %v",kv.me, kv.gid, config)
		ready := make(map[int]map[string]string)
		for shard, gid := range config.Shards {
			if gid == kv.gid && kv.config.Shards[shard] != kv.gid && kv.config.Shards[shard] != 0 {
				DPrintf("me:%v gid:%v need shard %v from %v",kv.me, kv.gid, shard, kv.config.Shards[shard])
				kv.migrating[shard] = true
			}
			if gid != kv.gid && kv.config.Shards[shard] == kv.gid {
				kv.migrating[shard] = true
				DPrintf("me:%v gid:%v send shard %v to %v",kv.me, kv.gid, shard, gid)
				if ready[shard] == nil {
					ready[shard] = make(map[string]string)
				}
				for key, value := range kv.data[shard] {
					ready[shard][key] = value
				}
			}
		}
		DPrintf("me:%v gid:%v migrating %v",kv.me, kv.gid, kv.migrating)
		DPrintf("me:%v gid:%v update config %v to %v",kv.me, kv.gid, kv.config.Num, config.Num)
		kv.config = config
		kv.mu.Unlock()
		DPrintf("me:%v gid:%v ready send %v",kv.me, kv.gid, ready)

		for shard, data := range ready {
			kv.clerk.Migrate(shard, data)
			kv.mu.Lock()
			kv.migrating[shard] = false
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Args{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	for i := 0; i < shardctrler.NShards; i++ {
		kv.data[i] = make(map[string]string)
	}
	kv.mseq = make(map[int64]int64)
	kv.recv = make(map[int]chan Args)

	kv.clerk = MakeClerk(ctrlers, make_end)
	kv.migrating = make(map[int]bool)

	go kv.Update()
	go kv.Pull()

	return kv
}
