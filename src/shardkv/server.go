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
	wait map[int]chan Args
	
	clerk *Clerk
	config shardctrler.Config
	need map[int]int
	send map[int]map[int]map[string]string
	hold map[int]bool
}

func (kv *ShardKV) Request(args *Args, reply *Reply) {
	if !kv.rightShard(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}

	index, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err, reply.Value = kv.Receive(index, args.Cid, args.Seq)
}

func (kv *ShardKV) Update() {
	for {
		msg := <-kv.applyCh
		if !msg.CommandValid {
			kv.SnapshotHandler(msg.Snapshot, msg.SnapshotIndex, msg.SnapshotTerm)
			kv.Compaction(msg.SnapshotIndex)
		} else {
			if config, ok := msg.Command.(shardctrler.Config); ok {
				DPrintf("update config %v", config)
				kv.ConfigHandler(config)
			} else if migrateData, ok := msg.Command.(MigrateData); ok {
				DPrintf("update migrate %v", migrateData)
				kv.MigrateHandler(migrateData)
			} else {
				kv.CommandHandler(msg.Command.(Args), msg.CommandIndex)
			}
			kv.Compaction(msg.CommandIndex)
		}
	}
}

func (kv *ShardKV) PullConfig() {
	for {
		time.Sleep(100 * time.Millisecond)
		if _, isLeader := kv.rf.GetState(); !isLeader || kv.needShard() {
			continue
		}
		if config, changed := kv.checkConfig(); changed {
			DPrintf("pull config %v", config)
			kv.rf.Start(config)
		}
	}
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.ConfigNum > kv.config.Num {
		reply.Err = ErrRetry
		return
	}
	reply.Data = kv.send[args.ConfigNum][args.Shard]
	reply.Mseq = kv.mseq
	reply.Err = OK
}

func (kv *ShardKV) PullShard() {
	for {
		time.Sleep(100 * time.Millisecond)
		if _, isLeader := kv.rf.GetState(); !isLeader || !kv.needShard() {
			continue
		}
		var wg sync.WaitGroup
		kv.mu.Lock()
		for shard, configNum := range kv.need {
			go func(shard int, config shardctrler.Config) {
				defer wg.Done()
				args := MigrateArgs{Shard: shard, ConfigNum: config.Num}
				gid := config.Shards[shard]
				for _, server := range config.Groups[gid] {
					srv := kv.make_end(server)
					reply := MigrateReply{}
					ok := srv.Call("ShardKV.Migrate", &args, &reply)
					if ok && reply.Err == OK {
						kv.rf.Start(MigrateData{
							Shard: shard,
							ConfigNum: config.Num,
							Data: reply.Data,
							Mseq: reply.Mseq,
						})
					}
				}
			}(shard, kv.clerk.sm.Query(configNum))
			wg.Add(1)
		}
		kv.mu.Unlock()
		wg.Wait()
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
	labgob.Register(shardctrler.Config{})
	labgob.Register(MigrateData{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	for i := 0; i < shardctrler.NShards; i++ {
		kv.data[i] = make(map[string]string)
	}
	kv.mseq = make(map[int64]int64)
	kv.wait = make(map[int]chan Args)

	kv.clerk = MakeClerk(ctrlers, make_end)
	kv.need = make(map[int]int)
	kv.send = make(map[int]map[int]map[string]string)
	kv.hold = make(map[int]bool)

	go kv.Update()
	go kv.PullConfig()
	go kv.PullShard()

	return kv
}
