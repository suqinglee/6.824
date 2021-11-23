package shardkv

import (
	// "github.com/sasha-s/go-deadlock"
	"sync"
	"time"
	"sync/atomic"

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
	dead         int32

	// Your definitions here.
	data [shardctrler.NShards]map[string]string
	mseq map[int64]int64
	wait map[int]chan Args
	
	clerk *Clerk
	config shardctrler.Config
	need map[int]int
	send map[int]map[int]map[string]string
	hold map[int]bool
	garbages map[int]map[int]bool
}

func (kv *ShardKV) Request(args *Args, reply *Reply) {
	kv.mu.Lock()
	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err, reply.Value = kv.Receive(index, args.Cid, args.Seq)
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	// DPrintf("%v migrate shard %v num %v", kv.gid, args.Shard, args.ConfigNum)
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// DPrintf("%v me %v leader migrate shard %v num %v", kv.gid, kv.me, args.Shard, args.ConfigNum)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// DPrintf("%v migrate shard %v num %v, self num %v", kv.gid, args.Shard, args.ConfigNum, kv.config.Num)
	if args.ConfigNum >= kv.config.Num {
		reply.Err = ErrRetry
		return
	}
	reply.Data, reply.Mseq = kv.copyMap(args.ConfigNum, args.Shard)
	reply.Err = OK
}

func (kv *ShardKV) GC(args *GCArgs, reply *GCReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	reply.Err = ErrRetry
	if _, ok := kv.send[args.ConfigNum]; !ok {
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.send[args.ConfigNum][args.Shard]; !ok {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err, _ = kv.Receive(index, 0, 0)
}

func (kv *ShardKV) PullConfig() {
	for !kv.killed() {
		// time.Sleep(100 * time.Millisecond)
		_, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		if !isLeader || len(kv.need) > 0{
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		next := kv.config.Num + 1
		kv.mu.Unlock()
		// 不能加锁，会死锁
		config := kv.clerk.sm.Query(next)
		if config.Num == next {
			kv.rf.Start(config)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) PullShard() {
	for !kv.killed() {
		// time.Sleep(100 * time.Millisecond)
		_, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		if !isLeader || len(kv.need) == 0 {
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
			// return
		}
		var wg sync.WaitGroup
		// DPrintf("%v pull shard", kv.gid)
		for shard, configNum := range kv.need {
			// DPrintf("%v pull shard %v num %v", kv.gid, shard, configNum)
			wg.Add(1)
			go func(shard int, config shardctrler.Config) {
				defer wg.Done()
				args := MigrateArgs{Shard: shard, ConfigNum: config.Num}
				gid := config.Shards[shard]
				// DPrintf("%v pull shard %v num %v from %v", kv.gid, shard, config.Num, gid)
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
				// DPrintf("%v get shard %v num %v", kv.gid, shard, config.Num)
			}(shard, kv.clerk.sm.Query(configNum))
		}
		kv.mu.Unlock()
		wg.Wait()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) DetectGarbage() {
	for !kv.killed() {
		// time.Sleep(100 * time.Millisecond)
		_, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		if !isLeader || len(kv.garbages) == 0 {
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		var wg sync.WaitGroup
		for configNum, shards := range kv.garbages {
			for shard := range shards {
				wg.Add(1)
				go func(num int, shard int, config shardctrler.Config) {
					defer wg.Done()
					args := GCArgs{Shard: shard, ConfigNum: num}
					gid := config.Shards[shard]
					for _, server := range config.Groups[gid] {
						srv := kv.make_end(server)
						reply := GCReply{}
						// 确认对方已经收到了send[num][shard]
						ok := srv.Call("ShardKV.GC", &args, &reply)
						if ok && reply.Err == OK {
							kv.mu.Lock()
							defer kv.mu.Unlock()
							delete(kv.garbages[configNum], shard)
							if len(kv.garbages[configNum]) == 0 {
								delete(kv.garbages, configNum)
							}
						}
					}
				}(configNum, shard, kv.clerk.sm.Query(configNum))
			}
		}
		kv.mu.Unlock()
		wg.Wait()
		time.Sleep(100 * time.Millisecond)
	}
}


func (kv *ShardKV) Update() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if !msg.CommandValid {
			DPrintf("%v snapshot", kv.gid)
			kv.SnapshotHandler(msg.Snapshot, msg.SnapshotIndex, msg.SnapshotTerm)
			kv.Compaction(msg.SnapshotIndex)
		} else {
			if config, ok := msg.Command.(shardctrler.Config); ok {
				DPrintf("%v config", kv.gid)
				kv.ConfigHandler(config)
			} else if migrateData, ok := msg.Command.(MigrateData); ok {
				DPrintf("%v migrating", kv.gid)
				kv.MigrateHandler(migrateData)
			} else if gcArgs, ok := msg.Command.(GCArgs); ok {
				DPrintf("%v gc", kv.gid)
				kv.GCHandler(gcArgs, msg.CommandIndex)
			} else {
				DPrintf("%v command", kv.gid)
				kv.CommandHandler(msg.Command.(Args), msg.CommandIndex)
			}
			kv.Compaction(msg.CommandIndex)
		}
	}
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Args{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(MigrateData{})
	labgob.Register(GCArgs{})

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
	kv.garbages = make(map[int]map[int]bool)

	go kv.Update()
	go kv.PullConfig()
	go kv.PullShard()
	go kv.DetectGarbage()

	return kv
}
