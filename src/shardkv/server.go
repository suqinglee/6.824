package shardkv

import (
	"bytes"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type Op struct {
	Key   string
	Value string
	Gid   int
	Cid   int64
	Seq   int64
	Act   string
}

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
	data map[string]string
	mseq map[int64]int64
	recv map[int]chan Op
}

func (kv *ShardKV) Request(args *Args, reply *Reply) {
	if args.Gid != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Lock()
	if args.Seq <= kv.mseq[args.Cid] {
		reply.Err = OK
		reply.Value = kv.data[args.Key]
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(Op{
		Key:   args.Key,
		Value: args.Value,
		Gid:   args.Gid,
		Cid:   args.Cid,
		Seq:   args.Seq,
		Act:   args.Act,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan Op)
	kv.recv[index] = ch
	kv.mu.Unlock()

	select {
	case op := <-ch:
		if op.Cid != args.Cid || op.Seq != args.Seq {
			reply.Err = ErrRetry
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.data[op.Key]
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
		index := 0
		if msg.CommandValid {
			index = msg.CommandIndex
			op := msg.Command.(Op)
			kv.mu.Lock()
			if op.Seq > kv.mseq[op.Cid] {
				kv.mseq[op.Cid] = op.Seq
				switch op.Act {
				case PUT:
					kv.data[op.Key] = op.Value
				case APPEND:
					kv.data[op.Key] += op.Value
				}
			}

			if _, ok := kv.recv[msg.CommandIndex]; ok {
				kv.recv[msg.CommandIndex] <- op
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			if msg.Snapshot != nil && len(msg.Snapshot) > 0 {
				ok := kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot)
				if ok {
					r := bytes.NewBuffer(msg.Snapshot)
					d := labgob.NewDecoder(r)
					kv.mu.Lock()
					d.Decode(&kv.data)
					d.Decode(&kv.mseq)
					kv.mu.Unlock()
					index = msg.SnapshotIndex
				}
			}
		}

		if index != 0 && kv.rf.StateSize() > kv.maxraftstate && kv.maxraftstate != -1 {
			kv.mu.Lock()
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.data)
			e.Encode(kv.mseq)
			kv.rf.Snapshot(index, w.Bytes())
			kv.mu.Unlock()
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

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

	kv.data = make(map[string]string)
	kv.mseq = make(map[int64]int64)
	kv.recv = make(map[int]chan Op)

	go kv.Update()

	return kv
}
