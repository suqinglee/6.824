package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
	"bytes"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key   string
	Value string
	Cid   int64
	Seq   int64
	Act   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	maxraftstate int // snapshot if log grows this big

	data map[string]string
	mseq map[int64]int64
	recv map[int]chan Op
}

func (kv *KVServer) Request(args *Args, reply *Reply) {
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
		Cid:   args.Cid,
		Seq:   args.Seq,
		Act:   args.Act,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[%v] (%v, %v) cid=%v, seq=%v", args.Act, args.Key, args.Value, args.Cid, args.Seq)

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

	DPrintf("cid=%v, seq=%v, reply=%v, value=%v", args.Cid, args.Seq, reply.Err, reply.Value)

	kv.mu.Lock()
	close(kv.recv[index])
	delete(kv.recv, index)
	kv.mu.Unlock()
}

func (kv *KVServer) Update() {
	for !kv.killed() {
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
		} else if msg.SnapshotValid  {
			DPrintf("Receive Snapshot, lastIndex %v, len(snapshot) %v", msg.SnapshotIndex, len(msg.Snapshot))
			if msg.Snapshot != nil && len(msg.Snapshot) > 0 {
				ok := kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot)
				if ok {
					DPrintf("Install Snapshot, lastIndex %v", msg.SnapshotIndex)
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

		size := kv.rf.StateSize()
		if index != 0 && size > kv.maxraftstate && kv.maxraftstate != -1 {
			DPrintf("Log Trim, index %v, size %v", index, size)
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

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	DPrintf("Start KVServer %v, maxraftstate %v", me, maxraftstate)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.mseq = make(map[int64]int64)
	kv.recv = make(map[int]chan Op)

	go kv.Update()

	return kv
}
