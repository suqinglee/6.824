package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

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

// type OpCtx struct {
// 	op      *Op
// 	applied chan byte
// 	err     string
// }

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	maxraftstate int // snapshot if log grows this big

	// ClientA PutAppend，RPC发送成功，返回失败，ClientA resent，resent的op提交时忽略，所以加了下面两个字段
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

	kv.mu.Lock()
	ch := make(chan Op)
	kv.recv[index] = ch
	kv.mu.Unlock()

	select {
	case op := <-ch:
		if op.Cid != args.Cid || op.Seq != args.Seq {
			reply.Err = ErrRetry
			return
		}
		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.data[op.Key]
		kv.mu.Unlock()

	case <-time.After(2 * time.Second):
		reply.Err = ErrRetry
	}

	kv.mu.Lock()
	close(kv.recv[index])
	delete(kv.recv, index)
	kv.mu.Unlock()
}

func (kv *KVServer) Update() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if !msg.CommandValid {
			continue
		}
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
	}
}

// func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
// 	// Your code here.
// 	reply.Err = OK
// 	_, isLeader := kv.rf.GetState()
// 	if !isLeader {
// 		reply.Err = ErrWrongLeader
// 		return
// 	}

// 	kv.mu.Lock()
// 	defer kv.mu.Unlock()

// 	_, exist := kv.updating[args.Key]
// 	if exist {
// 		reply.Err = ErrRetry
// 	}
// 	reply.Value = kv.data[args.Key]
// 	DPrintf("return cid:%v seq:%v key:%v val:%v", args.Cid, args.Seq, args.Key, reply.Value)
// }

// func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
// 	op := Op{
// 		Action: args.Op,
// 		Key:    args.Key,
// 		Value:  args.Value,
// 		Cid:    args.Cid,
// 		Seq:    args.Seq,
// 	}

// 	var isLeader bool
// 	op.Index, op.Term, isLeader = kv.rf.Start(op)
// 	if !isLeader {
// 		reply.Err = ErrWrongLeader
// 		return
// 	}

// 	ctx := &OpCtx{
// 		op:      &op,
// 		applied: make(chan byte),
// 		err:     OK,
// 	}
// 	kv.mu.Lock()
// 	kv.asking[op.Index] = ctx
// 	kv.updating[op.Key]++
// 	prev := kv.updating[op.Key]
// 	kv.mu.Unlock()

// 	select {
// 	case <-ctx.applied:
// 		reply.Err = ctx.err
// 	case <-time.After(1 * time.Second):
// 		reply.Err = ErrRetry
// 	}

// 	kv.mu.Lock()
// 	delete(kv.asking, op.Index)
// 	if prev == kv.updating[op.Key] {
// 		delete(kv.updating, op.Key)
// 	}
// 	kv.mu.Unlock()
// 	DPrintf("start %v %v %v, cid=%v, seq=%v", args.Op, args.Key, args.Value, args.Cid, args.Seq)
// }

// func (kv *KVServer) Update() {
// 	for !kv.killed() {
// msg := <-kv.applyCh
// index := msg.CommandIndex
// op := msg.Command.(Op)
// if msg.CommandValid {
// 	kv.execute(op, index)
// }
// 	}
// }

// func (kv *KVServer) execute(op Op, i int) {
// 	kv.mu.Lock()
// 	defer kv.mu.Unlock()

// 	ctx, exist := kv.asking[i]
// 	if exist && op.Seq > kv.applied[op.Cid] {
// 		// if ctx.op.Term != op.Term {
// 		// 	ctx.err = ErrWrongLeader
// 		// }
// 		switch op.Action {
// 		case "Put":
// 			kv.data[op.Key] = op.Value
// 		case "Append":
// 			kv.data[op.Key] += op.Value
// 		}
// 	}
// 	if exist {
// 		close(ctx.applied)
// 	}
// 	DPrintf("exec %v %v %v %v %v", op.Action, op.Key, op.Cid, op.Seq, op.Value)
// }

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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.mseq = make(map[int64]int64)
	kv.recv = make(map[int]chan Op)

	go kv.Update()

	return kv
}
