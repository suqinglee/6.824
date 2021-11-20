package shardkv

import (
	"log"
	"bytes"
	"6.824/labgob"
)	

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrRetry       = "ErrRetry"

	PUT     = "Put"
	GET     = "Get"
	APPEND  = "Append"
	MIGRATE = "Migrate"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Args struct {
	Key   string
	Value string
	Shard int
	Act   string
	Gid   int
	Cid   int64
	Seq   int64
	Num   int
	Data  map[string]string
}

type Reply struct {
	Err   string
	Value string
}

func (kv *ShardKV) PutHandler(op Args) {
	if op.Seq > kv.mseq[op.Cid] {
		kv.mseq[op.Cid] = op.Seq
		kv.data[key2shard(op.Key)][op.Key] = op.Value
	}
}

func (kv *ShardKV) AppendHandler(op Args) {
	if op.Seq > kv.mseq[op.Cid] {
		kv.mseq[op.Cid] = op.Seq
		kv.data[key2shard(op.Key)][op.Key] += op.Value
	}
}

func (kv *ShardKV) MigrateHandler(op Args) {
	if op.Seq > kv.mseq[op.Cid] {
		DPrintf("me:%v gid:%v recv shard %v data %v", kv.me, kv.gid, op.Shard, op.Data)
		for key, value := range op.Data {
			kv.data[key2shard(key)][key] = value
		}
		kv.migrating[op.Shard] = false
		DPrintf("me:%v gid:%v migrating %v", kv.me, kv.gid, kv.migrating)
	}
}

func (kv *ShardKV) SnapshotHandler(snapshot []byte, index int, term int) {
	if len(snapshot) > 0 && kv.rf.CondInstallSnapshot(term, index, snapshot) {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		kv.mu.Lock()
		d.Decode(&kv.data)
		d.Decode(&kv.mseq)
		// d.Decode(&kv.config)
		// d.Decode(&kv.migrating)
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) CommandHandler(op Args, index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch op.Act {
	case PUT:
		kv.PutHandler(op)
	case APPEND:
		kv.AppendHandler(op)
	case MIGRATE:
		kv.MigrateHandler(op)
	}
	if _, ok := kv.recv[index]; ok {
		kv.recv[index] <- op
	}
}

func (kv *ShardKV) Compaction(index int) {
	if kv.rf.StateSize() > kv.maxraftstate && kv.maxraftstate != -1 {
		kv.mu.Lock()
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.data)
		e.Encode(kv.mseq)
		// e.Encode(kv.config)
		// e.Encode(kv.migrating)
		kv.rf.Snapshot(index, w.Bytes())
		kv.mu.Unlock()
	}
}