package shardkv

import (
	"log"
	"bytes"
	"time"
	"6.824/labgob"
	"6.824/shardctrler"
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
	MOVE    = "Move"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Args struct {
	Key   string
	Value string
	Act   string
	Gid   int
	Cid   int64
	Seq   int64
}

type Reply struct {
	Err   string
	Value string
}

type MigrateArgs struct {
	Shard     int
	ConfigNum int
}

type MigrateReply struct {
	Err  string
	Data map[string]string
	Mseq map[int64]int64
}

type MigrateData struct {
	Shard int
	ConfigNum int
	Data map[string]string
	Mseq map[int64]int64
}

func (kv *ShardKV) rightShard(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.config.Shards[key2shard(key)] == kv.gid
}

func (kv *ShardKV) checkConfig() (shardctrler.Config, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	config := kv.clerk.sm.Query(kv.config.Num + 1)
	return config, config.Num != kv.config.Num
}

func (kv *ShardKV) needShard() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return len(kv.need) > 0
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

func (kv *ShardKV) GetHandler(op Args) string {
	return kv.data[key2shard(op.Key)][op.Key]
}

func (kv *ShardKV) SnapshotHandler(snapshot []byte, index int, term int) {
	if len(snapshot) > 0 && kv.rf.CondInstallSnapshot(term, index, snapshot) {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		kv.mu.Lock()
		d.Decode(&kv.data)
		d.Decode(&kv.mseq)
		d.Decode(&kv.need)
		d.Decode(&kv.send)
		d.Decode(&kv.hold)
		d.Decode(&kv.config)
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) CommandHandler(op Args, index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.hold[key2shard(op.Key)]; !ok {
		op.Act = MOVE
	} else {
		switch op.Act {
		case PUT:
			kv.PutHandler(op)
		case APPEND:
			kv.AppendHandler(op)
		case GET:
			op.Value = kv.GetHandler(op)
		}
	}
	if _, ok := kv.wait[index]; ok {
		kv.wait[index] <- op
	}
}

func (kv *ShardKV) ConfigHandler(config shardctrler.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if config.Num <= kv.config.Num {
		return
	}
	oldConfig, toSend := kv.config, kv.hold
	kv.hold, kv.config = make(map[int]bool), config
	for shard, gid := range config.Shards {
		if gid != kv.gid {
			continue
		}
		if _, ok := toSend[shard]; ok || oldConfig.Num == 0 {
			kv.hold[shard] = true
			delete(toSend, shard)
		} else {
			kv.need[shard] = oldConfig.Num
		}
	}
	if len(toSend) > 0 {
		kv.send[oldConfig.Num] = make(map[int]map[string]string)
		for shard := range toSend {
			kv.send[oldConfig.Num][shard] = make(map[string]string)
			for k, v := range kv.data[shard] {
				kv.send[oldConfig.Num][shard][k] = v
				delete(kv.data[shard], k)
			}
		}
	}
	kv.config = config
}

func (kv *ShardKV) MigrateHandler(data MigrateData) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if data.ConfigNum != kv.config.Num - 1 {
		return
	}
	delete(kv.need, data.Shard)
	if _, ok := kv.hold[data.Shard]; !ok {
		kv.hold[data.Shard] = true
		for k, v := range data.Data {
			kv.data[data.Shard][k] = v
		}
		for k, v := range data.Mseq {
			kv.mseq[k] = v
		}
	}
}

func (kv *ShardKV) Compaction(index int) {
	if kv.rf.StateSize() > kv.maxraftstate && kv.maxraftstate != -1 {
		kv.mu.Lock()
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.data)
		e.Encode(kv.mseq)
		e.Encode(kv.need)
		e.Encode(kv.send)
		e.Encode(kv.hold)
		e.Encode(kv.config)
		kv.rf.Snapshot(index, w.Bytes())
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) Receive(index int, cid int64, seq int64) (err string, value string) {
	kv.mu.Lock()
	ch := make(chan Args)
	kv.wait[index] = ch
	kv.mu.Unlock()

	select {
	case op := <-ch:
		if op.Cid != cid || op.Seq != seq {
			err = ErrRetry
		} else {
			if op.Act == MOVE {
				err = ErrWrongGroup
			} else {
				err = OK
				value = op.Value
			}
		}
	case <-time.After(1 * time.Second):
		err = ErrRetry
	}

	kv.mu.Lock()
	close(kv.wait[index])
	delete(kv.wait, index)
	kv.mu.Unlock()

	return err, value
}