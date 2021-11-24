package shardkv

import (
	"log"
	"bytes"
	"time"
	"6.824/labgob"
	"6.824/shardctrler"
	"fmt"
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
	GC      = "GC"
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
	Cid   int64
	Seq   int64
}

type Reply struct {
	Err   string
	Value string
}

type GCArgs struct {
	Shard int
	ConfigNum int
}

type GCReply struct {
	Err string
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

func (kv *ShardKV) myLock() {
	kv.mu.Lock()
	kv.s = time.Now()
}

func (kv *ShardKV) myUnlock(name string) {
	if time.Since(kv.s) > time.Duration(1*time.Second) {
		fmt.Printf("%v time out %v\n", name, time.Since(kv.s))
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) copyMap(num int, shard int) (map[string]string, map[int64]int64) {
	data, mseq := make(map[string]string), make(map[int64]int64)
	for k, v := range kv.send[num][shard] {
		data[k] = v
	}
	for k, v := range kv.mseq {
		mseq[k] = v
	}
	return data, mseq
}

// func (kv *ShardKV) rightShard(key string) bool {
// 	kv.mu.Lock()
// 	defer kv.mu.Unlock()
// 	return kv.config.Shards[key2shard(key)] == kv.gid
// }

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

func (kv *ShardKV) CommandHandler(op Args, index int) {
	// kv.mu.Lock()
	kv.myLock()
	// defer kv.mu.Unlock()
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
	ch, ok := kv.wait[index]
	// kv.mu.Unlock()
	kv.myUnlock("CommandHandler")

	if ok {
		select {
		case ch <- op:
		case <-time.After(2 * time.Second):
		}
	}

	// if _, ok := kv.wait[index]; ok {
	// 	select {
	// 	case kv.wait[index] <- op:
	// 	case <-time.After(2 * time.Second):
	// 	}
	// }
}

func (kv *ShardKV) GCHandler(args GCArgs, index int) {
	// kv.mu.Lock()
	kv.myLock()
	// defer kv.mu.Unlock()
	if _, ok := kv.send[args.ConfigNum]; ok {
		delete(kv.send[args.ConfigNum], args.Shard)
		if len(kv.send[args.ConfigNum]) == 0 {
			delete(kv.send, args.ConfigNum)
		}
	}
	ch, ok := kv.wait[index]
	// kv.mu.Unlock()
	kv.myUnlock("GCHandler")

	if ok {
		select {
		case ch <- Args{
			Act: GC,
			Cid: 0,
			Seq: 0,
		}:
		case <-time.After(2 * time.Second):
		}
	}

	// if _, ok := kv.wait[index]; ok {
	// 	select {
	// 	case kv.wait[index] <- Args{
	// 		Act: GC,
	// 		Cid: 0,
	// 		Seq: 0,
	// 	}:
	// 	case <-time.After(2 * time.Second):
	// 	}
	// }
}

func (kv *ShardKV) ConfigHandler(config shardctrler.Config) {
	// kv.mu.Lock()
	kv.myLock()
	// defer kv.mu.Unlock()
	defer kv.myUnlock("ConfigHandler")
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
	// kv.mu.Lock()
	kv.myLock()
	// defer kv.mu.Unlock()
	defer kv.myUnlock("MigrateHandler")
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
			if v > kv.mseq[k] {
				kv.mseq[k] = v
			}
		}
		if _, ok := kv.garbages[data.ConfigNum]; !ok {
			kv.garbages[data.ConfigNum] = make(map[int]bool)
		}
		kv.garbages[data.ConfigNum][data.Shard] = true
	}
}

func (kv *ShardKV) SnapshotHandler(snapshot []byte, index int, term int) {
	if len(snapshot) > 0 && kv.rf.CondInstallSnapshot(term, index, snapshot) {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		// kv.mu.Lock()
		kv.myLock()
		d.Decode(&kv.data)
		d.Decode(&kv.mseq)
		d.Decode(&kv.need)
		d.Decode(&kv.send)
		d.Decode(&kv.hold)
		d.Decode(&kv.config)
		d.Decode(&kv.garbages)
		// kv.mu.Unlock()
		kv.myUnlock("SnapshotHandler")
	}
}

func (kv *ShardKV) Compaction(index int) {
	if kv.rf.StateSize() > kv.maxraftstate && kv.maxraftstate != -1 {
		// kv.mu.Lock()
		kv.myLock()
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.data)
		e.Encode(kv.mseq)
		e.Encode(kv.need)
		e.Encode(kv.send)
		e.Encode(kv.hold)
		e.Encode(kv.config)
		e.Encode(kv.garbages)
		// kv.mu.Unlock()
		kv.myUnlock("Compaction")
		kv.rf.Snapshot(index, w.Bytes())
	}
}

func (kv *ShardKV) Receive(index int, cid int64, seq int64) (err string, value string) {
	// kv.mu.Lock()
	kv.myLock()
	ch := make(chan Args)
	kv.wait[index] = ch
	// kv.mu.Unlock()
	kv.myUnlock("Receive 1")

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
	case <-time.After(2 * time.Second):
		err = ErrRetry
	}

	// kv.mu.Lock()
	kv.myLock()
	// close(kv.wait[index])
	delete(kv.wait, index)
	// kv.mu.Unlock()
	kv.myUnlock("Receive 2")

	return err, value
}