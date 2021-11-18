package shardkv

import "log"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrRetry       = "ErrRetry"

	PUT    = "Put"
	GET    = "Get"
	APPEND = "Append"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// type Err string

// // Put or Append
// type PutAppendArgs struct {
// 	// You'll have to add definitions here.
// 	Key   string
// 	Value string
// 	Op    string // "Put" or "Append"
// 	// You'll have to add definitions here.
// 	// Field names must start with capital letters,
// 	// otherwise RPC will break.
// }

// type PutAppendReply struct {
// 	Err Err
// }

// type GetArgs struct {
// 	Key string
// 	// You'll have to add definitions here.
// }

// type GetReply struct {
// 	Err   Err
// 	Value string
// }

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
