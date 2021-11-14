package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrRetry       = "ErrRetry"

	PUT    = "PUT"
	GET    = "GET"
	APPEND = "APPEND"
)

// // Put or Append
// type PutAppendArgs struct {
// 	Key   string
// 	Value string
// 	Op    string // "Put" or "Append"
// 	Cid   string
// 	Seq   int64
// }

// type PutAppendReply struct {
// 	Err Err
// }

// type GetArgs struct {
// 	Key string
// 	Cid string
// 	Seq int64
// }

// type GetReply struct {
// 	Err   Err
// 	Value string
// }

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
