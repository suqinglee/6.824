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
