package models

type Task struct {
	FileName  string
	X         int // mr-X-Y
	M         int // NMap
	R         int // NReduce
	Type      int // Map or Reduce
	StartTime int64
}

const (
	MAP    = 0
	REDUCE = 1
	DONE   = 2
)
