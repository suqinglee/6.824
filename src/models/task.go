package models

type Task struct {
	FileName  string
	Worker    int // Worker's pid
	XY        int // Map or Reduce task id
	M         int // NMap
	R         int // NReduce
	Type      int // Map or Reduce
	Status    int // READY | DOING | DONE
	StartTime int64
}

const (
	MAP    = 0
	REDUCE = 1
	END    = 3

	READY = 4
	DOING = 5
	DONE  = 6
)
