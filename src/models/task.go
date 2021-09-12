package models

import "fmt"

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

func (t *Task) Unique() string {
	return fmt.Sprintf("%v%v%06v", t.Type, t.XY, t.Worker)
}

const (
	MAP    = 0
	REDUCE = 1
	END    = 2

	READY = 3
	DOING = 4
	DONE  = 5

	INPROGRESS = 6
	HANG       = 7
	FINISH     = 8
)
