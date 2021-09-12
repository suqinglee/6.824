package models

import "fmt"

type Task struct {
	Files     []string
	Worker    int // Worker's pid
	XY        int // Map or Reduce task id
	M         int
	R         int
	Type      int // Map or Reduce
	StartTime int64
}

func (t *Task) Unique() string {
	return fmt.Sprintf("%v%v%06v", t.Type, t.XY, t.Worker)
}

const (
	MAP    = 0
	REDUCE = 1
	END    = 2
)
