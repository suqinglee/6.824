package models

import (
	"encoding/json"
)

type Task struct {
	Files     []string
	Worker    int
	XY        int
	M         int
	R         int
	Type      int
	TempFiles []string `json:"-"`
}

func (t *Task) ToString() string {
	bytes, _ := json.Marshal(t)
	return string(bytes)
}

func FromString(str string) (task Task) {
	json.Unmarshal([]byte(str), &task)
	return task
}

const (
	MAP    = 0
	REDUCE = 1
	END    = 2
)
