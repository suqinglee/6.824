package raft

type LogEntry struct {
	Term    int
	Command interface{}
}

type Log struct {
	Entries []LogEntry
	Base    int
}

func (l *Log) size() int {
	return l.Base + len(l.Entries)
}

func (l *Log) get(i int) LogEntry {
	return l.Entries[i-l.Base]
}

// func (l *Log) set(i int, e LogEntry) {
// 	l.Entries[i-l.Base] = e
// }
