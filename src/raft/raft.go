package raft

import (
	"sync"
	"time"

	"6.824/labrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

type Raft struct {
	mu           sync.Mutex
	peers        []*labrpc.ClientEnd
	persister    *Persister
	me           int
	dead         int32
	lastRecv     time.Time
	role         Role
	currentTerm  int
	votedFor     int
	logs         []LogEntry
	commitIndex  int
	lastApplied  int
	nextIndex    []int
	matchedIndex []int
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != Leader && time.Since(rf.lastRecv) > rf.electTimeout() {
			rf.elect()
		}
		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastRecv = time.Unix(0, 0)

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()

	return rf
}
