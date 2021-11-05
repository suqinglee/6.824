package raft

import (
	"sync"
	"time"

	"6.824/labrpc"
)

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

type Raft struct {
	mu          sync.Mutex
	peers       []*labrpc.ClientEnd
	persister   *Persister
	me          int
	dead        int32
	lastRecv    time.Time
	role        Role
	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()

		/* Rules for Followers */
		/* 2. If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate */
		/* Rules for Candidates */
		/* 4. If election timeout elapses: start new election */
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
	rf.lastRecv = time.Now()

	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.apply(applyCh)

	return rf
}
