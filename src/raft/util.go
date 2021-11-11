package raft

import (
	"bytes"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	"6.824/labgob"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) syncInterval() time.Duration {
	return 120 * time.Millisecond
}

func (rf *Raft) electTimeout() time.Duration {
	return time.Duration(150+rand.Int31n(200)) * time.Millisecond
}

func (rf *Raft) toLeader() {
	rf.role = Leader
	rf.votedFor = -1
	rf.lastRecv = time.Now()

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.log.size()
		rf.matchIndex[i] = 0
	}
	// DPrintf("[%v %v] to leader", rf.me, rf.currentTerm)
}

func (rf *Raft) toFollower(term int) {
	rf.role = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.lastRecv = time.Now()
	// DPrintf("[%v %v] to follower", rf.me, rf.currentTerm)
}

func (rf *Raft) toCandidate() {
	rf.role = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.lastRecv = time.Now()
	// DPrintf("[%v %v] to candidate", rf.me, rf.currentTerm)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.currentTerm, Leader == rf.role
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
}

// func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
// 	w := new(bytes.Buffer)
// 	e := labgob.NewEncoder(w)
// 	e.Encode(rf.currentTerm)
// 	e.Encode(rf.votedFor)
// 	e.Encode(rf.log)
// 	data := w.Bytes()

// 	rf.persister.SaveStateAndSnapshot(data, snapshot)
// }

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		// DPrintf("Decode error.")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
// func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

// 	// Your code here (2D).

// 	return true
// }

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// func (rf *Raft) Snapshot(index int, snapshot []byte) {
// 	// Your code here (2D).

// }

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return -1, -1, false
	}

	/* Rules for Leader */
	/* 2. If command received from client: append entry to local log, respond after entry applied to state machine (5.3) */

	rf.log.Entries = append(rf.log.Entries, LogEntry{Term: rf.currentTerm, Command: command})
	rf.nextIndex[rf.me] = rf.log.size()
	rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1

	rf.persist()
	return rf.log.size() - 1, rf.currentTerm, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
