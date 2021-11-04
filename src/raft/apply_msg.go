package raft

import "time"

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

func (rf *Raft) apply(applyCh chan ApplyMsg) {
	for !rf.killed() {
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.commitIndex <= rf.lastApplied {
				return
			}
			/* Rules for All Servers */
			/* 1. If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (5.3) */
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i,
				}
				DPrintf("[%d]: apply index %d - 1", rf.me, i)
			}
			rf.lastApplied = rf.commitIndex
		}()

		time.Sleep(10 * time.Millisecond)
	}
}
