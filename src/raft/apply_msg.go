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

func (rf *Raft) apply() {
	for !rf.killed() {
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.commitIndex <= rf.lastApplied {
				return
			}
			/* Rules for All Servers */
			/* 1. If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (5.3) */
			for i := rf.lastApplied + 1; i > rf.log.Base && i <= rf.commitIndex; i++ {
				// i > Base 有可能在<-的时候Base发生变化，导致i越界
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log.get(i).Command,
					CommandIndex: i,
				}
				rf.mu.Unlock()
				rf.applyCh <- msg
				rf.mu.Lock()
			}
			rf.lastApplied = rf.commitIndex
		}()

		time.Sleep(10 * time.Millisecond)
	}
}
