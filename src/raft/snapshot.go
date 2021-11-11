package raft

import (
	"time"

	"6.824/labrpc"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastRecv = time.Now()

	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.log.Base {
		return
	}

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	go func() {rf.applyCh <- msg}()
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	if lastIncludedIndex <= rf.log.size()-1 && rf.log.get(lastIncludedIndex).Term == lastIncludedTerm {
		rf.log.Entries = append([]LogEntry(nil), rf.log.Entries[lastIncludedIndex-rf.log.Base:]...)
	} else {
		rf.log.Entries = append([]LogEntry(nil), LogEntry{Term: lastIncludedTerm})
	}

	rf.log.Base = lastIncludedIndex
	rf.snapshot = snapshot
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.persistSnapshot()

	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v snapshot", rf.me)
	if index <= rf.log.Base {
		return
	}
	rf.log.Entries = rf.log.Entries[index-rf.log.Base:]
	rf.log.Base = index
	rf.snapshot = snapshot
	rf.persistSnapshot()
}

func (rf *Raft) sendSnapshot(id int, peer *labrpc.ClientEnd, args *InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	ok := peer.Call("Raft.InstallSnapshot", args, &reply)
	if !ok {
		return
	}
	DPrintf("%v send snapshot", rf.me)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.toFollower(reply.Term)
		return
	}

	rf.nextIndex[id] = args.LastIncludedIndex + 1
}
