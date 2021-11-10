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

	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	rf.log.Entries = rf.log.Entries[lastIncludedIndex-rf.log.Base:]
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.toFollower(reply.Term)
		return
	}

	rf.nextIndex[id] = args.LastIncludedIndex + 1
}
