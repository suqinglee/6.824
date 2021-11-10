package raft

// import (
// 	"time"

// 	"6.824/labrpc"
// )

// type InstallSnapshotArgs struct {
// 	Term              int
// 	LeaderId          int
// 	LastIncludedIndex int
// 	LastIncludedTerm  int
// 	Data              []byte
// }

// type InstallSnapshotReply struct {
// 	Term int
// }

// func (rf *Raft) InstallSnapshot1(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	rf.lastRecv = time.Now()

// 	if args.Term > rf.currentTerm {
// 		rf.toFollower(args.Term)
// 	}
// 	reply.Term = rf.currentTerm

// 	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.log.Base {
// 		return
// 	}

// 	rf.applyCh <- ApplyMsg{
// 		SnapshotValid: true,
// 		Snapshot:      args.Data,
// 		SnapshotTerm:  args.LastIncludedTerm,
// 		SnapshotIndex: args.LastIncludedIndex,
// 	}
// }

// func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
// 	rf.mu.Lock()
// 	// DPrintf("server %d receive InstallSnapshot from %d\n", rf.me, args.LeaderId)
// 	defer rf.mu.Unlock()
// 	defer func() { reply.Term = rf.currentTerm }()

// 	switch {
// 	case args.Term < rf.currentTerm:
// 		//outdated request
// 		// DPrintf("server %d: InstallSnapshot, args.Term%d < rf.currentTerm%d\n", rf.me, args.Term, rf.currentTerm)
// 		return

// 	case args.Term > rf.currentTerm:
// 		//we are outdated
// 		rf.currentTerm = args.Term
// 		rf.persist()

// 		if rf.role != Follower {
// 			rf.role = Follower
// 		}

// 	case args.Term == rf.currentTerm:
// 		//normal
// 		if rf.role == Leader {
// 			// fmt.Printf("ERROR! Another leader in current term?!") //impossible
// 		} else if rf.role == Candidate {
// 			//fmt.Printf("Candidate %d abdicate!\n", rf.me)
// 			rf.role = Follower

// 		}
// 	}

// 	if args.LastIncludedIndex <= rf.log.Base {
// 		//coming snapshot is older than our snapshot
// 		// DPrintf("WARNING: outdated InstallSnapshot. This should only appear in unreliable cases.\n")
// 		return
// 	}
// 	rf.lastRecv = time.Now()

// 	msg := ApplyMsg{SnapshotValid: true, Snapshot: args.Data, SnapshotIndex: args.LastIncludedIndex, SnapshotTerm: args.LastIncludedTerm}
// 	go func() { rf.applyCh <- msg }()
// }

// //
// // A service wants to switch to snapshot.  Only do so if Raft hasn't
// // have more recent info since it communicate the snapshot on applyCh.
// //
// func (rf *Raft) CondInstallSnapshot1(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	if lastIncludedIndex <= rf.log.Base {
// 		return false
// 	}
// 	rf.log.cut(lastIncludedIndex, rf.log.size())
// 	rf.log.Base = lastIncludedIndex
// 	rf.snapshot = snapshot

// 	rf.persistSnapshot()
// 	return true
// }

// func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

// 	// Your code here (2D).
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	if lastIncludedIndex <= rf.commitIndex {
// 		//fmt.Printf("CondInstallSnapshot refused\n")
// 		return false
// 	}

// 	defer func() {
// 		rf.log.Base = lastIncludedIndex
// 		// rf.log.LastIncludedTerm = lastIncludedTerm
// 		rf.snapshot = snapshot
// 		rf.commitIndex = lastIncludedIndex //IMPORTANT
// 		rf.lastApplied = lastIncludedIndex //IMPORTANT
// 		rf.persistSnapshot()

// 	}()
// 	if lastIncludedIndex <= rf.log.size()-1 && rf.log.get(lastIncludedIndex).Term == lastIncludedTerm {
// 		rf.log.Entries = append([]LogEntry(nil), rf.log.Entries[lastIncludedIndex-rf.log.Base:]...)
// 		return true
// 	}

// 	//discard the entire log
// 	rf.log.Entries = make([]LogEntry, 0)
// 	return true
// }

// // the service says it has created a snapshot that has
// // all info up to and including index. this means the
// // service no longer needs the log through (and including)
// // that index. Raft should now trim its log as much as possible.
// func (rf *Raft) Snapshot1(index int, snapshot []byte) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	if index <= rf.log.Base {
// 		return
// 	}
// 	rf.log.cut(index, rf.log.size())
// 	rf.log.Base = index
// 	rf.snapshot = snapshot

// 	rf.persistSnapshot()
// }

// func (rf *Raft) Snapshot(index int, snapshot []byte) {
// 	// Your code here (2D).
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	// DPrintf("Server %d: Snapshot create\n", rf.me)
// 	if index <= rf.log.Base {
// 		//already created a snapshot
// 		return
// 	}
// 	rf.log.Entries = append([]LogEntry(nil), rf.log.Entries[index-rf.log.Base:]...)
// 	rf.log.Base = index
// 	// rf.log.LastIncludedTerm = rf.log.index(index).Term
// 	rf.snapshot = snapshot
// 	rf.persistSnapshot()
// }

// func (rf *Raft) sendSnapshot(id int, peer *labrpc.ClientEnd, args *InstallSnapshotArgs) {
// 	reply := InstallSnapshotReply{}
// 	ok := peer.Call("Raft.InstallSnapshot", args, &reply)
// 	if !ok {
// 		return
// 	}

// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	if reply.Term > rf.currentTerm {
// 		rf.toFollower(reply.Term)
// 		return
// 	}

// 	rf.nextIndex[id] = args.LastIncludedIndex + 1
// }
