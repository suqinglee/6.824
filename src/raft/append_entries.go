package raft

import (
	"time"

	"6.824/labrpc"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	/* 1. AppendEntries RPC Implementation */

	// 1.1 Reply false if term < currentTerm (5.1)
	if args.Term < rf.currentTerm {
		return
	}
	// 1.2 Reply false if log doesn't contain an entry at pervLogIndex whose term matches pervLogTerm (5.3)

	// 1.3 If an existing entry conficts with a new one (same index but different terms), delete the existing entry and all that follow it (5.3)

	// 1.4 Append any new entries not already in the log

	// 1.5 If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	/* 2. Rules for All Servers */

	// 2.1 If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (5.3)

	// 2.2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (5.1)
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}
	reply.Term = rf.currentTerm

	reply.Success = true
	rf.lastRecv = time.Now()
}

func (rf *Raft) sync() {
	for !rf.killed() {
		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role != Leader {
				return
			}

			for id, peer := range rf.peers {
				if id == rf.me {
					continue
				}

				go func(peer *labrpc.ClientEnd, args *AppendEntriesArgs) {
					reply := AppendEntriesReply{}
					peer.Call("Raft.AppendEntries", args, &reply)

					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.currentTerm {
						rf.toFollower(reply.Term)
					}
				}(peer, &AppendEntriesArgs{Term: rf.currentTerm})
			}

		}()
		time.Sleep(rf.syncInterval())
	}
}
