package raft

import (
	"sort"
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

	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastRecv = time.Now()
	reply.Success = false
	reply.ConflictIndex = rf.log.size()
	reply.ConflictTerm = -1

	/* Rules for All Servers */
	/* 1. If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (5.3) */

	/* Rules for All Servers */
	/* 2. If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (5.1) */
	/* Rules for Candidates */
	/* 3. If AppendEntries RPC received from new leader: convert to follower */
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}
	reply.Term = rf.currentTerm

	/* AppendEntries RPC Implementation */
	/* 1 Reply false if term < currentTerm (5.1) */
	if args.Term < rf.currentTerm {
		return
	}

	if args.PrevLogIndex < rf.log.Base {
		return
	}

	/* AppendEntries RPC Implementation */
	/* 2. Reply false if log doesn't contain an entry at pervLogIndex whose term matches pervLogTerm (5.3) */
	if rf.log.size() <= args.PrevLogIndex {
		return
	}
	if rf.log.get(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log.get(args.PrevLogIndex).Term
		for i := rf.log.Base + 1; i <= args.PrevLogIndex; i++ {	// 不是从1开始，snapshot就越界了
			if rf.log.get(i).Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
		return
	}

	for i := 0; i < len(args.Entries); i++ {
	// for i, e := range args.Entries {
		e := args.Entries[i]
		j := args.PrevLogIndex + i + 1
		if j >= rf.log.size() {
			/* AppendEntries RPC Implementation */
			/* 4. Append any new entries not already in the log */
			// rf.log = append(rf.log, e)
			rf.log.Entries = append(rf.log.Entries, e)
		} else {
			/* AppendEntries RPC Implementation */
			/* 3. If an existing entry conficts with a new one (same index but different terms), delete the existing entry and all that follow it (5.3) */
			if rf.log.get(j).Term != e.Term {
				rf.log.Entries = rf.log.Entries[:j-rf.log.Base]
				rf.log.Entries = append(rf.log.Entries, e)
			} else {
				// rf.log.set(j, e)
				rf.log.Entries[j-rf.log.Base] = e
			}
		}
	}

	/* AppendEntries RPC Implementation */
	/* 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) */
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.log.size()-1 < rf.commitIndex {
			rf.commitIndex = rf.log.size() - 1
		}
	}

	rf.lastRecv = time.Now()
	rf.persist()
	reply.Success = true
}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return
	}

	for id, peer := range rf.peers {
		if id == rf.me {
			continue
		}

		next := rf.nextIndex[id]
		if next <= rf.log.Base {
			go rf.sendSnapshot(id, peer, &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.log.Base,
				LastIncludedTerm:  rf.log.Entries[0].Term,
				Data:              rf.snapshot,
			})
			continue
		}
		// entries := make([]LogEntry, 0)
		// prevLogTerm := 0
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: next - 1,
			// PrevLogTerm:  prevLogTerm,
			// Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		args.PrevLogTerm = 0
		if next-1 < rf.log.size() {
			args.PrevLogTerm = rf.log.get(next - 1).Term
			args.Entries = append([]LogEntry(nil), rf.log.Entries[next-rf.log.Base:]...)
		}
		/* Rules for Leaders
		 * 3. If last log index >= nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
		 *    1) If successful: update nextIndex and matchIndex for follower (5.3)
		 *    2) If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (5.3)
		 */
		go func(id int, peer *labrpc.ClientEnd, args AppendEntriesArgs) {
			reply := AppendEntriesReply{}
			ok := peer.Call("Raft.AppendEntries", &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			/* Rules for All Servers */
			/* 2. If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (5.1) */
			if reply.Term > rf.currentTerm {
				rf.toFollower(reply.Term)
				return
			}

			if reply.Success {
				/* 1) If successful: update nextIndex and matchIndex for follower (5.3) */
				// rf.nextIndex[id] += len(args.Entries)
				// rf.matchIndex[id] = rf.nextIndex[id] - 1
				rf.nextIndex[id] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[id] = rf.nextIndex[id] - 1

				match := make([]int, len(rf.peers))
				copy(match, rf.matchIndex)
				sort.Ints(match)
				/* Rule for Leaders */
				/* 4. If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N, and log[N].Term == currentTerm: set commitIndex = N (5.3, 5.4) */
				majority := match[(len(rf.peers)-1)/2]
				if majority > rf.commitIndex && rf.log.get(majority).Term == rf.currentTerm {
					rf.commitIndex = majority
				}
			} else {
				/* 2) If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (5.3) */
				if reply.ConflictTerm == -1 {
					rf.nextIndex[id] = reply.ConflictIndex
				} else {
					conflictIndex := -1
					for i := args.PrevLogIndex; i > rf.log.Base; i-- { // not i > 0
						if rf.log.get(i).Term == reply.ConflictTerm {
							conflictIndex = i
							break
						}
					}
					if conflictIndex != -1 {
						rf.nextIndex[id] = conflictIndex + 1
					} else {
						rf.nextIndex[id] = reply.ConflictIndex
					}
				}
			}
		}(id, peer, args)
	}
	rf.persist()
}

func (rf *Raft) sync() {
	for !rf.killed() {
		rf.heartbeat()
		time.Sleep(rf.syncInterval())
	}
}
