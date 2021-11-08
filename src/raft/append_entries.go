package raft

import (
	"sort"
	"time"

	"6.824/labrpc"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

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
	reply.ConflictIndex = len(rf.log)
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

	/* AppendEntries RPC Implementation */
	/* 2. Reply false if log doesn't contain an entry at pervLogIndex whose term matches pervLogTerm (5.3) */
	// if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
	// 	return
	// }
	if len(rf.log) <= args.PrevLogIndex {
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		for i := 1; i <= args.PrevLogIndex; i++ {
			if rf.log[i].Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
		return
	}

	for i, e := range args.Entries {
		j := args.PrevLogIndex + i + 1
		if j >= len(rf.log) {
			/* AppendEntries RPC Implementation */
			/* 4. Append any new entries not already in the log */
			rf.log = append(rf.log, e)
		} else {
			/* AppendEntries RPC Implementation */
			/* 3. If an existing entry conficts with a new one (same index but different terms), delete the existing entry and all that follow it (5.3) */
			if rf.log[j].Term != e.Term {
				rf.log = rf.log[:j]
				rf.log = append(rf.log, e)
			} else {
				rf.log[j] = e
			}
		}
	}

	/* AppendEntries RPC Implementation */
	/* 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) */
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if len(rf.log)-1 < rf.commitIndex {
			rf.commitIndex = len(rf.log) - 1
		}
	}

	rf.persist()
	reply.Success = true
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

				next := rf.nextIndex[id]
				entries := make([]LogEntry, 0)
				prevLogTerm := 0
				if next-1 < len(rf.log) {
					prevLogTerm = rf.log[next-1].Term
					entries = rf.log[next:]
				}
				/* Rules for Leaders
				 * 3. If last log index >= nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
				 *    1) If successful: update nextIndex and matchIndex for follower (5.3)
				 *    2) If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (5.3)
				 */
				go func(id int, peer *labrpc.ClientEnd, args *AppendEntriesArgs) {
					reply := AppendEntriesReply{}
					ok := peer.Call("Raft.AppendEntries", args, &reply)
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
						if majority > rf.commitIndex && rf.log[majority].Term == rf.currentTerm {
							rf.commitIndex = majority
						}
					} else {
						/* 2) If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (5.3) */
						// rf.nextIndex[id] = args.PrevLogIndex
						// if reply.ConflictIndex != -1 {
						// 	rf.nextIndex[id] = reply.ConflictIndex
						// }
						// if rf.nextIndex[id] < 1 {
						// 	rf.nextIndex[id] = 1
						// }

						if reply.ConflictTerm == -1 {
							rf.nextIndex[id] = reply.ConflictIndex
						} else {
							conflictIndex := -1
							for i := args.PrevLogIndex; i > 0; i-- {
								if rf.log[i].Term == reply.ConflictTerm {
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
				}(id, peer, &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: next - 1,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				})
			}
			rf.persist()
		}()
		time.Sleep(rf.syncInterval())
	}
}
