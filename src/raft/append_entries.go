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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d]: received append entry from [%d], args term: %d, LeaderCommit: %d, prevLogIndex: %d, prevLogTerm: %d, len(entry): %d",
		rf.me, args.LeaderId, args.Term, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))

	reply.Success = false

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
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
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
		DPrintf("[%d]: commit index [%d]", rf.me, rf.commitIndex)
	}

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

				next := rf.nextIndex[id]
				if len(rf.peers)-1 < next {
					// DPrintf("pong pong pong")
				} else {
					DPrintf("[%d]: len of log: %d, next index of [%d]: %d", rf.me, len(rf.log), id, rf.nextIndex[id])
				}
				/* Rules for Leaders
				 * 3. If last log index >= nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
				 *    1) If successful: update nextIndex and matchIndex for follower (5.3)
				 *    2) If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (5.3)
				 */
				go func(id int, peer *labrpc.ClientEnd, args *AppendEntriesArgs) {
					reply := AppendEntriesReply{}
					peer.Call("Raft.AppendEntries", args, &reply)

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
						rf.nextIndex[id] += len(args.Entries)
						rf.matchIndex[id] = rf.nextIndex[id] - 1

						match := make([]int, len(rf.peers))
						copy(match, rf.matchIndex)
						sort.Ints(match)
						/* Rule for Leaders */
						/* 4. If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N, and log[N].Term == currentTerm: set commitIndex = N (5.3, 5.4) */
						majority := match[(len(rf.peers)-1)/2]
						if majority > rf.commitIndex && rf.log[majority].Term == rf.currentTerm {
							rf.commitIndex = majority
							DPrintf("[%d]: commit index [%d]", rf.me, rf.commitIndex)
						}
					} else {
						/* 2) If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (5.3) */
						if rf.nextIndex[id] > 1 {
							rf.nextIndex[id] -= 1
						}
					}
				}(id, peer, &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: next - 1,
					PrevLogTerm:  rf.log[next-1].Term,
					Entries:      rf.log[next:],
					LeaderCommit: rf.commitIndex,
				})
			}
		}()
		time.Sleep(rf.syncInterval())
	}
}
