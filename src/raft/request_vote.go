package raft

import (
	"sync"
	"time"

	"6.824/labrpc"
)

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d]: received vote request from [%d]", rf.me, args.CandidateId)

	reply.VoteGranted = false

	/* Rules for All Servers */
	/* 1. If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (5.3) */

	/* Rules for All Servers */
	/* 2. If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (5.1) */
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}
	reply.Term = rf.currentTerm

	/* RequestVote RPC Implementation */
	/* 1. Reply false if term < currentTerm (5.1) */
	if args.Term < rf.currentTerm {
		return
	}
	DPrintf("[%d]: term [%d], state [%s], vote for [%d]", rf.me, rf.currentTerm, rf.role, rf.votedFor)

	/* RequestVote RPC Implementation */
	/* 2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote (5.2 5.4) */
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term || args.LastLogIndex >= len(rf.log)-1 && rf.log[len(rf.log)-1].Term == args.LastLogTerm {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			DPrintf("[%d]: voted to [%d]", rf.me, args.CandidateId)
		}
	}

	rf.lastRecv = time.Now()
}

func (rf *Raft) elect() {
	/* Rules for Candidates
	 * 1. On conversion to candidate, start election:
	 *    1) Increment currentTerm
	 *    2) vote for self
	 *    3) Reset election timer
	 *    4) Send RequestVote RPCs to all other servers
	 */
	rf.toCandidate()
	voteCount := 1
	totalCount := 1

	cond := sync.NewCond(&rf.mu)
	for id, peer := range rf.peers {
		if id == rf.me {
			continue
		}
		DPrintf("[%d]: term: [%d], send request vote to: [%d]", rf.me, rf.currentTerm, id)
		go func(peer *labrpc.ClientEnd, args *RequestVoteArgs) {
			reply := RequestVoteReply{}
			peer.Call("Raft.RequestVote", args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			totalCount += 1
			/* Rule for All Servers */
			/* 2. If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (5.1) */
			if reply.Term > rf.currentTerm {
				rf.toFollower(reply.Term)
			} else if reply.VoteGranted {
				voteCount += 1
			}

			cond.Broadcast()
		}(peer, &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.log) - 1,
			LastLogTerm:  rf.log[len(rf.log)-1].Term,
		})
	}

	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		for voteCount <= len(rf.peers)/2 && totalCount < len(rf.peers) && rf.role == Candidate {
			cond.Wait()
		}
		/* Rules for Candidates */
		/* 2. If votes received from majority of servers: become leader */
		if voteCount > len(rf.peers)/2 && rf.role == Candidate {
			rf.toLeader()
			/* Rules for Leaders */
			/* 1. Upon election: send inital empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts (5.2) */
			go rf.sync()
		}
	}()
}
