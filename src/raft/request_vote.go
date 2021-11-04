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

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	/* 1. RequestVote RPC Implementation */
	/* 2. Rules for All Servers */
	// 2.1 If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (5.3)

	// 1.1 Reply false if term < currentTerm (5.1)
	if args.Term < rf.currentTerm {
		return
	}

	// 2.2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (5.1)
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}
	reply.Term = rf.currentTerm

	// 1.2 If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote (5.2 5.4)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}

	rf.lastRecv = time.Now()
}

func (rf *Raft) elect() {
	rf.toCandidate()
	cond := sync.NewCond(&rf.mu)
	voteCount := 1
	totalCount := 1

	for id, peer := range rf.peers {
		if id == rf.me {
			continue
		}
		go func(peer *labrpc.ClientEnd, args *RequestVoteArgs) {
			reply := RequestVoteReply{}
			peer.Call("Raft.RequestVote", args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			totalCount += 1
			// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (5.1)
			if reply.Term > rf.currentTerm {
				rf.toFollower(reply.Term)
			} else if reply.VoteGranted {
				voteCount += 1
			}

			cond.Broadcast()
		}(peer, &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me})
	}

	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		for voteCount <= len(rf.peers)/2 && totalCount < len(rf.peers) && rf.role == Candidate {
			cond.Wait()
		}
		if voteCount > len(rf.peers)/2 && rf.role == Candidate {
			rf.toLeader()
			go rf.sync()
		}
	}()
}
