package raft

import (
	"math/rand"

	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

const (
	Leader    = 0
	Candidate = 1
	Follower  = 2
)

type Raft struct {
	mu    sync.Mutex
	peers []*labrpc.ClientEnd
	me    int
	dead  int32

	currentTerm int
	votedFor    int
	log         []*LogEntry

	role             int
	lastReceiveLog   time.Time
	lastBroadcastLog time.Time
}

type LogEntry struct {
	Term int
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type AppendEntriesArgs struct {
	Term int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesReply struct {
	Term int
}

func (rf *Raft) lastLogTerm() int {
	return 0
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm < rf.lastLogTerm() || args.LastLogIndex < len(rf.log) {
			return
		}
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastReceiveLog = time.Now()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
	}
	rf.lastReceiveLog = time.Now()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) sync() {
	for !rf.killed() {
		time.Sleep(1 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.role != Leader || time.Since(rf.lastBroadcastLog) < 100*time.Millisecond {
				return
			}
			rf.lastBroadcastLog = time.Now()

			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go func(peerId int, args *AppendEntriesArgs) {
					reply := AppendEntriesReply{}
					if ok := rf.sendAppendEntries(peerId, args, &reply); ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if reply.Term > rf.currentTerm {
							rf.role = Follower
							rf.currentTerm = reply.Term
							rf.votedFor = -1
						}
					}
				}(i, &AppendEntriesArgs{Term: rf.currentTerm})
			}
		}()
	}
}

func (rf *Raft) election() {
	for !rf.killed() {
		time.Sleep(1 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.role == Leader || time.Since(rf.lastReceiveLog) < time.Duration(200+rand.Int31n(150))*time.Millisecond {
				return
			}

			rf.role = Candidate
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.lastReceiveLog = time.Now()

			voteCount := 1
			maxTerm := rf.currentTerm
			finishCount := 1

			rf.mu.Unlock()
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go func(peerId int, args *RequestVoteArgs) {
					reply := RequestVoteReply{}
					if ok := rf.sendRequestVote(peerId, args, &reply); ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if reply.VoteGranted {
							voteCount++
						}
						if reply.Term > maxTerm {
							maxTerm = reply.Term
						}
					}
					finishCount++
				}(i, &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: len(rf.log), LastLogTerm: rf.lastLogTerm()})
			}

			for finishCount < len(rf.peers) && voteCount <= len(rf.peers)/2 {
				time.Sleep(1 * time.Millisecond)
			}

			rf.mu.Lock()

			if rf.role != Candidate {
				return
			}
			if maxTerm > rf.currentTerm {
				rf.role = Follower
				rf.currentTerm = maxTerm
				rf.votedFor = -1
				return
			}
			if voteCount > len(rf.peers)/2 {
				rf.role = Leader
				rf.lastBroadcastLog = time.Unix(0, 0)
				return
			}

			/*

					// 请求投票req
					args := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.log),
					}
					if len(rf.log) != 0 {
						args.LastLogTerm = rf.log[len(rf.log)-1].Term
					}

					rf.mu.Unlock()

					// 并发RPC请求vote
					type VoteResult struct {
						peerId int
						resp   *RequestVoteReply
					}
					voteCount := 1   // 收到投票个数（先给自己投1票）
					finishCount := 1 // 收到应答个数
					voteResultChan := make(chan *VoteResult, len(rf.peers))
					for peerId := 0; peerId < len(rf.peers); peerId++ {
						go func(id int) {
							if id == rf.me {
								return
							}
							resp := RequestVoteReply{}
							if ok := rf.sendRequestVote(id, &args, &resp); ok {
								voteResultChan <- &VoteResult{peerId: id, resp: &resp}
							} else {
								voteResultChan <- &VoteResult{peerId: id, resp: nil}
							}
						}(peerId)
					}

					maxTerm := 0
					for {
						select {
						case voteResult := <-voteResultChan:
							finishCount += 1
							if voteResult.resp != nil {
								if voteResult.resp.VoteGranted {
									voteCount += 1
								}
								if voteResult.resp.Term > maxTerm {
									maxTerm = voteResult.resp.Term
								}
							}
							// 得到大多数vote后，立即离开
							if finishCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
								goto VOTE_END
							}
						}
					}
				VOTE_END:
					rf.mu.Lock()
					// 如果角色改变了，则忽略本轮投票结果
					if rf.role != Candidate {
						return
					}
					// 发现了更高的任期，切回follower
					if maxTerm > rf.currentTerm {
						rf.role = Follower
						rf.currentTerm = maxTerm
						rf.votedFor = -1
						return
					}
					// 赢得大多数选票，则成为leader
					if voteCount > len(rf.peers)/2 {
						rf.role = Leader
						rf.lastBroadcastLog = time.Unix(0, 0) // 令appendEntries广播立即执行
						return
					}
			*/
		}()
	}
}

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

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = Follower
	rf.votedFor = -1
	rf.lastReceiveLog = time.Now()

	go rf.election()
	go rf.sync()

	return rf
}
