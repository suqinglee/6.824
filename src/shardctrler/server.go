package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	mseq    map[int64]int64
	recv    map[int]chan Op
}

type Op struct {
	// Your data here.
	Act     string
	Cid     int64
	Seq     int64
	GIDs    []int
	Shard   int
	Num     int
	Servers map[int][]string
}

func (sc *ShardCtrler) GetConfig() Config {
	config := Config{
		Num:    sc.configs[len(sc.configs)-1].Num + 1,
		Shards: sc.configs[len(sc.configs)-1].Shards,
		Groups: make(map[int][]string),
	}
	for gid, group := range sc.configs[len(sc.configs)-1].Groups {
		config.Groups[gid] = append(config.Groups[gid], group...)
	}
	return config
}

func (sc *ShardCtrler) Join(servers map[int][]string) {
	// Your code here.
	// fmt.Printf("%v Join %v\n", sc.me, servers)
	config := sc.GetConfig()
	for gid, group := range servers {
		config.Groups[gid] = append([]string(nil), group...)
	}
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) Leave(gids []int) {
	// Your code here.
	// fmt.Printf("%v Leave %v\n", sc.me, gids)
	config := sc.GetConfig()
	for _, gid := range gids {
		delete(config.Groups, gid)
	}
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) Move(shard int, gid int) {
	// Your code here.
	// fmt.Printf("%v Move %v to %v", sc.me, shard, gid)
	config := sc.GetConfig()
	config.Shards[shard] = gid
	sc.configs = append(sc.configs, config)
}

// func (sc *ShardCtrler) Query() {
// 	// Your code here.
// }

func (sc *ShardCtrler) Request(args *Args, reply *Reply) {
	sc.mu.Lock()
	if args.Num == -1 || args.Num >= len(sc.configs) {
		args.Num = len(sc.configs) - 1
	}
	if args.Seq <= sc.mseq[args.Cid] {
		reply.Err = OK
		reply.Config = sc.configs[args.Num]
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	index, _, isLeader := sc.rf.Start(Op{
		Act:     args.Act,
		Cid:     args.Cid,
		Seq:     args.Seq,
		GIDs:    args.GIDs,
		Shard:   args.Shard,
		Num:     args.Num,
		Servers: args.Servers,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	sc.mu.Lock()
	ch := make(chan Op)
	sc.recv[index] = ch
	sc.mu.Unlock()

	select {
	case op := <-ch:
		if op.Cid != args.Cid || op.Seq != args.Seq {
			reply.Err = ErrRetry
		} else {
			reply.Err = OK
			sc.mu.Lock()
			reply.Config = sc.configs[op.Num]
			sc.mu.Unlock()
		}
	case <-time.After(1 * time.Second):
		reply.Err = ErrRetry
	}

	sc.mu.Lock()
	close(sc.recv[index])
	delete(sc.recv, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Update() {
	for {
		msg := <-sc.applyCh
		if !msg.CommandValid {
			continue
		}
		index := msg.CommandIndex
		op := msg.Command.(Op)
		sc.mu.Lock()
		if op.Seq > sc.mseq[op.Cid] {
			sc.mseq[op.Cid] = op.Seq
			switch op.Act {
			case Join:
				sc.Join(op.Servers)
				sc.Balance()
			case Leave:
				sc.Leave(op.GIDs)
				sc.Balance()
			case Move:
				sc.Move(op.Shard, op.GIDs[0])
				sc.Balance()
			}
		}
		if _, ok := sc.recv[index]; ok {
			sc.recv[index] <- op
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) Balance() {
	config := sc.configs[len(sc.configs)-1]
	if len(config.Groups) == 0 {
		return
	}
	// fmt.Printf("%v Balance %v\n", sc.me, config)
	m_gid_shardnum := make(map[int]int)
	unassigned := make([]int, 0)
	// groupNum := len(config.Groups)
	avgShardNum := NShards / len(config.Groups)

	for gid := range config.Groups {
		m_gid_shardnum[gid] = 0
	}
	for shard, gid := range config.Shards {
		if _, ok := config.Groups[gid]; !ok {
			unassigned = append(unassigned, shard)
			continue
		}
		if m_gid_shardnum[gid] < avgShardNum {
			m_gid_shardnum[gid]++
		} else {
			unassigned = append(unassigned, shard)
		}
	}

	s_gid_shardnum := make([][2]int, 0)
	for gid, shardnum := range m_gid_shardnum {
		s_gid_shardnum = append(s_gid_shardnum, [2]int{gid, shardnum})
	}

	sort.Slice(s_gid_shardnum, func(i, j int) bool {
		if s_gid_shardnum[i][1] == s_gid_shardnum[j][1] {
			return s_gid_shardnum[i][0] < s_gid_shardnum[j][0]
		}
		return s_gid_shardnum[i][1] < s_gid_shardnum[j][1]
	})

	// fmt.Println(s_gid_shardnum)

	base := 0
	for _, v := range s_gid_shardnum {
		gid := v[0]
		shardnum := v[1]
		for i := 0; i < avgShardNum-shardnum; i++ {
			config.Shards[unassigned[base+i]] = gid
		}
		if avgShardNum-shardnum > 0 {
			base += avgShardNum - shardnum
		}
	}

	for _, v := range s_gid_shardnum {
		if base >= len(unassigned) {
			break
		}
		gid := v[0]
		config.Shards[unassigned[base]] = gid
		base++
	}
	sc.configs[len(sc.configs)-1].Shards = config.Shards
	// fmt.Printf("%v Balance %v\n", sc.me, config)
}

func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.mseq = make(map[int64]int64)
	sc.recv = make(map[int]chan Op)

	go sc.Update()

	return sc
}
