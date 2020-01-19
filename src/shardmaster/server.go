package shardmaster

import (
	// "fmt"
	"raft"
	"time"
)
import "labrpc"
import "sync"
import "labgob"

type ShardMaster struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	processed map[int64]int
	notifyMap map[int]chan notifyAgrs
	configs   []Config // indexed by config num
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	reply.Err, _ = sm.start(args.copy())
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	reply.Err, _ = sm.start(args.copy())
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	reply.Err, _ = sm.start(args.copy())
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.Err = WrongLeader
		return
	}
	sm.mu.Lock()
	if args.Num < 0 || args.Num >= len(sm.configs) {
		sm.mu.Unlock()
		error, config := sm.start(args.copy())
		reply.Err = error
		if val, ok := config.(Config); ok && error == OK {
			reply.Config = val
		}
	} else {
		reply.Err, reply.Config = OK, sm.getConfig(args.Num)
		sm.mu.Unlock()
	}
}
func (sm *ShardMaster) getConfig(i int) Config {
	var config Config
	if i < 0 || i >= len(sm.configs) {
		config = sm.configs[len(sm.configs)-1]
	} else {
		config = sm.configs[i]
	}
	replyConfig := Config{
		Num:    config.Num,
		Shards: config.Shards,
		Groups: make(map[int][]string),
	}
	for gid, servers := range config.Groups {
		replyConfig.Groups[gid] = append([]string{}, servers...)
	}
	return replyConfig
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}
func (sm *ShardMaster) start(args interface{}) (Err, interface{}) {
	index, term, ok := sm.rf.Start(args)
	if !ok {
		return WrongLeader, struct{}{}
	}
	sm.mu.Lock()
	notityCh := make(chan notifyAgrs, 1)
	sm.notifyMap[index] = notityCh
	sm.mu.Unlock()
	select {
	case <-time.After(5 * time.Second):
		// fmt.Printf("time out .............................\n")
		sm.mu.Lock()
		delete(sm.notifyMap, index)
		sm.mu.Unlock()
		return WrongLeader, struct{}{}
	case result := <-notityCh:
		if result.Term != term {
			// fmt.Printf("term change .............................\n")
			return WrongLeader, struct{}{}
		} else {
			// fmt.Printf("start ok .............................\n")

			return OK, result.Args
		}
	}
	return OK, struct{}{}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func (sm *ShardMaster) run() {
	for {
		select {
		case msg := <-sm.applyCh:
			sm.mu.Lock()
			if msg.CommandValid {
				sm.apply(msg)
			} else if cmd, ok := msg.Command.(string); ok && cmd == "NewLeader" {
				sm.rf.Start("")
			}
			sm.mu.Unlock()
		}
	}
}

func (sm *ShardMaster) apply(msg raft.ApplyMsg) {
	reply := notifyAgrs{
		Term: msg.CommandTerm,
		Args: "",
	}
	if Args, ok := msg.Command.(JoinArgs); ok {
		sm.applyJoin(Args)
	} else if Args, ok := msg.Command.(LeaveArgs); ok {
		sm.applyLeave(Args)
	} else if Args, ok := msg.Command.(MoveArgs); ok {
		sm.applyMove(Args)
	} else if Args, ok := msg.Command.(QueryArgs); ok {
		reply.Args = sm.applyQuery(Args)
	}
	sm.notifyOk(msg.CommandIndex, reply)

}
func (sm *ShardMaster) applyJoin(Args JoinArgs) {
	if sm.processed[Args.ClientId] < Args.RequestNum {
		newconfig := sm.getConfig(-1)
		Gids := make([]int, 0)
		for gid, server := range Args.Servers {
			if s, ok := newconfig.Groups[gid]; ok {
				newconfig.Groups[gid] = append(s, server...)
			} else {
				newconfig.Groups[gid] = append([]string{}, server...)
				Gids = append(Gids, gid)
			}
		}
		if len(newconfig.Groups) == 0 {
			newconfig.Shards = [NShards]int{}
		} else if len(newconfig.Groups) < NShards {
			shardGid := make(map[int]int)
			var minShard, maxShard int
			minShard = NShards / len(newconfig.Groups)
			leftShard := NShards % len(newconfig.Groups)
			if leftShard != 0 {
				maxShard = minShard + 1
			} else {
				maxShard = minShard
			}
			for i, j := 0, 0; i < NShards; i++ {
				gid := newconfig.Shards[i]
				if gid == 0 ||
					(minShard == maxShard && shardGid[gid] == minShard) ||
					(minShard < maxShard && shardGid[gid] == minShard && leftShard <= 0) {
					newGids := Gids[j]
					newconfig.Shards[i] = newGids
					shardGid[newGids] += 1
					j = (j + 1) % len(Gids)
				} else {
					shardGid[gid] += 1
					if shardGid[gid] == minShard {
						leftShard -= 1
					}
				}
			}
		}
		sm.processed[Args.ClientId] = Args.RequestNum
		sm.appendConfig(newconfig)

	}
}
func (sm *ShardMaster) appendConfig(newconfig Config) {
	newconfig.Num = len(sm.configs)
	sm.configs = append(sm.configs, newconfig)
}
func (sm *ShardMaster) applyLeave(Args LeaveArgs) {
	if sm.processed[Args.ClientId] < Args.RequestNum {
		newConfig := sm.getConfig(-1)
		leaveGids := make(map[int]struct{})
		for _, gid := range Args.GIDs {
			delete(newConfig.Groups, gid)
			leaveGids[gid] = struct{}{}
		}
		if len(newConfig.Groups) == 0 {
			newConfig.Shards = [NShards]int{}
		} else {
			remainsGids := make([]int, 0)
			for gid := range newConfig.Groups {
				remainsGids = append(remainsGids, gid)
			}
			shardPerGid := NShards / len(newConfig.Groups)
			if shardPerGid < 1 {
				shardPerGid = 1
			}
			shardsNumGid := make(map[int]int)
		loop:
			for i, j := 0, 0; i < NShards; i++ {
				gid := newConfig.Shards[i]
				if _, ok := leaveGids[gid]; ok || shardsNumGid[gid] == shardPerGid {
					for _, id := range remainsGids {
						count := shardsNumGid[id]
						if count < shardPerGid {
							newConfig.Shards[i] = id
							shardsNumGid[id] += 1
							continue loop
						}
					}
					// loop for more balanced for every group
					id := remainsGids[j]
					j = (j + 1) % len(remainsGids)
					newConfig.Shards[i] = id
					shardsNumGid[id] += 1
				} else {
					shardsNumGid[gid] += 1
				}
			}
		}
		sm.processed[Args.ClientId] = Args.RequestNum
		sm.appendConfig(newConfig)
	}
}

func (sm *ShardMaster) applyMove(Args MoveArgs) {
	if sm.processed[Args.ClientId] < Args.RequestNum {
		newConfig := sm.getConfig(-1)
		newConfig.Shards[Args.Shard] = Args.GID
		sm.processed[Args.ClientId] = Args.RequestNum
		sm.appendConfig(newConfig)
	}
	return
}
func (sm *ShardMaster) applyQuery(Args QueryArgs) Config {
	return sm.getConfig(Args.Num)
}
func (sm *ShardMaster) notifyOk(index int, reply notifyAgrs) {
	if ch, ok := sm.notifyMap[index]; ok {
		ch <- reply
		delete(sm.notifyMap, index)
	}
}
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	labgob.Register(JoinArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(QueryArgs{})
	// labgob.Register(Config{})

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.processed = make(map[int64]int)
	sm.notifyMap = make(map[int]chan notifyAgrs)

	go sm.run()
	return sm
}

type notifyAgrs struct {
	Term int
	Args interface{}
}
