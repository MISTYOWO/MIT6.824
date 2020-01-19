package shardkv

// import "shardmaster"
import (
	"fmt"
	"labrpc"
	"shardmaster"
	"time"
)
import "raft"
import "sync"
import "labgob"

type ShardKV struct {
	mu            sync.Mutex
	me            int
	rf            *raft.Raft
	applyCh       chan raft.ApplyMsg
	make_end      func(string) *labrpc.ClientEnd
	gid           int
	masters       []*labrpc.ClientEnd
	maxraftstate  int // snapshot if log grows this big
	config        shardmaster.Config
	historyConfig []shardmaster.Config
	data          map[string]string
	cache         map[int64]string
	notifyChanMap map[int]chan notifyArgs
	ownShards     map[int]struct{}
}
type notifyArgs struct {
	Term  int
	Value string
	Err   Err
}

const startTimeOutInterval = time.Duration(3 * time.Second)

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	reply.Err, reply.Value = kv.start(args.ConfigNum, args.copy())
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.Err, _ = kv.start(args.ConfigNum, args.copy())
}

func (kv *ShardKV) start(configNum int, args interface{}) (Err, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if configNum != kv.config.Num {
		return ErrWrongGroup, ""
	}
	index, term, isleader := kv.rf.Start(args)
	if !isleader {
		return ErrWrongLeader, ""
	}
	notifych := make(chan notifyArgs, 1)
	kv.notifyChanMap[index] = notifych
	kv.mu.Unlock()
	select {
	case <-time.After(startTimeOutInterval):
		fmt.Printf("timeout ....................................\n")
		kv.mu.Lock()
		delete(kv.notifyChanMap, index)
		return ErrWrongLeader, ""
	case result := <-notifych:
		kv.mu.Lock()
		if result.Term != term {
			return ErrWrongLeader, ""
		} else {
			return result.Err, result.Value
		}
	}
	return OK, ""
}
func (kv *ShardKV) applyNewConf(newConfig shardmaster.Config) {
	if newConfig.Num <= kv.config.Num {
		return
	}
	oldConfig, oldShards := kv.config, kv.ownShards
	kv.historyConfigs = append(kv.historyConfig, oldConfig.Copy())
	kv.ownShards, kv.config = make(map[int]struct{}), newConfig.Copy
	for shard, newGID := range newConfig.Shards {
		if newGID == kv.gid {
			if _, ok := oldShards[shard]; ok || oldConfig.Num == 0 {
				kv.ownShards[shard] = struct{}{}
				delete(oldShards, shard)
			} else {
				kv.waitingShards[shard] = oldConfig.Num
			}
		}
	}
	if len(oldShards) != 0 { // prepare data that needed migration
		v := make(map[int]MigrationData)
		for shard := range oldShards {
			data := MigrationData{Data: make(map[string]string), Cache: make(map[int64]string)}
			for k, v := range kv.data {
				if key2shard(k) == shard {
					data.Data[k] = v
					delete(kv.data, k)
				}
			}
			for k, v := range kv.cache {
				if key2shard(v) == shard {
					data.Cache[k] = v
					delete(kv.cache, k)
				}
			}
			v[shard] = data
		}
		kv.migratingShards[oldConfig.Num] = v
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}
func (kv *ShardKV) apply(msg raft.ApplyMsg) {
	result := notifyArgs{
		Term:  msg.CommandTerm,
		Value: "",
		Err:   OK,
	}
	if args, ok := msg.Command.(GetArgs); ok {
		// shard := key2shard(args.Key)
		fmt.Printf("apply get in last ..................\n")
		if args.ConfigNum != kv.config.Num {
			result.Err = ErrWrongGroup
		} else {
			result.Value = kv.data[args.Key]
		}
	} else if args, ok := msg.Command.(PutAppendArgs); ok {
		fmt.Printf("apply put and get in last ..................\n")

		// shard := key2shard(args.Key)
		if args.ConfigNum != kv.config.Num {
			result.Err = ErrWrongGroup
		} else if _, ok := kv.cache[args.RequestId]; !ok {
			if args.Op == "Put" {
				kv.data[args.Key] = args.Value
			} else if args.Op == "Append" {
				kv.data[args.Key] += args.Value
			}
			delete(kv.cache, args.LastRequestId)
			kv.cache[args.RequestId] = args.Key
		}
	}
	if ch, ok := kv.notifyChanMap[msg.CommandIndex]; ok {
		ch <- result
		delete(kv.notifyChanMap, msg.CommandIndex)
	}
}
func (kv *ShardKV) run() {
	for {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			if msg.CommandValid {
				kv.apply(msg)
			}
			kv.mu.Unlock()
		}
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.ownShards = make(map[int]struct{})
	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.config = shardmaster.Config{}
	kv.historyConfig = make([]shardmaster.Config, 0)
	kv.data = make(map[string]string)
	kv.cache = make(map[int64]string)
	kv.notifyChanMap = make(map[int]chan notifyArgs)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.run()
	return kv
}
