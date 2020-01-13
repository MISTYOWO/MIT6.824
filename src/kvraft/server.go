package raftkv

import (
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// type Op struct {
// 	Operation string

// 	// Your definitions here.
// 	// Field names must start with capital letters,
// 	// otherwise RPC will break.

// }

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	notifiedMap  map[int]chan notifyArgs
	processed    map[int64]int
	maxraftstate int // snapshot if log grows this big
	data         map[string]string
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.if
	fmt.Printf("%d recive the call int get \n", kv.me)

	index, term, isleader := kv.rf.Start(args.copy())
	notifyArgschan := make(chan notifyArgs)
	kv.mu.Lock()
	kv.notifiedMap[index] = notifyArgschan
	kv.mu.Unlock()
	if !isleader {
		reply.WrongLeader = true
		reply.Err = "err"
	} else {
		select {
		case <-time.After(timeOut):
			reply.Err = "timeOut"
			kv.mu.Lock()
			delete(kv.notifiedMap, index)
			kv.mu.Unlock()
		case result := <-notifyArgschan:
			if result.Term != term {
				reply.Err = ErrNoleader
				reply.WrongLeader = true
			} else {
				reply.Value = result.Value
				reply.Err = OK
			}
		}
	}
	return
}
func (args *GetArgs) copy() GetArgs {
	return GetArgs{args.Key}
}
func (args *PutAppendArgs) copy() PutAppendArgs {
	return PutAppendArgs{args.Key, args.Value, args.Op, args.ClerkId, args.RequestNumber}
}
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	notifyArgschan := make(chan notifyArgs, 1)
	index, term, isleader := kv.rf.Start(args.copy())

	if !isleader {
		// fmt.Printf("%d recive the call but not leader from %d int put \n", kv.me, args.ClerkId)
		reply.WrongLeader = true
		reply.Err = ErrNoleader
	} else {
		fmt.Printf("%d recive the call as leader from %d int put \n", kv.me, args.ClerkId)
		kv.mu.Lock()
		kv.notifiedMap[index] = notifyArgschan
		kv.mu.Unlock()
		select {
		case <-time.After(timeOut):
			fmt.Printf("......................timeout in put and app.................................\n")
			reply.Err = "timeout"
			kv.mu.Lock()
			delete(kv.notifiedMap, index)
			kv.mu.Unlock()
		case result := <-notifyArgschan:
			if term != result.Term {
				fmt.Printf("......................ErrNoleader in put and app.................................\n")

				reply.Err = ErrNoleader
			} else {
				reply.Err = result.Err

			}
		}
	}
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
type notifyArgs struct {
	Term  int
	Value string
	Err   Err
}

func (kv *KVServer) apply(msg raft.ApplyMsg) {
	notified := &notifyArgs{Term: msg.CommandTerm, Value: "", Err: OK}
	if args, ok := msg.Command.(GetArgs); ok {
		notified.Value = kv.data[args.Key]
		fmt.Printf("apply get .....................................................\n")
	} else if args, ok := msg.Command.(PutAppendArgs); ok {
		if kv.processed[args.ClerkId] < args.RequestNumber {
			if args.Op == "Put" {
				fmt.Printf("apply put .....................................................\n")
				kv.data[args.Key] = args.Value
			} else if args.Op == "Append" {
				kv.data[args.Key] += args.Value
				fmt.Printf("apply Append .....................................................\n")

			}
			kv.processed[args.ClerkId] = args.RequestNumber
		}
	} else {
		fmt.Printf("ErrNoleader .....................................................\n")
		notified.Err = ErrNoleader
	}

	kv.notifyAns(msg.CommandIndex, *notified)
}
func (kv *KVServer) notifyAns(index int, result notifyArgs) {
	if notifyArgschan, ok := kv.notifiedMap[index]; ok {
		delete(kv.notifiedMap, index)
		notifyArgschan <- result
	}
}
func (kv *KVServer) run() {
	for {
		select {
		case msg := <-kv.applyCh:

			fmt.Printf("get msg in run .........................................................\n")
			if msg.CommandValid {
				kv.mu.Lock()

				kv.apply(msg)
				kv.mu.Unlock()

			}

		}
	}
}
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.processed = make(map[int64]int)
	kv.notifiedMap = make(map[int]chan notifyArgs)
	kv.data = make(map[string]string)
	// You may need initialization code here.go
	go kv.run()
	return kv
}
