package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key           string
	Value         string
	Op            string // "Put" or "Append"
	Clientid      int64
	RequestId     int64
	LastRequestId int64
	ConfigNum     int
}

func (args *PutAppendArgs) copy() PutAppendArgs {
	return PutAppendArgs{
		Key:           args.Key,
		Value:         args.Value,
		Op:            args.Op,
		Clientid:      args.Clientid,
		RequestId:     args.RequestId,
		LastRequestId: args.LastRequestId,
		ConfigNum:     args.ConfigNum,
	}
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key       string
	ConfigNum int
}

func (args *GetArgs) copy() GetArgs {
	return GetArgs{
		Key:       args.Key,
		ConfigNum: args.ConfigNum,
	}
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
