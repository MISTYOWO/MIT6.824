package raftkv

import (
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers       []*labrpc.ClientEnd
	leader        int
	requestNumber int
	id            int64
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

const retryInterval = time.Duration(150 * time.Millisecond)
const timeOut = time.Duration(1000 * time.Millisecond)

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = nrand()
	ck.requestNumber = 0
	ck.leader = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{key}
	for {
		var reply GetReply
		if ck.servers[ck.leader].Call("KVServer.Get", &args, &reply) && reply.Err == OK {
			return reply.Value
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(retryInterval)
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.requestNumber++

	args := PutAppendArgs{
		key,
		value,
		op,
		ck.id,
		ck.requestNumber,
	}
	var reply PutAppendReply
	for {
		if ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply) && reply.Err == OK {
			// fmt.Printf(".........................................................................................\n")
			return
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(retryInterval)
	}
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
