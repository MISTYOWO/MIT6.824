package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "fmt"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
const ApplyMsgInterval = time.Duration(100 * time.Millisecond)
const ElectionTime = time.Duration(1000 * time.Millisecond)

type ApplyMsg struct {
	CommandValid bool
	CommandIndex int
	CommandTerm  int
	Command      interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	leaderId  int

	currentTerm int
	votedFor    int
	logs        []logEntry

	commitIndex int
	lastApplied int

	nextIndex  []int // the index of next log should send to server
	matchIndex []int // the index of commited log of each server

	voteNum      int
	status       string
	lastLogIndex int
	lastLogTerm  int

	applyCh       chan ApplyMsg
	//notifyapplyCh chan struct{}
	kill          chan int
	Timeout       *time.Timer
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.\
	//Persistent state on all servers:
	//latest term server has seen (initialized to 0 on first boot, increases monotonically)
	//candidateId that candidateId that received vote in current term (or null if none)
	//log entries; each entry contains command
	// for state machine, and term when entry
	// was received by leader (first index is 1)
	//Volatile state on all servers:
	//index of highest log entry known to be
	//committed (initialized to 0, increases
	//monotonically)
	//index of highest log entry applied to state
	// machine (initialized to 0, increases
	// monotonically)
	//volatile state on leaders
	// for each server, index of the next log entry
	// to send to that server (initialized to leader
	// last log index + 1)
	// for each server, index of highest log entry
	// known to be replicated on server
	// (initialized to 0, increases monotonically)
	//vote num
	//status : leader follower candidate

}
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Success bool
	Term    int
}
type logEntry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	//current term
	Rpcok  bool
	Server int
	Term   int
	//true or false
	VoteGranted bool
}
type ApplyMsgReply struct {
	Ans  bool
	Term int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B)。
	//candidate term
	Term int
	//candidate requesting vote
	CandidateId int
	//index of candidate's last log entry
	LastLogIndex int
	//term of candidate's last log entry
	LastLogTerm int
}

//
// example RequestVote RPC handler.

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.status == "leader"
	rf.mu.Unlock()

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//ok
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Server = rf.me
	if rf.currentTerm == args.Term && rf.votedFor == args.CandidateId {
		reply.VoteGranted, reply.Term = true, rf.currentTerm
		return
	}
	if rf.currentTerm > args.Term ||
		(rf.currentTerm == args.Term && rf.votedFor != -1) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		// fmt.Printf("voter find a new term from RequestVote\n")
		rf.old(args.Term)
	}
	rf.leaderId = -1
	reply.Term = args.Term
	if rf.lastLogTerm > args.LastLogTerm ||
		(rf.lastLogTerm == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex) {
		reply.VoteGranted = false
		return
	}
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.resetTimeclock(randTime(ElectionTime))
}

//
// example code to send a RequestVote RPC to a server.
// server is the in()dex of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be aplastLogIndex,
//	lastLogTerm : rf.lastLogTerm.pended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	term, isLeader = rf.GetState()
	if isLeader {
		rf.mu.Lock()
		log := &logEntry{
			Term:    rf.currentTerm,
			Index:   rf.lastLogIndex+1,
			Command: command,
		}
		// args := &AppendEntriesArgs{
		// 	Term:         rf.currentTerm,
		// 	LeaderId:     rf.me,
		// 	PrevLogIndex: rf.lastLogIndex,
		// 	PrevLogTerm:  rf.lastLogTerm,
		// 	Entries:      log,
		// 	LeaderCommit: rf.commitIndex,
		// }
		rf.logs = append(rf.logs, *log)
		rf.lastLogIndex++
		rf.lastLogTerm = rf.currentTerm
		rf.mu.Unlock()
		go rf.replicate()
	}
	return index, term, isLeader
}

func randTime(duration time.Duration) time.Duration {
	timeLimit := time.Duration(rand.Int63())%duration + duration
	return timeLimit
}

//leader and follower contacts every one interval
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	reply.Term = args.Term
	reply.Success = true
	rf.leaderId = args.LeaderId
	rf.resetTimeclock(randTime(ElectionTime))
	return
}

//
//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.kill <- 1
	// Your code here, if desired.
}
func (rf *Raft) resetTimeclock(timeLimit time.Duration) {
	rf.Timeout.Stop()
	rf.Timeout.Reset(timeLimit)
}
// leader init its nextIndex 
func (rf *Raft) initIndex(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	indexNum = len(rf.nextIndex)
	for i = 0;i<index;i++{
		rf.nextIndex[i] = rf.lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
}
func (rf *Raft) newelection() {
	rf.mu.Lock()
	// fmt.Printf("%d at %d term start election \n", rf.me, rf.currentTerm)
	if rf.status == "leader" {
		rf.mu.Unlock()
		return
	}
	rf.leaderId = -1
	rf.status = "candidate"
	rf.votedFor = rf.me
	rf.currentTerm++
	term, lastLogIndex, me := rf.currentTerm, rf.lastLogIndex, rf.me
	lastLogTerm := rf.lastLogTerm
	args := RequestVoteArgs{Term: term, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
	rf.resetTimeclock(randTime(ElectionTime))
	timer := time.After(randTime(ElectionTime))
	rf.mu.Unlock()
	replyCh := make(chan RequestVoteReply, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i != me {
			go rf.requestforVote(i, args, replyCh)
		}
	}
	voteCount, threshold := 0, len(rf.peers)/2
	for voteCount < threshold {
		select {
		case <-timer:
			return
		case reply := <-replyCh:
			if !reply.Rpcok {
				rf.requestforVote(reply.Server, args, replyCh)
			} else {
				if reply.VoteGranted {
					voteCount++
					// fmt.Printf("%d term %d vote for %d \n", rf.currentTerm, rf.me, args.CandidateId)
				} else {
					rf.mu.Lock()
					if rf.currentTerm < reply.Term {
						// fmt.Printf("during election the candidate become older \n")
						rf.old(reply.Term)
					}
					rf.mu.Unlock()
					return
				}
			}
		}
	}
	rf.mu.Lock()
	if rf.status == "candidate" {
		rf.status = "leader"
		rf.initIndex()
		go rf.tick()
		//go rf.notifyNewLeader()
	}
	rf.mu.Unlock()
}

// when rf find a newer server ,it should be a follower
func (rf *Raft) old(term int) {

	// fmt.Printf("%d from %d to %d \n", rf.me, rf.currentTerm, term)
	rf.currentTerm = term
	rf.status = "follower"
	rf.votedFor, rf.voteNum = -1, -1
	// rf.persist()
	rf.resetTimeclock(randTime(ElectionTime))
}
func (rf *Raft) tick() {
	timer := time.NewTimer((time.Duration(0 * time.Millisecond)))
	for {
		select {
		case <-timer.C:
			if _, isLeader := rf.GetState(); !isLeader {
				return
			}
			go rf.replicate()
			timer.Reset(ApplyMsgInterval)
		}
	}
}
func (rf *Raft) replicate() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	sendList := make(chan int, len(rf.peers))
	go func() {
		for follower := range rf.peers {
			if follower != rf.me {
				rf.sendLogEntry(follower, sendList)
			}
		}
	}()
	sendNum := 0
	for sendNum < len(rf.peers)/2+1 {
		select {
		case Follower := <-sendList:
			{
				//<0 means ok else it means has some net problem
				if Follower < 0 {
					sendNum++
					//fmt.Printf("%d and %d has contact \n", rf.me, -Follower+1)
				} else {
					rf.sendLogEntry(Follower-1, sendList)
					//fmt.Printf("%d and %d has not contact \n", rf.me, Follower-1)
				}
			}
		}
	}
	//fmt.Printf("replicate ok \n")
	rf.resetTimeclock(randTime(ElectionTime))
}
func (rf *Raft) sendLogEntry(follower int, sendList chan int) {
	rf.mu.Lock()
	if rf.status != "leader" {
		rf.mu.Unlock()
		return
	}
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex : rf.lastLogIndex
		PrevLogTerm : rf.lastLogTerm
		
	}
	reply := &AppendEntriesReply{
		Success: false,
		Term:    -1,
	}
	rf.mu.Unlock()
	if rf.peers[follower].Call("Raft.AppendEntries", args, reply) {
		rf.mu.Lock()
		if !reply.Success {
			if reply.Term > rf.currentTerm {
				fmt.Printf("find a new term from heartebeats \n")
				rf.old(reply.Term)
			}
		}
		rf.mu.Unlock()
		if reply.Success {
			//fmt.Printf("sendList <- -1 * (follower + 1)")
			sendList <- -1 * (follower + 1)
		}

	} else {
		sendList <- (follower + 1)
	}
	return
}

func (rf *Raft) requestforVote(server int, args RequestVoteArgs, replyCh chan RequestVoteReply) {
	var reply RequestVoteReply
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	reply.Rpcok = ok
	replyCh <- reply
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
//this is for follower and checking what happens in applych
// func (rf *Raft) checkSend() {
// 	for {
// 		time.Sleep(time.Duration(500 * time.Millisecond))
// 		// fmt.Printf("%d at %d term as %s \n", rf.me, rf.currentTerm, rf.status)
// 	}
// }
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.leaderId = -1
	
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]logEntry, 0) //

	rf.commitIndex = 0
	rf.lastApplied = 0
	
	rf.nextIndex = make([]int,len(rf.peers))
	rf.matchIndex = make([]int,len(rf.peers))

	rf.voteNum = 0
	rf.status = "follower"
	rf.lastLogIndex = 0
	rf.lastLogTerm = 0
	
	rf.applyCh = applyCh
	//rf.notifyapplyCh = make(chan struct{}, 100)
	rf.Timeout = time.NewTimer(randTime(ElectionTime))
	rf.kill = make(chan int)
	// fmt.Printf("%d term %d start up\n", rf.currentTerm, rf.me)
	// go rf.checkSend()
	go func() {
		for {
			select {
			case <-rf.Timeout.C:
				rf.newelection()
			case <-rf.kill:
				return
			}
		}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
