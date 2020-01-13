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
	"bytes"
	"fmt"
	"labgob"
	"log"
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
const ApplyMsgInterval = time.Duration(200 * time.Millisecond)
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

	commitIndex      int
	lastApplied      int
	lastIncludeIndex int
	nextIndex        []int // the index of next log should send to server
	matchIndex       []int // the index of commited log of each server

	voteNum      int
	status       string
	lastLogIndex int
	notifiedCh   chan struct{}
	applyCh      chan ApplyMsg
	//notifyapplyCh chan struct{}
	kill    chan int
	Timeout *time.Timer
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
	Len          int
}
type AppendEntriesReply struct {
	Success        bool
	Term           int
	InconsistIndex int
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
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.status == "leader"

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastLogIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []logEntry
	var commitIndex int
	var lastIncludeIndex int
	var lastLogIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastLogIndex) != nil {
		log.Fatal("error int read persist")
	} else {
		rf.currentTerm, rf.votedFor, rf.logs, rf.commitIndex, rf.lastIncludeIndex, rf.lastLogIndex =
			currentTerm, votedFor, logs, commitIndex, lastIncludeIndex, lastLogIndex
	}

}

//ok
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("%d at %d term get the requestvote from %d \n", rf.me, rf.currentTerm, args.CandidateId)
	reply.Server = rf.me

	if rf.currentTerm == args.Term && rf.votedFor == args.CandidateId {
		rf.resetTimeclock(randTime(ElectionTime))
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
	rflastLogTerm := rf.getEntry(rf.getReallyIndex(rf.lastLogIndex - 1)).Term
	if rflastLogTerm > args.LastLogTerm ||
		(rflastLogTerm == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex) {
		reply.VoteGranted = false
		return
	}
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	// fmt.Printf("%d at %d term vote for %d and reset time\n", rf.me, rf.currentTerm, rf.votedFor)
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
	isLeader := false
	// Your code here (2B).
	term, isLeader = rf.GetState()
	if isLeader {
		rf.mu.Lock()
		log := logEntry{
			rf.lastLogIndex,
			rf.currentTerm,
			command,
		}
		fmt.Printf("start in raft.go has been start ............................................................\n")

		index = rf.lastLogIndex
		rf.logs = append(rf.logs, log)
		rf.matchIndex[rf.me] = rf.lastLogIndex
		rf.nextIndex[rf.me] = rf.lastLogIndex + 1
		rf.lastLogIndex++
		rf.mu.Unlock()
		go rf.persist()
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
	if args == nil {
		fmt.Printf("noooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo\n")
		return
	}

	//this is an old leader,ignore it
	if rf.currentTerm > args.Term {
		reply.Term, reply.Success, reply.InconsistIndex = rf.currentTerm, false, -1
		return
	}
	rf.resetTimeclock(randTime(ElectionTime))

	if args.Term >= rf.currentTerm {
		rf.leaderId = args.LeaderId
		rf.votedFor = -1
		rf.voteNum = 0
		rf.status = "follower"
		rf.currentTerm = args.Term

	}

	fmt.Printf("%d reset time in appendEntries and commitedIndex is %d leader is %d\n", rf.me, args.LeaderCommit, args.LeaderId)

	// this means the follower has more informations
	if args.PrevLogIndex < rf.lastIncludeIndex {
		reply.Success, reply.InconsistIndex, reply.Term = false, rf.lastIncludeIndex+1, rf.currentTerm
		return
	}
	// because of snapshot,the true offset we need caculate
	// this meanss the leader need modified the nextIndex[follower] because mismatch
	if rf.lastLogIndex <= args.PrevLogIndex || rf.getEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
		var conflictIndex int

		conflictIndex = min(rf.lastLogIndex-1, args.PrevLogIndex)
		conflictTerm := rf.getEntry(conflictIndex).Term
		var down int
		down = max(rf.lastIncludeIndex, rf.commitIndex)
		for ; conflictIndex > down && rf.getEntry(conflictIndex-1).Term == conflictTerm; conflictIndex-- {
		}
		reply.Success, reply.InconsistIndex, reply.Term = false, conflictIndex, rf.currentTerm
		return
	}
	// this means followe and leader finally get the matched index
	reply.Success, reply.InconsistIndex = true, -1
	// after match the index and we add some log which leader has but follower doesnot have
	i := 0
	for ; i < args.Len; i++ {
		if args.PrevLogIndex+1+i >= rf.lastLogIndex {
			break
		}
		if rf.getEntry(args.PrevLogIndex+i+1).Term != args.Entries[i].Term {
			rf.lastLogIndex = args.PrevLogIndex + i + 1
			truncat := rf.getReallyIndex(rf.lastLogIndex)
			rf.logs = append(rf.logs[:truncat])
			break
		}
	}

	for ; i < args.Len; i++ {
		rf.logs = append(rf.logs, args.Entries[i])
		rf.lastLogIndex += 1
	}
	oldCommitedIndex := rf.commitIndex
	rf.commitIndex = max(rf.commitIndex, min(args.LeaderCommit, args.PrevLogIndex+args.Len))
	if rf.commitIndex > oldCommitedIndex {
		rf.notifiedCh <- struct{}{}
	}
	reply.Term = args.Term
	reply.Success = true
	reply.InconsistIndex = -1
	return
}
func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
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

func (rf *Raft) getEntry(i int) logEntry {
	off := rf.getReallyIndex(i)
	return rf.logs[off]
}

func (rf *Raft) getReallyIndex(i int) int {
	return i - rf.lastIncludeIndex
}
func (rf *Raft) resetTimeclock(timeLimit time.Duration) {
	rf.Timeout.Stop()
	rf.Timeout.Reset(timeLimit)
}

// leader init its nextIndex
func (rf *Raft) initIndex() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	//fmt.Printf("at %d term %d initindex as %s\n", rf.currentTerm, rf.me, rf.status)
	indexNum := len(rf.peers)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < indexNum; i++ {
		rf.nextIndex[i] = rf.lastLogIndex
		rf.matchIndex[i] = 0
	}
}
func (rf *Raft) newelection() {
	rf.mu.Lock()
	fmt.Printf("%d at %d term start election \n", rf.me, rf.currentTerm)
	if rf.status == "leader" {
		rf.mu.Unlock()
		return
	}
	rf.leaderId = -1
	rf.status = "candidate"
	rf.votedFor = rf.me
	rf.currentTerm++
	term, lastLogIndex, me := rf.currentTerm, rf.lastLogIndex, rf.me
	lastLogTerm := rf.getEntry(lastLogIndex - 1).Term
	go rf.persist()
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
					// fmt.Printf("at %d term %d vote for %d \n", rf.currentTerm, reply.Server, args.CandidateId)
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
		fmt.Printf("%d at %d term  become %s \n", rf.me, rf.currentTerm, rf.status)
		rf.initIndex()
		rf.leaderId = rf.me
		rf.votedFor = -1
		rf.voteNum = -1
		rf.resetTimeclock(randTime(ElectionTime))
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
	go rf.persist()
	rf.resetTimeclock(randTime(ElectionTime))
}
func (rf *Raft) tick() {
	// fmt.Printf("%d at %d term tick start \n", rf.me, rf.currentTerm)
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

	// fmt.Printf("%d at %d term replicate start \n", rf.me, rf.currentTerm)
	go func() {
		rf.mu.Lock()

		numofPeer := len(rf.peers)
		rf.mu.Unlock()
		for follower := 0; follower < numofPeer; follower++ {
			if follower != rf.me {
				go rf.sendLogEntry(follower)
			}
			// select {
			// case kill := <-rf.kill:
			// 	rf.kill <- kill
			// 	return
			// }
		}
	}()
	// sendNum := 0
	rf.mu.Lock()
	rf.resetTimeclock(randTime(ElectionTime))
	rf.mu.Unlock()
}
func (rf *Raft) sendLogEntry(follower int) {
	rf.mu.Lock()
	// fmt.Printf("%d at %d term sendLogEntry start \n", rf.me, rf.currentTerm)
	if rf.status != "leader" {
		rf.mu.Unlock()
		return
	}
	PreLogIndex := rf.nextIndex[follower]
	entries := rf.getbetweenEntry(PreLogIndex, rf.lastLogIndex)
	PreLogIndex--
	args := AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
		PreLogIndex,
		rf.getEntry(PreLogIndex).Term,
		entries,
		rf.commitIndex,
		len(entries),
	}
	if args.Len > 0 {
		fmt.Printf("%d at %d term sendLogEntry start \n", rf.me, rf.currentTerm)
	}
	rf.mu.Unlock()
	var reply AppendEntriesReply

	if rf.peers[follower].Call("Raft.AppendEntries", &args, &reply) {
		rf.mu.Lock()
		if !reply.Success {
			if reply.Term > rf.currentTerm {
				fmt.Printf("find a new term from heartebeats \n")
				rf.old(reply.Term)
			} else {
				rf.nextIndex[follower] = max(1, min(reply.InconsistIndex, rf.lastLogIndex))
				fmt.Printf("%d as follower and %d as leader retry find log index \n", follower, rf.me)
				fmt.Printf("rf.nextIndex[follower] %d reply.InconsistIndex %d \n", rf.nextIndex[follower], reply.InconsistIndex)

			}
		} else {
			PreLogIndex := args.PrevLogIndex
			if PreLogIndex+args.Len >= rf.nextIndex[follower] {
				rf.nextIndex[follower] = PreLogIndex + args.Len + 1
				rf.matchIndex[follower] = rf.nextIndex[follower] - 1
			}
			oldCommitedIndex := rf.commitIndex
			commitedIndex := PreLogIndex + args.Len
			fmt.Printf("prelogindex %d ,commited %d \n ", PreLogIndex, commitedIndex)
			if rf.checkCommited(commitedIndex) {
				rf.commitIndex = commitedIndex
				go rf.persist()
			}
			if rf.commitIndex > oldCommitedIndex {
				rf.notifiedCh <- struct{}{}
			}
			// check if commited
		}
		rf.mu.Unlock()
	} else {
		fmt.Printf("call fail ?\n")
	}
	return
}
func (rf *Raft) checkCommited(index int) bool {
	count := 0
	if index > rf.lastLogIndex || rf.commitIndex > index {
		return false
	}
	numofPeer := len(rf.peers)
	for peer := 0; peer < numofPeer; peer++ {
		if rf.matchIndex[peer] >= index {
			count++
		}
	}
	return count > len(rf.peers)/2
}
func (rf *Raft) requestforVote(server int, args RequestVoteArgs, replyCh chan RequestVoteReply) {
	var reply RequestVoteReply
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	reply.Rpcok = ok
	replyCh <- reply
}

// func (rf *Raft) getRangeEntries(index int) []logEntry {
// 	if index >= rf.lastLogIndex {
// 		return nil
// 	} else {
// 		return append(rf.logs[index:]...)
// 	}
// }

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

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludeIndex = 0
	rf.lastLogIndex = 1

	rf.voteNum = 0
	rf.status = "follower"

	rf.logs = []logEntry{{0, 0, nil}} //
	rf.applyCh = applyCh
	rf.notifiedCh = make(chan struct{}, 100)
	//rf.notifyapplyCh = make(chan struct{}, 100)
	rf.Timeout = time.NewTimer(randTime(ElectionTime))
	rf.kill = make(chan int)
	fmt.Printf("%d term %d start up\n", rf.currentTerm, rf.me)
	go rf.apply()
	go func() {
		for {
			select {
			case <-rf.Timeout.C:
				// fmt.Printf("%d at %d term time out \n", rf.me, rf.currentTerm)
				rf.newelection()
			case kill := <-rf.kill:
				rf.kill <- kill
				return
			}
		}
	}()
	rf.readPersist(persister.ReadRaftState())

	// initialize from state persisted before a crash
	return rf
}
func (rf *Raft) apply() {
	for {
		select {
		case <-rf.notifiedCh:
			rf.mu.Lock()
			var commandValid bool
			var entries []logEntry
			if rf.lastApplied < rf.lastIncludeIndex {
				commandValid = false
				rf.lastApplied = rf.lastIncludeIndex
				entries = []logEntry{{Index: rf.lastIncludeIndex, Term: rf.logs[0].Term, Command: "InstallSnapshot"}}
			} else if rf.lastApplied < rf.lastLogIndex && rf.lastApplied < rf.commitIndex {
				commandValid = true
				entries = rf.getbetweenEntry(rf.lastApplied+1, rf.commitIndex+1)
				rf.lastApplied = rf.commitIndex
			}
			go rf.persist()
			rf.mu.Unlock()
			for _, entry := range entries {
				fmt.Printf(".................here in apply in raft.................................\n")
				rf.applyCh <- ApplyMsg{CommandValid: commandValid, CommandIndex: entry.Index, CommandTerm: entry.Term, Command: entry.Command}
			}

		case <-rf.kill:
			return

		}
	}
}
func (rf *Raft) getbetweenEntry(start int, end int) []logEntry {
	startoff := rf.getReallyIndex(start)
	endoff := rf.getReallyIndex(end)
	return append([]logEntry{}, rf.logs[startoff:endoff]...)
}
