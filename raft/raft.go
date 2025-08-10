package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"github.com/TienMinh25/mit-labs-6-824-2025/labrpc"
	"github.com/TienMinh25/mit-labs-6-824-2025/raft/raftapi"
	tester "github.com/TienMinh25/mit-labs-6-824-2025/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state        RaftState
	currentTerm  int
	votedFor     int
	logs         []LogEntry
	commitIndex  int
	lastApplied  int
	nextIndex    []int
	matchIndex   []int
	lastLogIndex int

	lastAccessed time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// idempotent
	if args.Term == rf.currentTerm && rf.votedFor == args.CandidateId {
		reply.Term, reply.VoteGranted = rf.currentTerm, true
		return
	}

	// reply false if term < currentTerm or if currentTerm = term and grated vote before
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	// neu da bat dau 1 cuoc bau cu moi -> set lai votedFor la -1
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.state = FOLLOWER
	}

	// check up to date log (log election restriction)
	lastLogTerm := rf.logs[rf.lastLogIndex].Term

	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && args.LastLogIndex < rf.lastLogIndex) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	rf.state = FOLLOWER
	rf.lastAccessed = time.Now()
	reply.Term, reply.VoteGranted = args.Term, true
	rf.votedFor = args.CandidateId
	rf.persist()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// MIT provide the code
// for me: it is [deprecated] and not used
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	// tat ca server khi moi bat dau run thi deu se la state follower
	rf.state = FOLLOWER
	rf.currentTerm = 1
	rf.votedFor = -1
	rf.logs = []LogEntry{
		{
			Command: nil,
			Term:    0,
		},
	}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.lastLogIndex = len(rf.logs) - 1
	rf.lastAccessed = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.runServer()

	return rf
}

// tien minh code
type RaftState int

const (
	FOLLOWER RaftState = iota
	CANDIDATE
	LEADER
)

type LogEntry struct {
	Command interface{}
	Term    int
}

func (rf *Raft) runServer() {
	for !rf.killed() {
		rf.mu.Lock()
		status := rf.state
		rf.mu.Unlock()

		switch status {
		case FOLLOWER:
			rf.manageFollower()
		case CANDIDATE:
			rf.manageCandidate()
		case LEADER:
			rf.manageLeader()
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) manageFollower() {
	timeout := getRandomizedTime() * time.Millisecond

	// always have timeout
	time.Sleep(timeout)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if time.Since(rf.lastAccessed).Milliseconds() >= timeout.Milliseconds() {
		fmt.Printf("Follower %v transition to candidate\n", rf.me)
		rf.state = CANDIDATE
		rf.votedFor = -1
		rf.currentTerm++
		rf.persist()
	}
}

func (rf *Raft) manageCandidate() {
	// thiet lap timeout moi
	timeout := getRandomizedTime() * time.Millisecond
	start := time.Now()

	rf.mu.Lock()
	fmt.Printf("Candidate %v start election \n", rf.me)

	countingVote := 0
	majorityAccepts := len(rf.peers)/2 + 1
	finished := 0
	peers := len(rf.peers)

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex,
		LastLogTerm:  rf.logs[rf.lastLogIndex].Term,
	}
	me := rf.me
	term := rf.currentTerm
	rf.votedFor = rf.me

	rf.mu.Unlock()

	// broadcast request votes
	for idx := range peers {
		if me == idx {
			countingVote++
			finished++
			continue
		}

		go func(serverID int) {
			var reply RequestVoteReply

			ok := rf.sendRequestVote(idx, &args, &reply)
			finished++

			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.VoteGranted {
				countingVote++
			} else if reply.Term > term {
				// election restriction show here when
				// candidate receive value and handle
				rf.state = FOLLOWER
				rf.currentTerm = reply.Term
				rf.persist()
			}
		}(idx)
	}

	for {
		rf.mu.Lock()
		if finished == peers || countingVote >= majorityAccepts || time.Since(start).Milliseconds() > timeout.Microseconds() {
			break
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}

	// if timeout, return
	if time.Since(start).Milliseconds() > timeout.Microseconds() {
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
		rf.mu.Unlock()
		return
	}

	// if not timeout, check condition pass
	if rf.state == CANDIDATE && countingVote >= majorityAccepts {
		rf.state = LEADER
		// khoi tao nextIndex va matchIndex
		for peer := range peers {
			rf.nextIndex[peer] = rf.lastLogIndex + 1
		}
	} else {
		rf.state = FOLLOWER
	}

	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) manageLeader() {
	// when become leader -> send heartbeat
	rf.mu.Lock()

	rf.nextIndex[rf.me] = rf.lastLogIndex + 1
	rf.matchIndex[rf.me] = rf.lastLogIndex

	rf.mu.Unlock()

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}

		go func(peerId int) {
			args := AppendEntriesArgs{}
			reply := AppendEntriesReply{}

		LOOP:
			for {
				rf.mu.Lock()
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[peerId] - 1
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
				args.Entries = rf.logs[rf.nextIndex[peerId]:]

				term := rf.currentTerm

				rf.mu.Unlock()

				ok := rf.sendAppendEntries(peerId, &args, &reply)

				// indefinetly retry
				if !ok {
					break LOOP
				}

				if reply.Success {
					// update match index
					// when append entries occur, the log of leader may be added more,
					// so needed to handle matchIndex and nextIndex by using len(args.Entries)
					rf.mu.Lock()
					rf.matchIndex[peerId] = len(args.Entries) + rf.nextIndex[peerId] - 1
					rf.nextIndex[peerId] = len(args.Entries) + rf.nextIndex[peerId]
					rf.mu.Unlock()
					break LOOP
				}

				// occur conflict between leader and follower
				if reply.Term > term {
					rf.mu.Lock()
					rf.state = FOLLOWER
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.persist()
					rf.mu.Unlock()
					break LOOP
				}

				//
				if rf.logs[reply.ConflictIndex].Term != reply.ConflictTerm {

				}
			}

		}(idx)
	}
}

func getRandomizedTime() time.Duration {
	return time.Duration(50 + rand.Intn(400))
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false

	// for rule 1
	if args.Term < rf.currentTerm {
		return
	}

	// for rule 2
	if len(rf.logs) < args.PrevLogIndex+1 {
		return
	}

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term

		for i, v := range rf.logs {
			if v.Term != reply.ConflictTerm {
				break
			}

			reply.ConflictIndex = i
		}

		return
	}

	// for rule 3
	// for _, entry := range args.Entries {

	// }

	rf.lastAccessed = time.Now()
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(peerId int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peerId].Call("Raft.AppendEntries", args, reply)
	return ok
}
