package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"github.com/TienMinh25/mit-labs-6-824-2025/labrpc"
	"github.com/TienMinh25/mit-labs-6-824-2025/raft/raftapi"
	tester "github.com/TienMinh25/mit-labs-6-824-2025/tester1"
)

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
	state           RaftState
	electionTimeout time.Timer
	heartbeatChan   chan bool

	// persistent data
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// volatile data for each raft server
	commitIndex int
	lastApplied int

	// volatile data for leader
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// get current term and check is leader or not
	term = rf.currentTerm
	isleader = rf.state == LEADER

	return term, isleader
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
	CandidateID  int
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

	// parallel get votes from other servers
	mux := sync.Mutex{}
	votes := 1
	rf.votedFor = rf.me

	for idx := range rf.peers {
		if idx != rf.me {
			go func(serverID int) {

				rf.sendRequestVote(serverID, args)
			}(idx)
		}
	}

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

// function ticker() is used to check if a leader election should be started
// received heartbeat message and when don't received -> it timeout, start new leader election
// deprecated: not used in my implementation
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		select {
		case <-rf.electionTimeout.C:
			// election timeout -> should be changed to candidate and start new leader election
			rf.startLeaderElection()
		case <-rf.heartbeatChan:
			// when received heartbeat or received append log entries will be reset timeout
			rf.electionTimeout.Reset(rf.getElectionTimeout() * time.Millisecond)
		}
	}
}

func (rf *Raft) startLeaderElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = CANDIDATE
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.persist()

	// reset election timeout for candidate
	rf.electionTimeout.Reset(rf.getElectionTimeout() * time.Millisecond)

	// broadcast request votes, 
	// if success, convert to leader state and start heartbeat also received command from client
	if ok := rf.broadcastRequestVotes(); ok {
		// stop timeout for leader
		rf.electionTimeout.Stop()
	}
}

// broadcast RequestVote RPCs to all peers in parallel.
// lock must be held before calling this.
func (rf *Raft) broadcastRequestVotes() bool {
	votes := 1
	mux := sync.Mutex{}
	wg := sync.WaitGroup{}

	requestVoteMsg := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		wg.Add(1)
		// send request vote in parallel
		go func(serverID int) {
			defer wg.Done()
			resp := RequestVoteReply{}

			ok := rf.sendRequestVote(serverID, requestVoteMsg, &resp)

			if !ok {
				return
			}

			// candidate convert to follower if current term is smaller than other term
			if resp.Term > rf.currentTerm {
				rf.state = FOLLOWER
				rf.currentTerm = resp.Term
				rf.votedFor = -1
				rf.persist()
				return
			}

			// handle race condition, maybe when send request vote, time election is timeout,
			// or some heartbeat from other leader come, so the state of current raft is changed.
			if rf.state != CANDIDATE || rf.currentTerm != requestVoteMsg.Term {
				return
			}

			if resp.VoteGranted {
				mux.Lock()
				votes++
				mux.Unlock()
			}
		}(idx)
	}

	wg.Wait()

	if rf.state == CANDIDATE && votes > len(rf.peers)/2 {
		return true
	}

	return false
}

// getLastLogIndex return last log index
// lock must be held before calling this.
func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1
}

// getLastLogIndex return last log term
// lock must be held before calling this.
func (rf *Raft) getLastLogTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

// function added
// getElectionTimeout get the randomized election timeout
func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(50 + rand.Intn(300))
}

// runServer method is used to handle with all state of raft instance:
// - LEADER: send heartbeat, received command from client, log replication
// - CANDIDATE: send request vote, handle case when become leader or follower
// - FOLLOWER: passive handle request vote, log replication
func (rf *Raft) runServer() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case LEADER:
		case CANDIDATE:
			select {
			case <-rf.electionTimeout.C:
				// retry leader election again =))
				rf.startLeaderElection()
			case <-rf.heartbeatChan:
				// down state to follower and also reset election timeout
				rf.downToFollower()				
				rf.electionTimeout.Reset(rf.getElectionTimeout() * time.Millisecond)
			}
		case FOLLOWER:
			select {
			case <-rf.electionTimeout.C:
				// start leader election
				rf.startLeaderElection()
			case <-rf.heartbeatChan:
				// reset election timeout
				rf.electionTimeout.Reset(rf.getElectionTimeout() * time.Millisecond)
			}
		}
	}
}

// todo: future will be handled something more
func (rf *Raft) downToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = FOLLOWER
	rf.votedFor = -1
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

	// tienminh: init data when start
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.logs = append(rf.logs, LogEntry{Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.electionTimeout = *time.NewTimer(rf.getElectionTimeout() * time.Millisecond)

	// initialize from state persisted before a crash
	// tienminh: it will persist from disk when it restart from crash, or not if it is run the first time
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// go rf.ticker()

	go rf.runServer()

	return rf
}
