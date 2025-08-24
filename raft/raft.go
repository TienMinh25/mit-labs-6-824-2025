package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TienMinh25/mit-labs-6-824-2025/labgob"
	"github.com/TienMinh25/mit-labs-6-824-2025/labrpc"
	"github.com/TienMinh25/mit-labs-6-824-2025/raft/raftapi"
	tester "github.com/TienMinh25/mit-labs-6-824-2025/tester1"
)

type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
}

type RaftState string

const (
	FOLLOWER  RaftState = "follower"
	CANDIDATE RaftState = "candidate"
	LEADER    RaftState = "leader"
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

	// non-volatile state
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// volatile state
	commitIndex int
	lastApplied int

	// volatile state of leader
	nextIndex  []int
	matchIndex []int

	state    RaftState
	lastPing time.Time

	countVote     int
	currentLeader int

	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond

	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A).

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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		DPrintf("occur error when read persist data\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.snapshot = snapshot
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastIncludedIndex >= index {
		return
	}

	DPrintf("[server: %v, state: %s] snapshot at index %v\n", rf.me, rf.state, index)
	// trim log if needed
	rf.snapshot = snapshot

	// luon co dummy entry de func getPhysicalIndex hoat dong dung
	/*
			rf.log = [
		    {Term: 0, Command: nil, Index: 0},    // dummy entry
		    {Term: 1, Command: "A", Index: 1},
		    {Term: 1, Command: "B", Index: 2},
		    {Term: 2, Command: "C", Index: 3},
		    {Term: 2, Command: "D", Index: 4},
		    {Term: 2, Command: "E", Index: 5},
			]
			rf.lastIncludedIndex = 0

			Case 1: Lan dau tien compact log: Snapshot(3, snapshot_data)
			len(rf.logs) - rf.getPhysicalIndex(3) = 6 - (3 - 0) = 6 - 3 = 3
			newLogs := make([]LogEntry, 3)
			rf.logs[3 - 0:] = rf.logs[3:] -> [
				{Term: 2, Command: "C", Index: 3},
		    {Term: 2, Command: "D", Index: 4},
		    {Term: 2, Command: "E", Index: 5},
			]

			copy(newLogs, rf.logs[3:]) -> newLogs -> [
				{Term: 2, Command: "C", Index: 3},
		    {Term: 2, Command: "D", Index: 4},
		    {Term: 2, Command: "E", Index: 5},
			]

			luc nay {Term: 2, Command: "C", Index: 3} chinh la dummy entry vi da snapshot roi
			=> rf.lastIncludedIndex = 3
			=> rf.lastIncludedTerm = rf.logs[0].Term = 2

			Case 2: Snapshot(5, snapshot_data) (tiep tuc tu case truoc do)
			len(rf.logs) - rf.getPhysicalIndex(5) = 3 - (5 - 3) = 3 - 2 = 1
			newLogs := make([]LogEntry, 1)
			rf.logs[5 - 3:] = rf.logs[2:] -> [{Term: 2, Command: "E", Index: 5}]

			newLogs := [{Term: 2, Command: "E", Index: 5}]
			Voi dummy entry la {Term: 2, Command: "E", Index: 5}

			=> cong thuc: lam the nao de tinh duoc do dai cua array can cap khi log compact
			-> vi du: [
				{Term: 2, Command: "C", Index: 3},
		    {Term: 2, Command: "D", Index: 4},
		    {Term: 2, Command: "E", Index: 5},
			]

			Khi compact -> can co dummy entry o day la {Term: 2, Command: "E", Index: 5} => do dai sau khi compact se la 1
			=> do dai sau khi compact = len(rf.logs) - rf.getPhysicalIndex(index-compact) = 3 - 2 = 1
			=> Nen copy vao logs moi tu dau trong rf.logs ban dau (vi luon phai co dummy entry la log da compact cuoi)
			=> can phai included index cua dummy entry do vao log moi sau khi compact
			=> newLogs := rf.logs[rf.getPhysicalIndex(index-compact):]
	*/

	lenNewLogs := len(rf.logs) - rf.getPhysicalIndex(index)
	includedDummyEntry := rf.getPhysicalIndex(index)

	newLogs := make([]LogEntry, lenNewLogs)
	copy(newLogs, rf.logs[includedDummyEntry:])

	rf.logs = newLogs
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.logs[0].Term
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	LastLogIndex int
	LastLogTerm  int
	CandidateId  int
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

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// idempotence
	if args.Term == rf.currentTerm && rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		return
	}

	// if currentTerm > term of args -> reject request vote
	if args.Term < rf.currentTerm {
		return
	}

	defer rf.persist()
	if args.Term > rf.currentTerm {
		rf.revertToFollower(args.Term)
	}

	if args.LastLogTerm < rf.getLastLogTerm() ||
		(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastLogIndex()) ||
		rf.votedFor != -1 {
		return
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.updateLastPing()
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

	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.revertToFollower(reply.Term)
		rf.persist()
		return true
	}

	// case previous server have candidate state va da bi election timeout
	// thi phai xem term hien tai va term trong args co con giong nhau hay ko
	if rf.state != CANDIDATE || args.Term != rf.currentTerm {
		return true
	}

	if reply.VoteGranted {
		rf.countVote++

		if rf.countVote > len(rf.peers)/2 {
			// become leader
			rf.state = LEADER
			rf.countVote = 0

			for idx := range rf.nextIndex {
				rf.nextIndex[idx] = rf.getLastLogIndex() + 1
			}

			DPrintf("[server: %v, state: %s, term: %v] become leader, broadcast update\n", rf.me, rf.state, rf.currentTerm)
			// broadcase update immediately to prevent one candidate send request vote further more
			rf.BroadcastUpdate()
		}
	}

	return ok
}

func (rf *Raft) revertToFollower(term int) {
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.updateLastPing()
	rf.votedFor = -1
	rf.countVote = 0
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return -1, rf.currentTerm, false
	}

	DPrintf("[server: %v, state: %s, term: %v] accept command %v from client\n", rf.me, rf.state, rf.currentTerm, command)

	rf.logs = append(rf.logs, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
		Index:   rf.getLastLogIndex() + 1,
	})
	rf.nextIndex[rf.me] = rf.getLastLogIndex() + 1
	rf.matchIndex[rf.me] = rf.getLastLogIndex()
	rf.persist()

	// Your code here (3B).

	return rf.getLastLogIndex(), rf.currentTerm, true
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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()

		// Your code here (3A)
		// Check if a leader election should be started.
		if rf.state == LEADER {
			rf.BroadcastUpdate()
		} else {
			timeDiff := time.Now().Sub(rf.lastPing).Milliseconds() + int64(rand.Intn(10)*20)

			// candidate and follower is the same when need to check election timeout to start new election
			if timeDiff >= 1000 {
				rf.AttemptBecomeLeader()
			}
		}

		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 100 + (rand.Int63()%10)*5
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
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		currentTerm:       0,
		votedFor:          -1,
		logs:              []LogEntry{{Term: 0, Command: nil, Index: 0}},
		commitIndex:       0,
		lastApplied:       0,
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
		state:             FOLLOWER,
		lastPing:          time.Now(),
		countVote:         0,
		currentLeader:     -1,
		applyCh:           applyCh,
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
	}

	rf.applyCond = sync.NewCond(&rf.mu)

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyCommitMsgs()

	return rf
}

func (rf *Raft) getLastLogIndex() int {
	// vi luon co dummy entry nen ko can check case nay nua
	// if len(rf.logs) == 0 {
	// 	return rf.lastIncludedIndex
	// }

	return rf.logs[len(rf.logs)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	// vi luon co dummy entry nen ko can check case nay nua
	// if len(rf.logs) == 0 {
	// 	return rf.lastIncludedTerm
	// }

	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) updateLastPing() {
	rf.lastPing = time.Now()
}

func (rf *Raft) AttemptBecomeLeader() {
	// update state of raft instance
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = CANDIDATE
	rf.countVote = 0
	rf.countVote++
	rf.updateLastPing()
	rf.persist()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	// send request vote to other followers
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}

		var reply RequestVoteReply

		go rf.sendRequestVote(idx, &args, &reply)
	}
}

func (rf *Raft) BroadcastUpdate() {
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}

		DPrintf("[server: %v, state: %s] have last included index: %v and nextIndex: %#v\n", rf.me, rf.state, rf.lastIncludedIndex, rf.nextIndex)
		// send append entry or send install snapshot based condition
		if rf.nextIndex[idx] <= rf.lastIncludedIndex {
			// send snapshot instead
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Offset:            0,
				Data:              rf.snapshot,
				Done:              true,
			}

			DPrintf("[%d] Sending snapshot at [%d] to [%d]", rf.me, rf.lastIncludedIndex, idx)

			reply := InstallSnapshotReply{}

			go rf.sendInstallSnapshotRPC(idx, &args, &reply)

			continue
		}

		prevLogIndex := rf.nextIndex[idx] - 1
		physicalPrevLogIndex := rf.getPhysicalIndex(prevLogIndex)
		prevLogTerm := rf.logs[physicalPrevLogIndex].Term

		logs := rf.logs[physicalPrevLogIndex+1:]

		// DPrintf("[server: %v, state: %s] Log append to server %v: %#v\n", rf.me, rf.state, idx, logs)

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Logs:         logs,
			LeaderCommit: rf.commitIndex,
		}

		var reply AppendEntriesReply

		go rf.sendAppendEntries(idx, &args, &reply)

	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Logs         []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// field to optimize I/O network
	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[server: %v, state: %s, term: %v, logs: %#v] received append entries from server %v\n", rf.me, rf.state, rf.currentTerm, args.Logs, args.LeaderId)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = -1

	// reject any request contains term < rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	defer rf.persist()
	if args.Term > rf.currentTerm {
		rf.revertToFollower(args.Term)
	}

	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.XLen = rf.getLastLogIndex() + 1
		return
	}

	physicalPrevLogIdx := rf.getPhysicalIndex(args.PrevLogIndex)
	DPrintf("[server: %v, state: %s] physicalPrevLogIdx: %v, arg prev term: %v, term in prev: %v\n", rf.me, rf.state, physicalPrevLogIdx, args.PrevLogTerm, rf.logs[physicalPrevLogIdx].Term)
	if args.PrevLogTerm != rf.logs[physicalPrevLogIdx].Term {
		// find the first index store the term
		reply.XTerm = rf.logs[physicalPrevLogIdx].Term

		for idx := 0; idx < len(rf.logs); idx++ {
			if rf.logs[idx].Term == reply.XTerm {
				reply.XIndex = rf.logs[idx].Index
				break
			}
		}

		return
	}

	if len(args.Logs) > 0 {
		// Remove conflicting entries and append new ones
		// Keep logs up to prevLogIndex, then append new logs
		newLogs := make([]LogEntry, len(rf.logs[:physicalPrevLogIdx+1]))
		copy(newLogs, rf.logs[:physicalPrevLogIdx+1])

		newLogs = append(newLogs, args.Logs...)
		rf.logs = newLogs
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		rf.applyCond.Broadcast()
	}

	reply.Success = true
	rf.currentLeader = args.LeaderId
	rf.updateLastPing()
	DPrintf("[server: %v, state: %s, term: %v] end append entries from server %v\n", rf.me, rf.state, rf.currentTerm, args.LeaderId)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.revertToFollower(reply.Term)
		rf.persist()
		return
	}

	if rf.state != LEADER {
		return
	}

	if reply.Success == true {
		// update match index and next index
		rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Logs))
		rf.nextIndex[server] = max(rf.nextIndex[server], rf.matchIndex[server]+1)
		// update commit index

		rf.updateCommitIndex()
		return
	}

	// handle append entry fail
	if reply.XLen != -1 {
		rf.nextIndex[server] = reply.XLen
		return
	}

	found := false
	for idx := 1; idx < len(rf.logs); idx++ {
		if rf.logs[idx].Term == reply.XTerm {
			rf.nextIndex[server] = rf.logs[idx].Index
			found = true
			break
		}
	}

	if !found {
		rf.nextIndex[server] = reply.XIndex
	}
}

func (rf *Raft) applyCommitMsgs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		if rf.commitIndex > rf.lastApplied {
			var msg raftapi.ApplyMsg

			if rf.lastIncludedIndex > rf.lastApplied {
				rf.lastApplied = rf.lastIncludedIndex

				msg = raftapi.ApplyMsg{
					SnapshotValid: true,
					Snapshot:      rf.snapshot,
					SnapshotTerm:  rf.lastIncludedTerm,
					SnapshotIndex: rf.lastIncludedIndex,
				}
			} else {
				rf.lastApplied++
				msg = raftapi.ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[rf.getPhysicalIndex(rf.lastApplied)].Command,
					CommandIndex: rf.lastApplied,
				}
			}

			rf.mu.Unlock()
			rf.applyCh <- msg
			// DPrintf("[server: %v, state: %s] apply commit with index: %v, lastApplied: %v\n", rf.me, rf.state, rf.commitIndex, rf.lastApplied)
			rf.mu.Lock()
		} else {
			// DPrintf("[server: %v, state: %s] wait apply\n", rf.me, rf.state)
			rf.applyCond.Wait()
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	matchIndexs := make([]int, 0)

	for _, matchIdx := range rf.matchIndex {
		matchIndexs = append(matchIndexs, matchIdx)
	}

	sort.Ints(matchIndexs)
	lenPeer := len(rf.peers)
	majorityCommitIndex := -1

	if lenPeer%2 == 0 {
		majorityCommitIndex = matchIndexs[lenPeer/2-1]
	} else {
		majorityCommitIndex = matchIndexs[lenPeer/2]
	}

	// chi commit log o term hien tai cua leader (safety 5.4.2)
	if majorityCommitIndex > rf.commitIndex && rf.logs[rf.getPhysicalIndex(majorityCommitIndex)].Term == rf.currentTerm {
		rf.commitIndex = majorityCommitIndex
		rf.applyCond.Broadcast()
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshotRPC(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[server: %v, state: %s, term: %v] received InstallSnapshotRPC from server: %v\n", rf.me, rf.state, rf.currentTerm, args.LeaderId)

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	rf.updateLastPing()
	defer rf.persist()
	if args.Term > rf.currentTerm {
		rf.revertToFollower(args.Term)
	}

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	if args.LastIncludedIndex >= rf.getLastLogIndex() || rf.logs[rf.getPhysicalIndex(args.LastIncludedIndex)].Term != args.LastIncludedTerm {
		rf.logs = []LogEntry{
			{
				Term:    args.LastIncludedTerm,
				Command: nil,
				Index:   args.LastIncludedIndex,
			},
		}
	} else {
		// luon giu lai dummy entry khi snapshot
		rf.logs = rf.logs[rf.getPhysicalIndex(args.LastIncludedIndex):]
	}

	rf.currentLeader = args.LeaderId
	rf.snapshot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	if rf.lastIncludedIndex > rf.commitIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}

	rf.applyCond.Broadcast()
	DPrintf("[server: %v, state: %s, term: %v] received InstallSnapshotRPC from server: %v endddddd\n", rf.me, rf.state, rf.currentTerm, args.LeaderId)
}

func (rf *Raft) sendInstallSnapshotRPC(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.peers[server].Call("Raft.InstallSnapshotRPC", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer rf.persist()
	if rf.state != LEADER || rf.currentTerm != args.Term {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.revertToFollower(reply.Term)
		return
	}

	// update next index and match index
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	DPrintf("[server: %v, state: %s] nextIndex: %#v\nmatchIndex:%#v\n", rf.me, rf.state, rf.nextIndex, rf.matchIndex)
	rf.updateCommitIndex()
}

func (rf *Raft) getPhysicalIndex(actualIndex int) int {
	return actualIndex - rf.lastIncludedIndex
}
