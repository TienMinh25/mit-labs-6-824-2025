package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TienMinh25/mit-labs-6-824-2025/labgob"
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
	state       RaftState
	currentTerm int
	votedFor    int
	logs        []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan raftapi.ApplyMsg

	lastAccessed time.Time

	// additional field for snapshot operation (log compaction)
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte

	// additional field for handle last timeout =))
	lastTimeoutDuration time.Duration
}

type RaftState int

const (
	FOLLOWER RaftState = iota
	CANDIDATE
	LEADER
)

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int // Absolute index trong toàn bộ log
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

// Helper functions được cải tiến
func (rf *Raft) getLastLogIndex() int {
	if len(rf.logs) == 0 {
		return rf.lastIncludedIndex
	}
	return rf.logs[len(rf.logs)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.logs) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.logs[len(rf.logs)-1].Term
}

// Tìm log entry theo absolute index
func (rf *Raft) getLogEntry(absoluteIndex int) *LogEntry {
	if absoluteIndex <= rf.lastIncludedIndex {
		return nil // Entry đã bị snapshot
	}

	for i := range rf.logs {
		if rf.logs[i].Index == absoluteIndex {
			return &rf.logs[i]
		}
	}
	return nil
}

// Lấy term của một absolute index
func (rf *Raft) getTermAtIndex(absoluteIndex int) int {
	if absoluteIndex == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}

	entry := rf.getLogEntry(absoluteIndex)
	if entry == nil {
		return -1 // Invalid index
	}

	return entry.Term
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
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
func (rf *Raft) readPersist(data []byte, snapshotRecover []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	decode := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	rf.snapshot = snapshotRecover

	if decode.Decode(&currentTerm) == nil &&
		decode.Decode(&votedFor) == nil &&
		decode.Decode(&logs) == nil &&
		decode.Decode(&lastIncludedIndex) == nil &&
		decode.Decode(&lastIncludedTerm) == nil {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}

	// Tìm entry tại index để lấy term
	entry := rf.getLogEntry(index)
	if entry == nil {
		return // Invalid index
	}

	rf.lastIncludedTerm = entry.Term
	rf.lastIncludedIndex = index
	rf.snapshot = snapshot

	// Trim logs - chỉ giữ entries sau index
	newLogs := []LogEntry{}
	for _, logEntry := range rf.logs {
		if logEntry.Index > index {
			newLogs = append(newLogs, logEntry)
		}
	}

	rf.logs = newLogs
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Printf("request vote from [candidate: %v, term: %v] to [follower: %v, state: %v, term: %v]\n", args.CandidateId, args.Term, rf.me, rf.state, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// idempotent
	if args.Term == rf.currentTerm && rf.votedFor == args.CandidateId {
		reply.Term, reply.VoteGranted = rf.currentTerm, true
		return
	}

	// reply false if term < currentTerm or if currentTerm = term and granted vote before
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	// neu da bat dau 1 cuoc bau cu moi -> set lai votedFor la -1
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.state = FOLLOWER
		rf.persist()
	}

	// check up to date log (log election restriction)
	lastLogTerm := rf.getLastLogTerm()
	lastLogIndex := rf.getLastLogIndex()

	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	rf.state = FOLLOWER
	rf.lastAccessed = time.Now()
	reply.Term, reply.VoteGranted = args.Term, true
	rf.votedFor = args.CandidateId
	rf.lastAccessed = time.Now()
	rf.persist()
	fmt.Printf("request vote from candidate %v to follower %v end\n", args.CandidateId, rf.me)
}

// example code to send a RequestVote RPC to a server.
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
	_, isLeader := rf.GetState()
	if !isLeader {
		return 0, 0, false
	}

	rf.mu.Lock()

	newIndex := rf.getLastLogIndex() + 1
	newEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
		Index:   newIndex,
	}

	rf.logs = append(rf.logs, newEntry)
	rf.nextIndex[rf.me] = newIndex + 1
	rf.matchIndex[rf.me] = newIndex
	rf.persist()
	rf.mu.Unlock()

	fmt.Printf("Leader %v added new log entry at index %v: %v\n", rf.me, newIndex, command)
	return newIndex, rf.currentTerm, isLeader
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.state == FOLLOWER {
			timeDiff := time.Since(rf.lastAccessed).Milliseconds()

			if timeDiff >= (rf.lastTimeoutDuration * time.Millisecond).Milliseconds() {
				rf.state = CANDIDATE
			}
		}

		rf.lastTimeoutDuration = getRandomizedTime()
		ms := rf.lastTimeoutDuration * time.Millisecond
		rf.mu.Unlock()
		time.Sleep(ms)
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
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = []LogEntry{} // Bỏ dummy entry
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.lastAccessed = time.Now()
	rf.applyCh = applyCh
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	rf.lastTimeoutDuration = getRandomizedTime()
	rf.lastAccessed = time.Now()

	// start ticker goroutine to start elections
	go rf.runServer()
	go rf.ticker()

	return rf
}

func (rf *Raft) runServer() {
	for !rf.killed() {
		rf.mu.Lock()
		status := rf.state
		rf.mu.Unlock()

		switch status {
		case FOLLOWER:
			// rf.manageFollower()
		case CANDIDATE:
			rf.manageCandidate()
		case LEADER:
			rf.manageLeader()
		}

		time.Sleep(35 * time.Millisecond)
	}
}

// func (rf *Raft) manageFollower() {
// 	timeout := getRandomizedTime() * time.Millisecond
// 	time.Sleep(timeout)

// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	if rf.state == FOLLOWER && time.Since(rf.lastAccessed).Milliseconds() >= timeout.Milliseconds() {
// 		rf.state = CANDIDATE
// 	}
// }

func (rf *Raft) manageCandidate() {
	timeout := getRandomizedTime() * time.Millisecond
	start := time.Now()
	fmt.Printf("candidate %v start new election\n", rf.me)

	rf.mu.Lock()

	countingVote := 0
	majorityAccepts := len(rf.peers)/2 + 1
	finished := 0
	peers := len(rf.peers)

	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	me := rf.me
	term := rf.currentTerm
	rf.votedFor = rf.me
	rf.lastAccessed = time.Now()

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
			ok := rf.sendRequestVote(serverID, &args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			finished++

			if !ok {
				return
			}

			if reply.VoteGranted {
				countingVote++
			} else if reply.Term > term {
				rf.state = FOLLOWER
				rf.currentTerm = reply.Term
				rf.persist()
			}
		}(idx)
	}

	for {
		rf.mu.Lock()
		if finished == peers || countingVote >= majorityAccepts || time.Since(start).Milliseconds() > timeout.Milliseconds() {
			break
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}

	// if timeout, return
	if time.Since(start).Milliseconds() > timeout.Milliseconds() {
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
		rf.mu.Unlock()
		return
	}

	// if not timeout, check condition pass
	if rf.state == CANDIDATE && countingVote >= majorityAccepts {
		rf.state = LEADER
		fmt.Printf("candidate %v win election\n", rf.me)

		// initialize nextIndex and matchIndex
		for peer := range peers {
			rf.nextIndex[peer] = rf.getLastLogIndex() + 1
			rf.matchIndex[peer] = 0
		}
		rf.matchIndex[rf.me] = rf.getLastLogIndex()
	} else {
		rf.state = FOLLOWER
	}

	rf.votedFor = -1
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) manageLeader() {
	rf.mu.Lock()
	rf.nextIndex[rf.me] = rf.getLastLogIndex() + 1
	rf.matchIndex[rf.me] = rf.getLastLogIndex()
	rf.updateCommitIndex()
	rf.mu.Unlock()

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}

		go func(peerId int) {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}

			nextIndex := rf.nextIndex[peerId]

			// Check if need to send snapshot
			if nextIndex <= rf.lastIncludedIndex {
				args := InstallSnapshotRPCArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              rf.snapshot,
					Offset:            0,
					Done:              true,
				}
				term := rf.currentTerm
				rf.mu.Unlock()

				var reply InstallSnapshotRPCReply
				if rf.sendInstallSnapshotRPC(peerId, &args, &reply) {
					rf.mu.Lock()
					if reply.Term > term {
						rf.state = FOLLOWER
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
					} else if rf.state == LEADER && term == rf.currentTerm {
						rf.nextIndex[peerId] = rf.lastIncludedIndex + 1
						rf.matchIndex[peerId] = rf.lastIncludedIndex
					}
					rf.mu.Unlock()
				}
				return
			}

			// Send AppendEntries
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: nextIndex - 1,
				PrevLogTerm:  rf.getTermAtIndex(nextIndex - 1),
				LeaderCommit: rf.commitIndex,
			}

			// Collect entries to send
			for _, entry := range rf.logs {
				if entry.Index >= nextIndex {
					args.Entries = append(args.Entries, entry)
				}
			}

			term := rf.currentTerm
			rf.mu.Unlock()

			var reply AppendEntriesReply
			if !rf.sendAppendEntries(peerId, &args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > term {
				rf.state = FOLLOWER
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				return
			}

			if rf.state != LEADER || term != rf.currentTerm {
				return
			}

			if reply.Success {
				rf.matchIndex[peerId] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1
				rf.updateCommitIndex()
			} else {
				// Handle conflict
				if reply.ConflictTerm == -1 {
					rf.nextIndex[peerId] = reply.MaxLen
				} else {
					// Tìm last entry với ConflictTerm
					found := false
					for i := rf.getLastLogIndex(); i > rf.lastIncludedIndex; i-- {
						if rf.getTermAtIndex(i) == reply.ConflictTerm {
							rf.nextIndex[peerId] = i + 1
							found = true
							break
						}
					}
					if !found {
						rf.nextIndex[peerId] = reply.ConflictIndex
					}
				}

				// Ensure nextIndex doesn't go below lastIncludedIndex + 1
				if rf.nextIndex[peerId] <= rf.lastIncludedIndex {
					rf.nextIndex[peerId] = rf.lastIncludedIndex + 1
				}
			}
		}(idx)
	}
}

// updateCommitIndex the function is serve for commit index for case normal operation
// also case in safety
func (rf *Raft) updateCommitIndex() {

	var applyMsgs []raftapi.ApplyMsg

	for index := rf.commitIndex + 1; index <= rf.getLastLogIndex(); index++ {
		// only commit entries from current term (safety requirement)
		if rf.getTermAtIndex(index) != rf.currentTerm {
			continue
		}

		count := 1 // Count self
		for peerId := range rf.peers {
			if peerId != rf.me && rf.matchIndex[peerId] >= index {
				count++
			}
		}

		if count >= (len(rf.peers)/2 + 1) {
			// Apply entries from lastApplied+1 to index
			for i := rf.lastApplied + 1; i <= index; i++ {
				if i <= rf.lastIncludedIndex {
					rf.lastApplied = i // Update lastApplied để skip snapshotted entries
					continue
				}

				entry := rf.getLogEntry(i)
				if entry != nil {
					fmt.Printf("Leader %v applying log index %v: [Term: %v, Command: %v]\n",
						rf.me, i, entry.Term, entry.Command)
					applyMsgs = append(applyMsgs, raftapi.ApplyMsg{
						CommandValid: true,
						Command:      entry.Command,
						CommandIndex: i,
					})
					rf.lastApplied = i
				}
			}
			rf.commitIndex = index
		}
	}

	// Unlock trước khi send messages (gọi từ caller)
	// Send messages trong separate goroutine để tránh block
	go rf.applyLogs(applyMsgs)
}

func (rf *Raft) applyLogs(msgs []raftapi.ApplyMsg) {
	rf.mu.Lock()

	for _, msg := range msgs {
		if rf.lastIncludedIndex > msg.CommandIndex {
			continue
		}

		rf.applyCh <- msg
	}

	rf.mu.Unlock()
}

func getRandomizedTime() time.Duration {
	return time.Duration(150 + rand.Int63()%300)
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
	MaxLen        int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	fmt.Printf("follower %v received append entry from [leader %v, term leader: %v, len entries: %v]\n", rf.me, args.LeaderId, args.Term, len(args.Entries))
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1
	reply.MaxLen = rf.getLastLogIndex() + 1
	reply.Success = false

	// Rule 1: Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	// Update term if needed
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.state = FOLLOWER
	fmt.Printf("follower %v updated last accessed from leader %v\n", rf.me, args.LeaderId)
	rf.lastAccessed = time.Now()

	// Rule 2: Check if log contains entry at prevLogIndex with matching term
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.MaxLen = rf.getLastLogIndex() + 1
		rf.mu.Unlock()
		return
	}

	if args.PrevLogIndex > rf.lastIncludedIndex {
		prevTerm := rf.getTermAtIndex(args.PrevLogIndex)
		if prevTerm != args.PrevLogTerm {
			reply.ConflictTerm = prevTerm
			// Tìm first index với conflict term
			for i := rf.lastIncludedIndex + 1; i <= args.PrevLogIndex; i++ {
				if rf.getTermAtIndex(i) == prevTerm {
					reply.ConflictIndex = i
					break
				}
			}
			rf.mu.Unlock()
			return
		}
	} else if args.PrevLogIndex == rf.lastIncludedIndex {
		if rf.lastIncludedTerm != args.PrevLogTerm {
			reply.ConflictIndex = rf.lastIncludedIndex
			rf.mu.Unlock()
			return
		}
	}

	// Rule 3 & 4: Handle conflicting entries và append new ones
	if len(args.Entries) > 0 {
		// Tìm vị trí để bắt đầu append/overwrite
		startAppendIndex := args.PrevLogIndex + 1

		// Remove conflicting entries
		newLogs := []LogEntry{}
		for _, entry := range rf.logs {
			if entry.Index < startAppendIndex {
				newLogs = append(newLogs, entry)
			}
		}

		// Append new entries với đúng index
		for i, entry := range args.Entries {
			newEntry := LogEntry{
				Command: entry.Command,
				Term:    entry.Term,
				Index:   startAppendIndex + i,
			}
			newLogs = append(newLogs, newEntry)
		}

		rf.logs = newLogs
		rf.persist()
	}

	reply.Success = true

	var applyMsgs []raftapi.ApplyMsg
	// Rule 5: Update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())

		// Apply committed entries
		for i := oldCommitIndex + 1; i <= rf.commitIndex; i++ {
			if i <= rf.lastIncludedIndex {
				rf.lastApplied = i // Update lastApplied để skip snapshotted entries
				continue
			}

			entry := rf.getLogEntry(i)
			if entry != nil {
				applyMsgs = append(applyMsgs, raftapi.ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: i,
				})
				rf.lastApplied = i
			}
		}
	}

	rf.mu.Unlock()

	// Send messages sau khi unlock
	go func() {
		for _, msg := range applyMsgs {
			rf.applyCh <- msg
		}
	}()

}

func (rf *Raft) sendAppendEntries(peerId int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peerId].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotRPCArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	Offset            int
	Done              bool
}

type InstallSnapshotRPCReply struct {
	Term int
}

func (rf *Raft) InstallSnapshotRPC(args *InstallSnapshotRPCArgs, reply *InstallSnapshotRPCReply) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	// update term if needed
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.lastAccessed = time.Now()
	rf.state = FOLLOWER
	reply.Term = rf.currentTerm

	// check is needed to apply state machine or compact log
	if args.LastIncludedIndex <= rf.commitIndex || args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	// trim log if needed
	if args.LastIncludedIndex <= rf.getLastLogIndex() && rf.getTermAtIndex(args.LastIncludedIndex) == args.LastIncludedTerm {
		newLogs := []LogEntry{}
		for _, entry := range rf.logs {
			if entry.Index > args.LastIncludedIndex {
				newLogs = append(newLogs, entry)
			}
		}
		rf.logs = newLogs
	} else {
		rf.logs = []LogEntry{}
	}

	// update state
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = args.Data
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	rf.persist()
	rf.mu.Unlock()

	// apply state machine because it is slow follower
	if args.Done {
		go func() {
			rf.applyCh <- raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      args.Data,
				SnapshotTerm:  args.LastIncludedTerm,
				SnapshotIndex: args.LastIncludedIndex,
			}
		}()
	}
}

func (rf *Raft) sendInstallSnapshotRPC(peerId int, args *InstallSnapshotRPCArgs, reply *InstallSnapshotRPCReply) bool {
	return rf.peers[peerId].Call("Raft.InstallSnapshotRPC", args, reply)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
