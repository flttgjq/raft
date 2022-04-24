package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"

	"6.824/labgob"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

// raft node state
const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

const (
	ELECTION_TIME_OUT_BASE = time.Millisecond * 350
	HEART_BEAT_TIMEOUT     = time.Millisecond * 120
)

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Entry log entry
type Entry struct {
	Term          int
	Command       interface{}
	SnapshotIndex int
}

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).

	// persistent state on all servers
	currentTerm int
	votedFor    int
	log         []Entry

	// volatile state on all servers
	meState        int // 当前状态
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	timerLock      sync.Mutex
	commitIndex    int
	lastApplied    int

	// volatile state on leader
	nextIndex  []int
	matchIndex []int

	ApplyMsgChan chan ApplyMsg
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCond      *sync.Cond
	replicatorCond []*sync.Cond
}

func (rf *Raft) resetElectionTimer() {
	rf.timerLock.Lock()
	defer rf.timerLock.Unlock()
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C: //try to drain from the channel
		default:
		}
	}
	rf.electionTimer.Reset(generateRandTime())
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = false
	// Your code here (2A).
	term = rf.currentTerm
	if rf.meState == LEADER {
		isleader = true
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	buffer := new(bytes.Buffer)
	enc := labgob.NewEncoder(buffer)
	err := enc.Encode(rf.log)
	if err != nil {
		return
	}
	err = enc.Encode(rf.votedFor)
	if err != nil {
		return
	}
	err = enc.Encode(rf.currentTerm)
	if err != nil {
		return
	}
	rf.persister.SaveRaftState(buffer.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2C).
	read := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(read)
	var votedFor int
	var log []Entry
	var currentTerm int
	if decoder.Decode(&log) != nil || decoder.Decode(&votedFor) != nil || decoder.Decode(&currentTerm) != nil {
		return
	} else {
		rf.votedFor = votedFor
		rf.log = log
		rf.currentTerm = currentTerm
	}
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	buffer := new(bytes.Buffer)
	enc := labgob.NewEncoder(buffer)
	err := enc.Encode(rf.log)
	if err != nil {
		return
	}
	err = enc.Encode(rf.votedFor)
	if err != nil {
		return
	}
	err = enc.Encode(rf.currentTerm)
	if err != nil {
		return
	}
	rf.persister.SaveStateAndSnapshot(buffer.Bytes(), snapshot)
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("{Node%v} receives a outdated snapshot, refuse", rf.me)
		return false
	}

	if lastIncludedIndex <= rf.log[len(rf.log)-1].SnapshotIndex && rf.log[lastIncludedIndex-rf.log[0].SnapshotIndex].Term == lastIncludedTerm {
		rf.log = append([]Entry(nil), rf.log[lastIncludedIndex-rf.log[0].SnapshotIndex:]...)
	} else {
		rf.log = make([]Entry, 1)
	}

	rf.log[0].SnapshotIndex = lastIncludedIndex
	rf.log[0].Term = lastIncludedTerm
	rf.log[0].Command = nil
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.persistStateAndSnapshot(snapshot)
	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("start create snapshot")
	if index <= rf.log[0].SnapshotIndex {
		DPrintf("{Node %v} refuse to create snapshot, outdated", rf.me)
		return
	}
	rf.log = append([]Entry(nil), rf.log[index-rf.log[0].SnapshotIndex:]...)
	rf.log[0].Command = nil
	rf.persistStateAndSnapshot(snapshot)
}

// gen rand time from 150-300
func generateRandTime() time.Duration {
	rand.Seed(time.Now().UnixNano())
	randomTime := time.Duration(rand.Int63n(150))
	return ELECTION_TIME_OUT_BASE + randomTime*time.Millisecond
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return -1, -1, false
	}
	term := rf.currentTerm
	isLeader := true
	// Your code here (2B).
	isLeader = rf.meState == LEADER
	if !isLeader {
		return index, rf.currentTerm, isLeader
	}
	DPrintf("{Node %v} receives a new command[%v] to replicate in term %v", rf.me, command, rf.currentTerm)
	index = len(rf.log) + rf.log[0].SnapshotIndex
	rf.log = append(rf.log, Entry{Command: command, Term: term, SnapshotIndex: index})
	rf.persist()
	rf.matchIndex[rf.me] = index
	// rf.nextIndex[rf.me] = index
	rf.broadcastHeartBeat(false)
	rf.heartbeatTimer.Reset(HEART_BEAT_TIMEOUT)
	return index, term, isLeader
}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	// DPrintf("{Node%v} has been killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) set_candidate() {
	rf.mu.Lock()
	if rf.meState == LEADER { //check if leader
		rf.mu.Unlock()
		return
	}

	rf.currentTerm++
	rf.meState = CANDIDATE

	//fmt.Printf("elect of process %d, term is %d\n", rf.me, rf.currentTerm)
	currentTerm := rf.currentTerm
	args := rf.genVoteArgs()
	rf.votedFor = rf.me //vote for itself
	rf.persist()
	rf.mu.Unlock()
	rf.startElection(args, currentTerm)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionTimer.C:
			DPrintf("{Node%v}'s etimer finish", rf.me)
			go rf.set_candidate()
			rf.electionTimer.Reset(generateRandTime())
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.meState == LEADER {
				// DPrintf("{Node%v} send heartbeat, nextIndex=%v\n matchIndex=%v\n", rf.me, rf.nextIndex, rf.matchIndex)
				rf.broadcastHeartBeat(true)
				rf.heartbeatTimer.Reset(HEART_BEAT_TIMEOUT)
			}
			rf.mu.Unlock()
		}
	}
	// DPrintf("raft server%v has been killed", rf.me)
}

// applier
// use condvar to sync applymsg
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
			if rf.killed() {
				return
			}
		}
		base, lastApplied, commitIndex := rf.log[0].SnapshotIndex, rf.lastApplied, rf.commitIndex
		entries := make([]Entry, commitIndex-lastApplied)
		copy(entries, rf.log[lastApplied+1-base:commitIndex+1-base])
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.ApplyMsgChan <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.SnapshotIndex,
			}
		}
		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v-%v in term %v", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

func (rf *Raft) replicate(peer int) {
	rf.mu.Lock()
	if rf.meState != LEADER || rf.killed() {
		rf.mu.Unlock()
		return
	}
	PrevLogIndex := rf.nextIndex[peer] - 1
	if PrevLogIndex < rf.log[0].SnapshotIndex {
		requests := InstallSnapshotArgs{
			Term:          rf.currentTerm,
			LeaderId:      rf.me,
			SnapshotIndex: rf.log[0].SnapshotIndex,
			SnapshotTerm:  rf.log[0].Term,
			Snapshot:      rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()
		DPrintf("{Node%v} send snapshot%v to{Node%v}", rf.me, requests, peer)
		rf.sendSnapshot(peer, &requests)
	} else {
		request := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: PrevLogIndex,
			LeaderCommit: rf.commitIndex,
		}
		if PrevLogIndex > 0 {
			request.PrevLogTerm = rf.log[request.PrevLogIndex-rf.log[0].SnapshotIndex].Term
		}
		request.Entries = make([]Entry, len(rf.log[rf.nextIndex[peer]-rf.log[0].SnapshotIndex:]))
		copy(request.Entries, rf.log[rf.nextIndex[peer]-rf.log[0].SnapshotIndex:])
		if len(request.Entries) != 0 {
			DPrintf("leader {Node%v} send log%v-%v to {Node%v}", rf.me, request.Entries[0].SnapshotIndex, request.Entries[len(request.Entries)-1].SnapshotIndex, peer)
		}
		rf.mu.Unlock()
		response := AppendEntriesReply{}
		if rf.sendAppendEntries(peer, &request, &response) {
			rf.handleReply(peer, &request, &response)
		}
	}
}

func (rf *Raft) need_replicate(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.meState == LEADER && rf.nextIndex[peer] <= rf.log[len(rf.log)-1].SnapshotIndex
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		// only wake up when need
		for !rf.need_replicate(peer) {
			rf.replicatorCond[peer].Wait()
		}
		rf.replicate(peer)
	}
}

// Make
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		meState:        FOLLOWER,
		votedFor:       -1,
		currentTerm:    0,
		electionTimer:  time.NewTimer(generateRandTime()),
		heartbeatTimer: time.NewTimer(HEART_BEAT_TIMEOUT),
		commitIndex:    0,
		lastApplied:    0,
		log:            make([]Entry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		ApplyMsgChan:   applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
	}

	rf.applyCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.log[0].SnapshotIndex + 1
	}
	rf.lastApplied = rf.log[0].SnapshotIndex
	rf.commitIndex = rf.log[0].SnapshotIndex
	// start ticker goroutine to start elections
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
		go rf.replicator(i)
	}
	go rf.ticker()
	go rf.applier()

	return rf
}
