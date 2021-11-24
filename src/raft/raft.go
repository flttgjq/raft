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
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// raft node state
const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

const (
	ELECTION_TIME_OUT_BASE = time.Millisecond * 150
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
	//Command string
	Term    int
	Command interface{}
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
	commitIndex    int
	lastApplied    int

	// volatile state on leader
	nextIndex  []int
	matchIndex []int

	ApplyMsgChan chan ApplyMsg
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func (rf *Raft) change_state(state int) {
	rf.meState = state
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

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

	term := rf.currentTerm
	isLeader := true
	// Your code here (2B).
	isLeader = rf.meState == LEADER
	//DPrintf("node %v: believes he is leader: %v", rf.me, isLeader)
	if !isLeader {
		return index, rf.currentTerm, isLeader
	}
	DPrintf("{node %v} service start: command:%v", rf.me, command)
	index = len(rf.log)
	//if rf.currentTerm == rf.log[len(rf.log)-1].Term && command == rf.log[len(rf.log) - 1].Command {
	//	// 命令相同，不写
	//
	//} else {
	//	rf.log = append(rf.log, Entry{Command: command, Term: term})
	//}
	rf.log = append(rf.log, Entry{Command: command, Term: term})
	DPrintf("cuurent leader %v's log %v", rf.me, rf.log)
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index
	rf.broadcastHeartBeat()
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

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//state := rf.meState
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.meState = CANDIDATE
			rf.currentTerm += 1
			rf.startElection()
			rf.electionTimer.Reset(generateRandTime()) // 重设超时时间
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.meState == LEADER {
				rf.broadcastHeartBeat()
				rf.heartbeatTimer.Reset(HEART_BEAT_TIMEOUT)
			}
			rf.mu.Unlock()
		}
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
	DPrintf("create server len= %v", len(peers))
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
	}
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	//rf.log = append(rf.log, Entry{Command: "null", Term: 0})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
