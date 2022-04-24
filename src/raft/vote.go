package raft

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogTerm  int
	LastLogIndex int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) startElection(request *RequestVoteArgs, currentTerm int) {
	grantedVotes := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			response := new(RequestVoteReply)
			DPrintf("{Node%v} send requestvote to {Node%v} in term%v", rf.me, peer, currentTerm)
			if rf.sendRequestVote(peer, request, response) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if request.Term != rf.currentTerm {
					return
				}
				if currentTerm < rf.currentTerm || rf.meState != CANDIDATE {
					return
				}
				if response.Term > rf.currentTerm {
					rf.meState = FOLLOWER
					rf.currentTerm = response.Term
					rf.persist()
					return
				}
				// if rf.currentTerm == request.Term && rf.meState == CANDIDATE {
				if response.VoteGranted {
					DPrintf("{Node%v} receive granted from {Node%v}", rf.me, peer)
					grantedVotes += 1
					if grantedVotes > len(rf.peers)/2 {
						DPrintf("{Node %v} receives majority votes in Term %v, receives %v vote", rf.me, rf.currentTerm, grantedVotes)
						for i := range rf.peers {
							if i == rf.me {
								continue
							}
							rf.nextIndex[i] = rf.log[len(rf.log)-1].SnapshotIndex + 1
							rf.matchIndex[i] = 0
						}
						rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].SnapshotIndex
						rf.meState = LEADER

						rf.heartbeatTimer.Reset(HEART_BEAT_TIMEOUT)
						rf.broadcastHeartBeat(true)
						return
					}
				}
				// }
			}
		}(peer)
	}
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// candidate Term out of date reject
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm {
		// 已经选出新的leader，request term过期， 返回新的term
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.meState = FOLLOWER
		rf.votedFor, rf.currentTerm = -1, args.Term
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted, reply.Term = false, rf.currentTerm
		return
	}

	if !rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	// granted
	// rf.electionTimer.Reset(generateRandTime())
	rf.resetElectionTimer()
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
}

// isLogUpToDate check log
func (rf *Raft) isLogUpToDate(lastLogTerm int, lastLogIndex int) bool {
	lastIndex := rf.log[len(rf.log)-1].SnapshotIndex
	lastTerm := rf.log[len(rf.log)-1].Term
	return lastLogTerm > lastTerm || (lastLogTerm == lastTerm && lastLogIndex >= lastIndex)
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// genVoteArgs
// generate RequestVoteArgs
func (rf *Raft) genVoteArgs() *RequestVoteArgs {
	return &RequestVoteArgs{Term: rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
		LastLogIndex: rf.log[len(rf.log)-1].SnapshotIndex,
	}
}
