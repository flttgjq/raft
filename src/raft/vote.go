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

func (rf *Raft) startElection() {
	request := rf.genVoteArgs()
	grantedVotes := 1
	rf.votedFor = rf.me
	rf.persist()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			response := new(RequestVoteReply)
			if rf.sendRequestVote(peer, request, response) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == request.Term && rf.meState == CANDIDATE {
					if response.VoteGranted {
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							DPrintf("{Node %v} receives majority votes in Term %v, receives %v vote", rf.me, rf.currentTerm, grantedVotes)
							for i := range rf.peers {
								if i == rf.me {
									continue
								}
								//rf.nextIndex[i] = rf.commitIndex + 1
								rf.nextIndex[i] = len(rf.log.Entries)
								rf.matchIndex[i] = 0
							}
							rf.meState = LEADER
							rf.votedFor = -1
							rf.persist()
							rf.broadcastHeartBeat()
							rf.heartbeatTimer.Reset(HEART_BEAT_TIMEOUT)
						}
					} else if response.Term > rf.currentTerm {
						//DPrintf("{Node %v} finds a new leader {Node %v} with Term %v and steps down in Term %v", rf.me, peer, response.Term, rf.currentTerm)
						rf.meState = FOLLOWER
						rf.currentTerm, rf.votedFor = response.Term, -1
						rf.persist()
					}
				}
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
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		// 已经选出新的leader，request term过期， 返回新的term
		//DPrintf("args.Term=%v  current Term=%v, current rf=%v", args, rf.votedFor, rf.me)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		DPrintf("")
		rf.meState = FOLLOWER
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
		//return
	}
	if !rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		DPrintf("node %v refuse to vote node %v because is not up to date", rf.me, args.CandidateId)
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	// granted
	rf.electionTimer.Reset(generateRandTime())
	rf.votedFor = args.CandidateId
	rf.persist()
	reply.VoteGranted = true
	//DPrintf("agree")
	reply.Term = args.Term
}

// isLogUpToDate check log
func (rf *Raft) isLogUpToDate(lastLogTerm int, lastLogIndex int) bool {
	if lastLogTerm > rf.log.Entries[len(rf.log.Entries)-1].Term {
		return true
	}
	if lastLogTerm < rf.log.Entries[len(rf.log.Entries)-1].Term {
		return false
	}
	if lastLogIndex < len(rf.log.Entries)-1 {
		return false
	}
	return true
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
		LastLogTerm:  rf.log.Entries[len(rf.log.Entries)-1].Term,
		LastLogIndex: len(rf.log.Entries) - 1,
	}
}
