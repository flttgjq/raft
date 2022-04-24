package raft

// AppendEntriesArgs
// For AppendEntriesRPC
type AppendEntriesArgs struct {
	Term         int // leader's Term
	LeaderId     int // in peers[]
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry // log entries to store
	LeaderCommit int     // leader commits index
}

// AppendEntriesReply
// For AppendEntriesRPC
type AppendEntriesReply struct {
	Term          int  // current Term
	Success       bool //
	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term          int
	LeaderId      int
	SnapshotIndex int
	SnapshotTerm  int
	Snapshot      []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) broadcastHeartBeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if rf.meState != LEADER {
			return
		}
		if peer == rf.me {
			rf.resetElectionTimer()
			continue
		}
		if isHeartBeat {
			go rf.replicate(peer)
		} else {
			rf.replicatorCond[peer].Signal()
		}
	}
}

func (rf *Raft) handleReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.meState != LEADER {
		return
	}

	if args.Term != rf.currentTerm {
		DPrintf("outdated reply")
		return
	}

	if reply.Term > rf.currentTerm {
		DPrintf("leader failed")
		rf.meState = FOLLOWER
		rf.currentTerm = reply.Term
		rf.persist()
		return
	}

	if reply.Success {
		if len(args.Entries) == 0 {
			return
		}
		// only update when its not a outdated op
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)

		// this log has already been commited, return directly

		// preCommitIndex := rf.commitIndex
		// for i := rf.commitIndex; i <= rf.matchIndex[server]; i++ {
		// 	count := 0
		// 	for p := range rf.peers {
		// 		if rf.matchIndex[p] >= i {
		// 			count += 1
		// 		}
		// 	}
		// 	if count > len(rf.peers)/2 && rf.log[i-rf.log[0].SnapshotIndex].Term == rf.currentTerm {
		// 		preCommitIndex = i
		// 	}
		// }

		majorityMatchIndex := findKthLargest(rf.matchIndex, len(rf.peers)/2+1)
		if majorityMatchIndex > rf.commitIndex && rf.log[majorityMatchIndex-rf.log[0].SnapshotIndex].Term == rf.currentTerm {
			rf.commitIndex = majorityMatchIndex
			DPrintf("leader matchIndex: %v\n", rf.matchIndex)
			if rf.commitIndex > rf.lastApplied {
				rf.applyCond.Signal()
			}
		}

		// rf.commitIndex = preCommitIndex
		// if rf.commitIndex > rf.lastApplied {
		// 	rf.applyCond.Signal()
		// }
	} else {
		if reply.ConflictIndex == -1 {
			return
		}
		if reply.ConflictTerm != -1 {
			// term conflict
			conflictIndex := -1
			for i := args.PrevLogIndex - rf.log[0].SnapshotIndex; i > 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					conflictIndex = i + rf.log[0].SnapshotIndex
					break
				}
			}
			if conflictIndex == -1 {
				rf.nextIndex[server] = reply.ConflictIndex
			} else {
				rf.nextIndex[server] = conflictIndex + 1
			}
		} else {
			rf.nextIndex[server] = reply.ConflictIndex
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	DPrintf("Node{%v} receive heartbeat in term %v from {Node%v}", rf.me, rf.currentTerm, args.LeaderId)
	reply.ConflictIndex, reply.ConflictTerm = -1, -1

	if args.Term < rf.currentTerm {
		DPrintf("{Node%v} receives a outdated AppendEntriesRPC", rf.me)
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}

	base := rf.log[0].SnapshotIndex
	rf.meState = FOLLOWER

	// 这个请求过期了，期间已经安装了快照，只是一个普通的心跳包，直接回复
	if args.PrevLogIndex < base {
		reply.Term, reply.Success = rf.currentTerm, false
		// reply.ConflictIndex, reply.ConflictTerm = base+1, -1
		DPrintf("info in snapshot, conflictindex=%v", base+1)
		return
	}

	// rf.electionTimer.Reset(generateRandTime())
	rf.resetElectionTimer()

	if len(rf.log)-1+base < args.PrevLogIndex {
		// 如果log不包含prevlogindex
		reply.Term, reply.Success = rf.currentTerm, false
		reply.ConflictIndex, reply.ConflictTerm = len(rf.log)+base, -1
		return
	} else if args.PrevLogTerm != rf.log[args.PrevLogIndex-base].Term {
		// 包含prevlogindex但term不一致, 不匹配S
		reply.Term, reply.Success = rf.currentTerm, false
		reply.ConflictTerm = rf.log[args.PrevLogIndex-base].Term
		for i := 0; i <= args.PrevLogIndex-base; i++ {
			if rf.log[i].Term == reply.ConflictTerm {
				reply.ConflictIndex = i + base
				// rf.log = append([]Entry(nil), rf.log[:i]...) // confilct trim log
				break
			}
		}
		return
	}

	for index, entry := range args.Entries {
		if entry.SnapshotIndex-base >= len(rf.log) || rf.log[entry.SnapshotIndex-base].Term != entry.Term {
			DPrintf("{Node%v} start append entries, len of past log is %v", rf.me, rf.log[len(rf.log)-1].SnapshotIndex)
			rf.log = append(rf.log[:entry.SnapshotIndex-base], args.Entries[index:]...)
			DPrintf("{Node%v} end append entries, len of now is %v", rf.me, rf.log[len(rf.log)-1].SnapshotIndex)
			break
		}
	}

	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].SnapshotIndex)
		if rf.commitIndex > rf.lastApplied {
			rf.applyCond.Signal()
		}
	}
	reply.Term, reply.Success = rf.currentTerm, true
}

func (rf *Raft) sendSnapshot(peer int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, args, reply)
	if ok && !rf.killed() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if args.Term != rf.currentTerm || rf.meState != LEADER {
			DPrintf("{Node %v} got a outdate snapshotRPC reply from {Node %v}, discard", rf.me, peer)
			return
		}
		if reply.Term > rf.currentTerm {
			rf.meState = FOLLOWER
			// rf.votedFor, rf.currentTerm = -1, reply.Term
			rf.currentTerm = reply.Term
			rf.persist()
			// rf.electionTimer.Reset(generateRandTime())
			// rf.resetElectionTimer()
			return
		}
		if reply.Term == rf.currentTerm && reply.Term != 0 {
			rf.nextIndex[peer] = args.SnapshotIndex + 1
			// rf.matchIndex[peer] = args.SnapshotIndex
		}
	}
	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		// rf.currentTerm, rf.votedFor = args.Term, -1
		rf.currentTerm = args.Term
		rf.persist()
		rf.meState = FOLLOWER
	}

	if args.SnapshotIndex < rf.log[0].SnapshotIndex {
		reply.Term = rf.currentTerm
		return
	}

	rf.meState = FOLLOWER
	// rf.electionTimer.Reset(generateRandTime())
	rf.resetElectionTimer()

	go func() {
		rf.ApplyMsgChan <- ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Snapshot,
			SnapshotTerm:  args.SnapshotTerm,
			SnapshotIndex: args.SnapshotIndex,
		}
	}()
	reply.Term = rf.currentTerm
	return
}
