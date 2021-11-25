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
	Term        int  // current Term
	Success     bool //
	CommitIndex int
}

func (rf *Raft) broadcastHeartBeat() {
	for peer := range rf.peers {
		if peer == rf.me {
			rf.electionTimer.Reset(generateRandTime())
			continue
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[peer] - 1,
			LeaderCommit: rf.commitIndex,
		}
		// prevLogIndex >= 0 表示存在合法的Prev log, 更新Term
		if args.PrevLogIndex >= 0 {
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		}
		// 是否包含未加入的log 即日志小于leader的日志
		if rf.nextIndex[peer] < len(rf.log) {
			args.Entries = rf.log[rf.nextIndex[peer]:]
		}
		go func(peer int) {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(peer, &args, &reply)
			if ok {
				rf.handleReply(peer, &reply)
			}
		}(peer)
	}
}

func (rf *Raft) handleReply(server int, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.meState != LEADER {
		return
	}

	if reply.Term > rf.currentTerm {
		DPrintf("leader failed election restart")
		rf.meState = FOLLOWER
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.electionTimer.Reset(generateRandTime())
		return
	}

	if reply.Success {
		rf.nextIndex[server] = len(rf.log) // 更新至leader的最新log+1
		rf.matchIndex[server] = len(rf.log) - 1
		numCommit := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= rf.matchIndex[server] {
				numCommit++
				if numCommit > (len(rf.peers)/2) && rf.log[rf.matchIndex[server]].Term == rf.currentTerm && rf.commitIndex < rf.matchIndex[server] {
					// 原则：只提交自己任期内的log
					//      一条log只提交1次
					//      只有多数同一在提交
					rf.commitIndex = rf.matchIndex[server]
					go rf.commitLog()
				}
			}
		}
	} else {
		rf.nextIndex[server]-- // 将nextindex-1重试
		//rf.broadcastHeartBeat()
	}
}

func (rf *Raft) commitLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.ApplyMsgChan <- ApplyMsg{
			CommandValid: true,
			CommandIndex: i,
			Command:      rf.log[i].Command,
		}
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	rf.currentTerm = args.Term
	rf.meState = FOLLOWER
	rf.votedFor = -1
	rf.electionTimer.Reset(generateRandTime())

	if len(rf.log)-1 < args.PrevLogIndex {
		// 如果log不包含prevlogindex
		reply.Term, reply.Success = rf.currentTerm, false
		return
	} else if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		// 包含prevlogindex但term不一致, 不匹配S
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	// 此时已经包含了prevlogindex, 检查是否是log的最后一个元素
	if args.PrevLogIndex != len(rf.log)-1 {
		rf.log = rf.log[:args.PrevLogIndex+1] // 删除之后的所有
	}

	if args.Entries != nil {
		//DPrintf("append %v %v", args.Entries, rf.log[args.PrevLogIndex])
		rf.log = append(rf.log, args.Entries...)
		//DPrintf("node {%v}'s log %v", rf.me, rf.log)
	}

	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		go rf.commitLog()
	}
	reply.Term, reply.Success = args.Term, true
}
