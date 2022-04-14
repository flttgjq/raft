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

func (rf *Raft) broadcastHeartBeat() {
	for peer := range rf.peers {
		if rf.meState != LEADER {
			return
		}
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
			if args.PrevLogIndex < rf.log[0].SnapshotIndex {
				DPrintf("need install snapshot, prevlogindex=%v, snapshotindex=%v", args.PrevLogIndex, rf.log[0].SnapshotIndex)
				args := &InstallSnapshotArgs{
					Term:          rf.currentTerm,
					LeaderId:      rf.me,
					SnapshotIndex: rf.log[0].SnapshotIndex,
					SnapshotTerm:  rf.log[0].Term,
					Snapshot:      rf.persister.snapshot,
				}
				go rf.sendSnapshot(peer, args)
				continue
			} else {
				args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.log[0].SnapshotIndex].Term
			}

		}
		// 是否包含未加入的log 即日志小于leader的日志
		if rf.nextIndex[peer] < len(rf.log)+rf.log[0].SnapshotIndex && rf.nextIndex[peer] > rf.log[0].SnapshotIndex {
			args.Entries = append([]Entry(nil), rf.log[rf.nextIndex[peer]-rf.log[0].SnapshotIndex:]...)
		}
		go func(peer int) {
			reply := AppendEntriesReply{}
			DPrintf("server send appendEntries to server%v have %v Entries, args=%v", peer, len(args.Entries), args)
			ok := rf.sendAppendEntries(peer, &args, &reply)
			if ok && !rf.killed() {
				rf.handleReply(peer, &args, &reply)
			}
		}(peer)
	}
}

func (rf *Raft) handleReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.meState != LEADER {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.meState = FOLLOWER
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.electionTimer.Reset(generateRandTime())
		return
	}

	if reply.Success {
		// only update when its not a outdated op
		if rf.nextIndex[server] < len(args.Entries)+args.PrevLogIndex+1 {
			rf.nextIndex[server] = len(args.Entries) + args.PrevLogIndex + 1 // 更新至leader的最新log+1
		}
		if rf.matchIndex[server] < args.PrevLogIndex+len(args.Entries) {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		}

		// this log has already been commited, return directly
		if rf.matchIndex[server] <= rf.log[0].SnapshotIndex || rf.matchIndex[server] <= rf.commitIndex {
			DPrintf("this log has already been commited")
			return
		}

		DPrintf("current commitindex=%v, matchIndex[server%v]=%v", rf.commitIndex, server, rf.matchIndex[server])
		numCommit := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= rf.matchIndex[server] {
				numCommit++
				if numCommit > (len(rf.peers)/2) && rf.log[rf.matchIndex[server]-rf.log[0].SnapshotIndex].Term == rf.currentTerm && rf.commitIndex < rf.matchIndex[server] {
					// 原则：只提交自己任期内的log
					//      一条log只提交1次
					//      只有多数同一在提交
					rf.commitIndex = rf.matchIndex[server]
					rf.applyCond.Signal()
				}
			}
		}
	} else {
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

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	rf.currentTerm = args.Term
	rf.meState = FOLLOWER
	rf.votedFor = -1
	base := rf.log[0].SnapshotIndex
	rf.electionTimer.Reset(generateRandTime())

	// 信息都在快照中，只是一个普通的心跳包，直接回复
	if args.PrevLogIndex < base {
		reply.Term, reply.Success = rf.currentTerm, false
		reply.ConflictIndex, reply.ConflictTerm = base+1, -1
		DPrintf("info in snapshot, conflictindex=%v", base+1)
		return
	}

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
				break
			}
		}
		return
	}

	// 此时已经包含了prevlogindex, 检查是否是log的最后一个元素
	if args.PrevLogIndex != len(rf.log)-1+base {
		rf.log = rf.log[:args.PrevLogIndex+1-base] // 删除之后的所有
	}

	if args.Entries != nil {
		rf.log = append(rf.log, args.Entries...)
	}

	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1+base)
		rf.applyCond.Signal()
	}
	reply.Term, reply.Success = args.Term, true
}

func (rf *Raft) sendSnapshot(peer int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, args, reply)
	if ok && !rf.killed() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term <= rf.currentTerm && reply.Term != 0 {
			rf.nextIndex[peer] = args.SnapshotIndex + 1
			rf.matchIndex[peer] = args.SnapshotIndex
		} else {
			rf.currentTerm = reply.Term
			rf.meState = FOLLOWER
			rf.persist()
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
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}

	rf.meState = FOLLOWER
	rf.electionTimer.Reset(generateRandTime())

	// < commitIndex not base
	if args.SnapshotIndex <= rf.commitIndex {
		DPrintf("[Warning]: network not stable, outdated Snapshot RPC arrive in server %v", rf.me)
		return
	}

	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.SnapshotTerm,
		SnapshotIndex: args.SnapshotIndex,
	}

	go func() { rf.ApplyMsgChan <- msg }()
	reply.Term = rf.currentTerm
	return
}
