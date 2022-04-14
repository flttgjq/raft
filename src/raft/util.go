package raft

import "log"

// Debug Debugging
const Debug = false

// const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) ifPrevLogEntryExist(prevLogTerm int, prevLogIndex int) bool {
	if len(rf.log) < prevLogIndex {
		return false
	}
	if rf.log[prevLogIndex].Term == prevLogTerm {
		return true
	} else {
		return false
	}
}

func min(x1 int, x2 int) int {
	if x1 > x2 {
		return x2
	} else {
		return x1
	}
}

func max(x1 int, x2 int) int {
	if x1 > x2 {
		return x1
	} else {
		return x2
	}
}
