package raft

type InstallSnapShotArgs struct {
	term              int // leader's term
	leaderId          int //leader's id
	lastIncludedIndex int // snapshot contain's final index
	lastIncludedTerm  int
	offset            int
	data              []byte
	done              bool
}

type InstallSnapShotReply struct {
	term int // current term
}
