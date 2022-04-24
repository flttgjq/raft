package raft

import (
	"container/heap"
	"log"
)

// Debug Debugging
const Debug = false

// const Debug = true

type IntHeap []int

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *IntHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

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

func findKthLargest(nums []int, k int) int {
	h := &IntHeap{}
	heap.Init(h)
	for _, item := range nums {
		heap.Push(h, item)
		if h.Len() > k {
			heap.Pop(h)
		}
	}
	return (*h)[0]
}
