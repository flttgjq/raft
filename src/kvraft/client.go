package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeaderIndex int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.lastLeaderIndex = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	request := GetArgs{
		Key: key,
	}
	response := GetReply{}
	i := ck.lastLeaderIndex
	for ck.servers[i].Call("KVServer.Get", &request, &response) {
		if response.Err == ErrWrongLeader {
			i = (i + 1) % len(ck.servers)
			if i == ck.lastLeaderIndex {
				DPrintf("[ERROR]: no leader exist?")
				break
			}
			continue
		}
		if response.Err == OK {
			return response.Value
		}
		if response.Err == ErrNoKey {
			break
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	request := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	response := PutAppendReply{}
	i := ck.lastLeaderIndex
	for ck.servers[i].Call("KVServer.PutAppend", &request, &response) {
		if response.Err == ErrWrongLeader {
			i = (i + 1) % len(ck.servers)
			if i == ck.lastLeaderIndex {
				DPrintf("[ERROR]: no leader exist?")
				return
			}
			continue
		}
		if response.Err == OK {
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
