package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.824/labrpc"

	mrand "math/rand"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	seqID    int
	clientID int64
	leaderID int
	mu       sync.Mutex
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
	// You'll have to add code here.
	ck.seqID = 0
	ck.clientID = nrand()
	ck.leaderID = mrand.Intn(len(servers))
	return ck
}

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
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	for {
		ck.mu.Lock()
		ck.seqID += 1
		// try contacting server randomly until leader is found
		args := GetArgs{
			Key:      key,
			SeqID:    ck.seqID,
			ClientID: ck.clientID,
		}
		DPrintf("Client %v: Sending GetArgs %+v to %d", ck.clientID, args, ck.leaderID)
		var reply GetReply
		ck.mu.Unlock()

		ok := ck.servers[ck.leaderID].Call("KVServer.Get", &args, &reply)

		ck.mu.Lock()
		if !ok {
			DPrintf("Sending GetArgs to %d failed", ck.leaderID)
			ck.leaderID = mrand.Intn(len(ck.servers))
			ck.mu.Unlock()
			continue
		}
		DPrintf("Got reply: %+v", reply)
		if reply.Err == "" {
			ck.seqID += 1
			ck.mu.Unlock()
			return reply.Value
		}
		if reply.Err == ErrWrongLeader {
			ck.leaderID = mrand.Intn(len(ck.servers))
		}
		ck.mu.Unlock()
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	for {
		ck.mu.Lock()
		ck.seqID += 1
		// try contacting server randomly until leader is found
		args := PutAppendArgs{
			Key:      key,
			Value:    value,
			SeqID:    ck.seqID,
			Op:       op,
			ClientID: ck.clientID,
		}
		DPrintf("Client %v: Sending PutAppendArgs %+v to %d", ck.clientID, args, ck.leaderID)
		var reply PutAppendReply
		ck.mu.Unlock()

		ok := ck.servers[ck.leaderID].Call("KVServer.PutAppend", &args, &reply)

		ck.mu.Lock()
		if !ok {
			DPrintf("Sending PutAppendArgs to %d failed", ck.leaderID)
			ck.leaderID = mrand.Intn(len(ck.servers))
			ck.mu.Unlock()
			continue
		}
		DPrintf("Got reply: %v", reply)
		if reply.Err == "" {
			ck.seqID += 1
			ck.mu.Unlock()
			return
		}
		if reply.Err == ErrWrongLeader {
			ck.leaderID = mrand.Intn(len(ck.servers))
		}
		ck.mu.Unlock()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
