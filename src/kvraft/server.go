package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Cmd   string
	Args  []interface{}
	SeqID string
}

type Mutex struct {
	mu sync.Mutex
}

func (m *Mutex) Lock() {
	m.mu.Lock()
	DPrintf("Mu Locked")
}

func (m *Mutex) Unlock() {
	m.mu.Unlock()
	DPrintf("Mu Unlocked")
}

type KVServer struct {
	mu      Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data map[interface{}]interface{} // kv storage data

	seqMap  map[string]interface{} // {clientID}_{sequenceID} -> reply, if exist
	doneMap map[string]chan bool   // notify the handling goroutine that the request is finished
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("Peer[%d]: Get request received, args: %+v", kv.me, args)

	sequenceID := fmt.Sprintf("%d_%d", args.ClientID, args.SeqID)
	kv.mu.Lock()
	if kv.doneMap[sequenceID] == nil {
		kv.doneMap[sequenceID] = make(chan bool)
	}
	op := Op{
		Cmd:   "Get",
		Args:  []interface{}{args.Key},
		SeqID: sequenceID,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	// if requests with a sequence number that has been answered, return immediately
	if existReply, ok := kv.seqMap[sequenceID]; ok {
		DPrintf("Peer[%d]: duplicate request received, reply immediately: %v", kv.me, existReply)
		*reply = existReply.(GetReply)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	ok := <-kv.doneMap[sequenceID]
	kv.mu.Lock()
	if ok {
		*reply = kv.seqMap[sequenceID].(GetReply)
		kv.mu.Unlock()
		return
	}
	reply.Err = ErrRaft
	kv.mu.Unlock()
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// if requests with a sequence number that has been answered, return immediately
	DPrintf("Peer[%d]: PutAppend request received, args: %+v", kv.me, args)
	sequenceID := fmt.Sprintf("%d_%d", args.ClientID, args.SeqID)
	kv.mu.Lock()
	if kv.doneMap[sequenceID] == nil {
		kv.doneMap[sequenceID] = make(chan bool)
	}
	op := Op{
		Cmd:   args.Op, // Put or Append
		Args:  []interface{}{args.Key, args.Value},
		SeqID: sequenceID,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if existReply, ok := kv.seqMap[sequenceID]; ok {
		DPrintf("Peer[%d]: duplicate request received, reply immediately: %v", kv.me, existReply)
		*reply = existReply.(PutAppendReply)
		kv.mu.Unlock()
		return
	}

	ok := <-kv.doneMap[sequenceID]
	kv.mu.Lock()
	if ok {
		*reply = kv.seqMap[sequenceID].(PutAppendReply)
		kv.mu.Unlock()
		return
	}
	reply.Err = ErrRaft
	kv.mu.Unlock()
	return
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.doneMap = make(map[string]chan bool)
	kv.seqMap = make(map[string]interface{})
	kv.data = make(map[interface{}]interface{})
	go kv.applyRoutine()

	return kv
}

func (kv *KVServer) applyRoutine() {
	for {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()
		DPrintf("Peer[%d]: apply channel new message: %+v", kv.me, applyMsg)
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			switch op.Cmd {
			case "Get":
				key := op.Args[0]
				reply := GetReply{
					Err: "",
				}
				// special logic to deal with nil data
				data := kv.data[key]
				if data == nil {
					reply.Value = ""
				} else {
					reply.Value = data.(string)
				}
				DPrintf("Peer[%d]: Get data[%v] = %v", kv.me, key, data)
				kv.seqMap[op.SeqID] = reply
			case "Put":
				key, value := op.Args[0], op.Args[1]
				DPrintf("peer[%d]: set data[%v] = %v", kv.me, key, value)
				kv.data[key] = value
				kv.seqMap[op.SeqID] = PutAppendReply{
					Err: "",
				}
			case "Append":
				key, value := op.Args[0], op.Args[1]
				var newData string
				if oldData, ok := kv.data[key]; ok {
					// has old data
					newData = oldData.(string)
				}
				newData += value.(string)
				DPrintf("peer[%d]: set data[%v] = %v", kv.me, key, newData)
				kv.data[key] = newData
				kv.seqMap[op.SeqID] = PutAppendReply{
					Err: "",
				}
			default:
				DPrintf("Unexpected Command: %v", op.Cmd)
			}
			kv.doneMap[op.SeqID] <- true
		}
		kv.mu.Unlock()
	}
}
