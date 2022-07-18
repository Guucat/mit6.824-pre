package main

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"6.824/labgob"
	"bytes"
	"math"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	CurrentTerm int     // latest term server has seen (initialized to 0 on first boot, increase monotonically)
	VotedFor    int     // candidateId that received vote in current term (or nil(-1) if none)
	Log         []Entry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	CommitIndex int // index of the highest log entry known to be committed (initialized to 0, increases monotonically)
	LastApplied int // index of the highest log entry applied to state machine (initialized to 0, increases monotonically)

	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of the highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	state                 int       // 1:leader, 2:candidate, 3:follower
	lastResetElectionTime time.Time //wait a period of time to receive tpc call, refresh once is called
	gotVotes              int

	applyCh chan ApplyMsg //used in 2B
	//gotStores             int // to judge the Entry whether a majority of servers have replicated it
	//commitCh chan int
}

type Entry struct {
	Term    int
	Command interface{}
	Index   int //方便获取log的索引
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isLeader bool
	// Your code here (2A).
	term = rf.CurrentTerm
	isLeader = rf.state == 1
	return term, isLeader
}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 选举限制
	//if rf.Log[len(rf.Log)-1].Term > args.LastLogTerm || (rf.Log[len(rf.Log)-1].Term == args.LastLogTerm && len(rf.Log)-1 > args.LastLogIndex) {
	//	reply.Term = rf.CurrentTerm
	//	reply.VoteGranted = false
	//	return
	//
	//}
	// rule2 for all servers
	if args.Term > rf.CurrentTerm {
		DPrintf("Server:%d, state:%d, Term:%d, len(rf.log):%d, commitIndex:%d  投选票时收到更大的Term:%d, 成为Follower", rf.me, rf.state, rf.CurrentTerm, len(rf.Log), rf.CommitIndex, args.Term)
		rf.CurrentTerm = args.Term
		rf.VotedFor = args.CandidateId
		rf.persist()
		// 过期Leader收到了选举
		rf.state = 3

	}
	// rule1 rule2(half) for RequestVote RPC
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		if args.LastLogTerm > rf.Log[len(rf.Log)-1].Term || args.LastLogIndex >= len(rf.Log)-1 && rf.Log[len(rf.Log)-1].Term == args.LastLogTerm {
			rf.VotedFor = args.CandidateId
			rf.lastResetElectionTime = time.Now()
			rf.state = 3
			rf.persist()
			reply.VoteGranted = true
			reply.Term = rf.CurrentTerm
			DPrintf("Server:%d, state:%d, Term:%d, len(rf.log):%d, commitIndex:%d  投出一票给:Server%d, 刷新计时", rf.me, rf.state, rf.CurrentTerm, len(rf.Log), rf.CommitIndex, args.CandidateId)
			return
		}
	}
	// rule2 for RequestVote RPC
	if args.Term == rf.CurrentTerm && rf.VotedFor != -1 && rf.VotedFor != args.CandidateId {
		DPrintf("Server:%d, state:%d, Term:%d, len(rf.log):%d, commitIndex:%d  已投票给:%d, 拒绝VoteFor:%d", rf.me, rf.state, rf.CurrentTerm, len(rf.Log), rf.CommitIndex, rf.VotedFor, args.CandidateId)
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	//rf.CurrentTerm = args.Term
	//rf.VotedFor = args.CandidateId
	//// 收到投票请求刷新超时选举时间
	//rf.lastResetElectionTime = time.Now()
	//
	//rf.state = 3
	//reply.Term = rf.CurrentTerm
	//reply.VoteGranted = true
	//rf.persist()
	return

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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	FollowerIndex int // Leader传来的PreLogIndex
	FollowerTerm  int // Leader传来的PreLogTerm
}

// AppendEntries 处理日志复制的RPC函数
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	entries := make([]Entry, len(args.Entries))
	copy(entries, args.Entries)
	// rule2 for all server
	if (args.Term == rf.CurrentTerm && rf.state == 2) || args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.persist()
		rf.state = 3
		DPrintf("S%d, State%d 收到更大的term%d心跳, 变成Follower", rf.me, rf.state, args.Term)
	}
	// rule1 rule2 for AppendEntriesRPC
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}

	// 此刻两者 Term肯定相等
	// nextIndex
	if len(rf.Log)-1 < args.PrevLogIndex {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		reply.FollowerIndex = len(rf.Log)
		reply.FollowerTerm = -1

		return
	}
	if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// 返回的是 Leader的 nextIndex NextTerm
		reply.Term = rf.CurrentTerm
		reply.Success = false
		reply.FollowerTerm = rf.Log[args.PrevLogIndex].Term
		for t := args.PrevLogIndex; t > 0; t-- {
			if rf.Log[t].Term != rf.Log[args.PrevLogIndex].Term {
				reply.FollowerIndex = t + 1
				return
			}
		}
		reply.FollowerIndex = 0
		return
		//reply.Term = rf.CurrentTerm
		//reply.Success = false
		//reply.FollowerIndex = 1
		//reply.FollowerTerm = 1
		//return
	}
	//rf.Log = rf.Log[0 : args.PrevLogIndex+1]
	//rf.Log = append(rf.Log, args.Entries...)
	//rule3 rule4 for AppendEntriesRPC
	for s := 0; s < len(args.Entries); s++ {
		//超出范围  args.PrevLogIndex+1+s / args.PrevLogIndex+len(args.Entries)
		if args.PrevLogIndex+1+s > len(rf.Log)-1 {
			//rf.Log = rf.Log[0 : args.PrevLogIndex+1]
			rf.Log = append(rf.Log, entries[s:]...)
			break
		}
		// || rf.Log[args.PrevLogIndex+1+s].Index != args.Entries[s].Index
		if rf.Log[args.PrevLogIndex+1+s].Term != args.Entries[s].Term {
			rf.Log = rf.Log[0 : args.PrevLogIndex+1+s]
			rf.Log = append(rf.Log, entries[s:]...)
			break
		}

	}
	rf.persist()
	//rule5 for AppendEntriesRPC
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.Log)-1)))
	}
	if rf.CommitIndex > len(rf.Log)-1 {
		rf.CommitIndex = len(rf.Log) - 1 //debug
	}
	rf.persist()
	for rf.LastApplied < rf.CommitIndex {
		rf.LastApplied++
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.Log[rf.LastApplied].Command,
			CommandIndex: rf.Log[rf.LastApplied].Index,
		}
	}

	// 收到有效心跳刷新超时选举时间
	rf.lastResetElectionTime = time.Now()
	rf.state = 3
	//DPrintf("Server:%d, state:%d, Term:%d, len(rf.log):%d, commitIndex:%d  从Leader:%d收到心跳/日志, 计数器刷新", rf.me, rf.state, rf.CurrentTerm, len(rf.Log), rf.CommitIndex, args.LeaderId)
	reply.Term = rf.CurrentTerm
	reply.Success = true
	rf.persist()
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.state = 3
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	//因为[]log的第一个index为1, 在0的位置加空Entry方便后续管理
	rf.Log = append(rf.Log, Entry{
		Term:    rf.CurrentTerm,
		Command: nil,
		Index:   0,
	})
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.lastResetElectionTime = time.Now()
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("Server:%d, state:%d, Term:%d, len(rf.log):%d, commitIndex:%d	重新开机", rf.me, rf.state, rf.CurrentTerm, len(rf.Log), rf.CommitIndex)
	for range rf.peers {
		rf.nextIndex = append(rf.nextIndex, len(rf.Log))
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats or granting vote to candidate recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		// 成为Leader后停止 超时计时
		if rf.state == 1 {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		nowTime := time.Now()                                                   // 200 100 / 150 150 / 200 150
		electionTimeOut := time.Duration(rand.Intn(150)+150) * time.Millisecond //ms
		time.Sleep(electionTimeOut)
		//如果electionTimeOut期间未收到任何RPC, 则LastResetElectionTime < nowTime, 以此判定超时
		rf.mu.Lock()
		// 超时(包括未收到心跳的超时 和 选举超时) then be a candidate  and begin an election
		if rf.lastResetElectionTime.Before(nowTime) && rf.state != 1 {
			//DPrintf("S%d, 超时选举%dms log length: %d", rf.me, electionTimeOut/1000000, len(rf.Log))
			rf.state = 2
			go rf.startElection()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server:%d, state:%d, Term:%d, len(rf.log):%d, commitIndex:%d	超时开始选举", rf.me, rf.state, rf.CurrentTerm, len(rf.Log), rf.CommitIndex)
	//开始选举前确定自己是候选者身份
	if rf.state != 2 {
		return
	}
	//DPrintf("S%d, 开始新选举!", rf.me)
	// term 自增 1
	rf.CurrentTerm++
	//为自己投票
	rf.VotedFor = rf.me
	rf.persist()
	rf.gotVotes = 1
	// 重置超时选举时间
	rf.lastResetElectionTime = time.Now()
	//Candidate向其他服务器发起选票请求
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// 确定自己是candidate身份
		if rf.state != 2 {
			break
		}
		//  issues RequestVote in parallel

		go func(server int) {
			rf.mu.Lock()
			currentTerm := rf.CurrentTerm
			args := RequestVoteArgs{
				Term:        currentTerm,
				CandidateId: rf.me,
				// !!???
				LastLogIndex: len(rf.Log) - 1,
				LastLogTerm:  rf.Log[len(rf.Log)-1].Term,
			}
			reply := RequestVoteReply{}
			//DPrintf("S%d请求S%d投票", rf.me, server)
			rf.mu.Unlock()

			ok := rf.sendRequestVote(server, &args, &reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.CurrentTerm {
					//DPrintf("Candidate%d 向S%d发送了无效投票请求, 变成follower", rf.me, server)
					rf.CurrentTerm = reply.Term
					rf.persist()
					rf.state = 3
					return
				}
				//保证是新鲜的选票
				if reply.VoteGranted && reply.Term == rf.CurrentTerm && currentTerm == rf.CurrentTerm {
					//DPrintf("S%d得到S%d投票", rf.me, server)
					rf.gotVotes++
					//become a leader, start to initialize
					if rf.gotVotes >= (len(rf.peers)/2)+1 && rf.state == 2 {
						rf.state = 1
						// 此处更新有问题 append函数是末尾追加元素, 不是更新
						//for range rf.peers {
						//	rf.nextIndex = append(rf.nextIndex, len(rf.Log))
						//	rf.matchIndex = append(rf.matchIndex, 0)
						//}
						for s := 0; s < len(rf.peers); s++ {
							rf.nextIndex[s] = len(rf.Log)
							rf.matchIndex[s] = 0
						}
						DPrintf("Server:%d, state:%d, Term:%d, len(rf.log):%d, commitIndex:%d	成为Leader len(nextIndex):%d", rf.me, rf.state, rf.CurrentTerm, len(rf.Log), rf.CommitIndex, len(rf.nextIndex))
						go rf.heartBeatTicker()
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) heartBeatTicker() {
	for rf.killed() == false {
		//首先确定当前server是Leader
		rf.mu.Lock()
		if rf.state != 1 {
			//DPrintf("Leader%d 停止发送心跳", rf.me)
			rf.mu.Unlock()
			//结束 heartBeatTicker
			go rf.ticker()
			break
		}
		rf.persist()
		for i := range rf.peers {
			if rf.state != 1 {
				//结束 HeartBeatTicker rpc
				break
			}
			if i == rf.me {
				continue
			}
			// 并发地方式向Follower发送心跳

			go func(server int) {
				rf.mu.Lock()
				args := AppendEntriesArgs{}
				nextLogIndex := rf.nextIndex[server]
				prevLogIndex := nextLogIndex - 1
				prevLogTerm := rf.Log[prevLogIndex].Term
				//AppendEntries RPC with log entries starting at nextIndex
				entries := make([]Entry, len(rf.Log[nextLogIndex:]))
				copy(entries, rf.Log[nextLogIndex:])
				//entries := rf.Log[nextLogIndex:]
				currentTerm := rf.CurrentTerm
				args = AppendEntriesArgs{
					Term:         currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.CommitIndex,
				}
				// If successful: update nextIndex and matchIndex for follower
				// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
				reply := AppendEntriesReply{}
				//DPrintf("Leader:%d, state:%d, Term:%d, len(rf.log):%d, commitIndex:%d  向Server:%d发送心跳, 携带log长度:%d", rf.me, rf.state, rf.CurrentTerm, len(rf.Log), rf.CommitIndex, server, len(entries))
				rf.mu.Unlock()
				ok := rf.sendAppendEntries(server, &args, &reply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.CurrentTerm {
						//DPrintf("Leader:%d, state:%d, Term:%d, len(rf.log):%d, commitIndex:%d  从Server:%d收到更大的Term:%d, 成为Follower", rf.me, rf.state, rf.CurrentTerm, len(rf.Log), rf.CommitIndex, server, reply.Term)
						rf.CurrentTerm = reply.Term
						rf.state = 3
						//刷新超时选举时间
						rf.persist()
						return
					}
					//                     Leader只添加当前Term的Log
					if reply.Success && rf.CurrentTerm == currentTerm {
						rf.matchIndex[server] = prevLogIndex + len(entries)
						rf.nextIndex[server] = rf.matchIndex[server] + 1
						//验证 Index 是否可以提交了
						//DPrintf("Leader:%d, state:%d, Term:%d, len(rf.log):%d, commitIndex:%d  日志复制成功,Server:%d,NextIndex[Server]:%d, len(entries):%d", rf.me, rf.state, rf.CurrentTerm, len(rf.Log), rf.CommitIndex, server, rf.nextIndex[server], len(entries))
						rf.checkCommit()
						return
					}
					if !reply.Success && rf.CurrentTerm == currentTerm {
						// Term有效期内  log匹配不成共，开始快速回退
						//DPrintf("Leader:%d, state:%d, Term:%d, len(rf.log):%d, commitIndex:%d  与Server:%d快速回退", rf.me, rf.state, rf.CurrentTerm, len(rf.Log), rf.CommitIndex, server)
						rf.quicklyBack(reply, server, prevLogIndex)
					}
				}
			}(i)
		}
		// the tester limits you to 10 heartbeats per second
		rf.mu.Unlock()
		time.Sleep(40 * time.Millisecond)
	}
}

// Leader检查自己的log是否可以提交了
func (rf *Raft) checkCommit() {
	for i := rf.CommitIndex + 1; i < len(rf.Log); i++ {
		commitSum := 1
		for s := range rf.peers {
			if s == rf.me {
				continue
			}
			if rf.matchIndex[s] >= i && rf.Log[i].Term == rf.CurrentTerm {
				commitSum++
				if commitSum >= (len(rf.peers)/2)+1 && rf.CommitIndex+1 < len(rf.Log) {
					rf.CommitIndex++
					rf.applyCh <- ApplyMsg{
						CommandValid: true,
						Command:      rf.Log[rf.CommitIndex].Command,
						CommandIndex: rf.Log[rf.CommitIndex].Index,
					}
					rf.LastApplied = rf.CommitIndex
					//DPrintf("Leader:%d, state:%d, Term:%d, len(rf.log):%d, commitIndex:%d  提交了日志", rf.me, rf.state, rf.CurrentTerm, len(rf.Log), rf.CommitIndex)
					break
				}
			}
		}
	}
}

// log不匹配时以Term为单位回退，以便与Follower的log快速匹配
func (rf *Raft) quicklyBack(reply AppendEntriesReply, server int, preLogIndex int) {
	if reply.FollowerTerm == -1 {
		rf.nextIndex[server] = reply.FollowerIndex
		return
	}
	//如果leader.log找到了Term为FollowerTerm的日志，
	//则下一次从leader.log中FollowerTerm的最后一个log的位置的下一个开始同步日志
	for t := preLogIndex; t > 0; t-- {
		if rf.Log[t].Term == reply.FollowerTerm {
			rf.nextIndex[server] = t + 1
			return
		}
	}
	//如果leader.log找不到Term为FollowerTerm的日志，
	//则下一次从follower.log中FollowerTerm的第一个log的位置开始同步日志。
	rf.nextIndex[server] = reply.FollowerIndex
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise, start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2B).
	//DPrintf("S%d  Start to add log", rf.me)
	if rf.state != 1 {
		isLeader = false
		return index, term, isLeader
	}

	term = rf.CurrentTerm
	//当前log的索引值
	index = len(rf.Log)
	rf.Log = append(rf.Log, Entry{
		term,
		command,
		index,
	})
	rf.persist()
	//DPrintf("S%d committed a log %d ", rf.me, len(rf.Log)-1)
	return index, term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Entry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		//DPrintf("[readPersist] error\n")
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = log

	}
}

// CondInstallSnapshot A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
