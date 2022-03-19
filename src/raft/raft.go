package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
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

	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	msgApply  chan ApplyMsg

	reset bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	status      int
	currentTerm int
	votedFor    int
	log         []LogEntry

	// all servers

	commitIndex int
	lastApplied int

	// for leader

	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.status == 3
	rf.mu.Unlock()
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//log.Printf("Request %d (term: %d) vote for %d (term: %d)", rf.me, rf.currentTerm, args.CandidateId, args.Term)

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm == args.Term {
		if rf.votedFor != args.CandidateId {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		} else {
			if len(rf.log) < 1 {
				rf.status = 1
				rf.currentTerm = args.Term
				rf.votedFor = args.CandidateId
				reply.Term = args.Term
				reply.VoteGranted = true
				rf.reset = true
				//log.Printf("%d granted vote for %d", rf.me, args.CandidateId)
				return
			} else {
				if rf.log[len(rf.log)-1].Term > args.LastLogTerm ||
					(rf.log[len(rf.log)-1].Term == args.LastLogTerm && len(rf.log) > args.LastLogIndex) {
					reply.Term = rf.currentTerm
					reply.VoteGranted = false
					return
				} else {
					rf.status = 1
					rf.currentTerm = args.Term
					rf.votedFor = args.CandidateId
					reply.Term = args.Term
					reply.VoteGranted = true
					rf.reset = true
					//log.Printf("%d granted vote for %d", rf.me, args.CandidateId)
					return
				}
			}
		}
	}

	if rf.currentTerm < args.Term {
		if len(rf.log) < 1 {
			rf.status = 1
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.Term = args.Term
			reply.VoteGranted = true
			rf.reset = true
			//log.Printf("%d granted vote for %d", rf.me, args.CandidateId)
			return
		} else {
			if rf.log[len(rf.log)-1].Term > args.LastLogTerm ||
				(rf.log[len(rf.log)-1].Term == args.LastLogTerm && len(rf.log) > args.LastLogIndex) {
				reply.Term = rf.currentTerm
				reply.VoteGranted = false
				return
			} else {
				rf.status = 1
				rf.currentTerm = args.Term
				rf.votedFor = args.CandidateId
				reply.Term = args.Term
				reply.VoteGranted = true
				rf.reset = true
				//log.Printf("%d granted vote for %d", rf.me, args.CandidateId)
				return
			}
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if len(args.Entries) < 1 {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		//log.Printf("%d (term: %d) received heartbeat from %d (term %d)", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		if rf.currentTerm <= args.Term {
			rf.status = 1
			rf.currentTerm = args.Term
			reply.Success = true
			reply.Term = args.Term
			rf.votedFor = args.LeaderId
			rf.reset = true
			if rf.commitIndex < args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
			}
			return
		}

		if rf.currentTerm > args.Term {
			reply.Success = false
			reply.Term = rf.currentTerm
		}
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		log.Printf("Received append request %d", rf.me)

		if rf.currentTerm > args.Term {
			reply.Success = false
			reply.Term = rf.currentTerm
		} else {
			rf.status = 1
			rf.currentTerm = args.Term
			reply.Term = args.Term
			rf.votedFor = args.LeaderId
			rf.reset = true

			if len(rf.log) < args.PrevLogIndex {
				reply.Success = false
				return
			}

			if args.PrevLogIndex == -1 {
				rf.log = args.Entries
				reply.Success = true
				log.Printf("Received a log append successfully %v at %d", rf.log, rf.me)
				// leaderCommit
				return
			}

			if rf.log[args.PrevLogIndex].Term != args.Term {
				reply.Success = false
				return
			} else {
				reply.Success = true
				rf.commitIndex = args.LeaderCommit
				rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)

				//leader commit
				return
			}
		}
	}
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
	ch := make(chan bool, 1)
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		ch <- ok
	}()
	time.Sleep(heartbeat)
	var ok bool
	select {
	case ok = <-ch:
		return ok
	default:
		return false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ch := make(chan bool, 1)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		ch <- ok
	}()
	time.Sleep(heartbeat)
	var ok bool
	select {
	case ok = <-ch:
		return ok
	default:
		return false
	}
}

func findMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) hdAppendEntriesCall(command interface{}) {

	check := make([]bool, len(rf.peers))

	for i := 0; i < len(rf.peers); i++ {
		check[i] = false
	}

	rf.mu.Lock()

	if rf.status != 3 {
		rf.mu.Unlock()
		return
	}

	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})

	log.Printf("Start log %d", rf.me)

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && !check[i] {
			rf.mu.Lock()
			if rf.status != 3 {
				rf.mu.Unlock()
				return
			}

			var prevIndex, prevTerm int
			var entries []LogEntry

			if len(rf.log) < 2 {
				prevIndex = -1
				prevTerm = -1
				entries = rf.log
			} else {
				prevIndex = rf.nextIndex[i] - 1
				prevTerm = rf.log[rf.nextIndex[i]-1].Term
				entries = rf.log[rf.nextIndex[i]-1:]
			}

			args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: prevIndex, PrevLogTerm: prevTerm, Entries: entries, LeaderCommit: rf.commitIndex}
			reply := &AppendEntriesReply{}
			rf.mu.Unlock()

			log.Printf("Sending append entries to %d from %d", i, rf.me)

			ok := rf.sendAppendEntries(i, args, reply)

			rf.mu.Lock()
			if ok && reply.Success {
				rf.nextIndex[i] = len(rf.log) + 1
				rf.matchIndex[i] = len(rf.log)
				check[i] = true

				var cnt, min int

				min = 100000

				for i := 0; i < len(rf.peers); i++ {
					if rf.matchIndex[i] > rf.commitIndex && rf.log[rf.matchIndex[i]].Term == rf.currentTerm {
						cnt++
						min = findMin(min, rf.matchIndex[i])
					}
				}

				if cnt > len(rf.peers)/2+1 {
					rf.commitIndex = min
				}

				log.Printf("%d have new commit index: %d", rf.me, rf.commitIndex)
			}

			if ok && reply.Success {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.status = 1
					rf.mu.Unlock()
					return
				} else {
					rf.nextIndex[i]--
				}
			}

			rf.mu.Unlock()
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.commitIndex
	term := rf.currentTerm
	isLeader := rf.status == 3

	go rf.hdAppendEntriesCall(command)

	return index, term, isLeader
}

//
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

func sendHeartbeat(rf *Raft, server int, check []bool, muCheck *sync.Mutex) {
	//log.Printf("Start send hearbeat from %d to %d", rf.me, server)
	for {
		rf.mu.Lock()

		muCheck.Lock()
		if rf.status == 1 || !check[server] {
			//log.Printf("Fail to send hearbeat from %d to %d", rf.me, server)
			muCheck.Unlock()
			rf.mu.Unlock()
			return
		}
		muCheck.Unlock()

		args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, Entries: []LogEntry{}, LeaderCommit: rf.commitIndex}
		reply := &AppendEntriesReply{}

		rf.mu.Unlock()
		//log.Printf("Sent hearbeat from %d to %d", rf.me, server)
		ok := rf.sendAppendEntries(server, args, reply)

		rf.mu.Lock()
		if ok && reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.status = 1
		}

		rf.mu.Unlock()

		time.Sleep(heartbeat)
	}
}

func randRange(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max-min) + min
}

func (rf *Raft) newElection(ch chan struct{}) {
	//log.Printf("New election created by %d", rf.me)
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	check := make([]bool, len(rf.peers))
	muCheck := &sync.Mutex{}
	for i := 0; i < len(rf.peers); i++ {
		check[i] = false
	}
	rf.mu.Unlock()

	cnt := 1

	for {
		time.Sleep(heartbeat / 2)
		select {
		case <-ch:
			//log.Printf("Time out vote for %d", rf.me)
			muCheck.Lock()
			for i := 0; i < len(rf.peers); i++ {
				check[i] = false
			}
			muCheck.Unlock()
			return
		default:
			rf.mu.Lock()

			if rf.status == 1 || rf.status == 3 {
				rf.mu.Unlock()
				return
			}

			//log.Printf("Sending vote request from %d", rf.me)
			rf.mu.Unlock()

			for i := 0; i < len(rf.peers); i++ {
				muCheck.Lock()

				if i != rf.me && !check[i] {
					muCheck.Unlock()
					rf.mu.Lock()
					args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: 0, LastLogTerm: 0}
					reply := &RequestVoteReply{}
					rf.mu.Unlock()
					//log.Printf("%d start to send request vote to %d", rf.me, i)
					ok := rf.sendRequestVote(i, args, reply)
					//log.Printf("%d sent request vote to %d", rf.me, i)
					if ok && reply.VoteGranted {
						cnt++
						muCheck.Lock()
						check[i] = true
						muCheck.Unlock()
						go sendHeartbeat(rf, i, check, muCheck)
					}

					rf.mu.Lock()

					if ok && !reply.VoteGranted && reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.status = 1
					}

					rf.mu.Unlock()

					if cnt >= len(rf.peers)/2+1 {
						//log.Printf("New leader %d", rf.me)
						rf.mu.Lock()
						rf.status = 3
						for i := 0; i < len(rf.peers); i++ {
							muCheck.Lock()
							if !check[i] && i != rf.me {
								check[i] = true
								go sendHeartbeat(rf, i, check, muCheck)
							}
							muCheck.Unlock()
						}
						rf.mu.Unlock()
						return
					}
				} else {
					muCheck.Unlock()
				}
			}
		}
	}
}

var heartbeat = time.Millisecond * 100

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	//log.Printf("Trigger ticker by %d", rf.me)
	var ch chan struct{}
	for !rf.killed() {

		time.Sleep(time.Duration(randRange(2, 10) * int(heartbeat)))
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		//log.Printf("Time out %d status %d", rf.me, rf.status)
		if (rf.status == 1 && !rf.reset) || rf.status == 2 {
			rf.status = 2
			//log.Printf("Starting new election %d status %d", rf.me, rf.status)
			if ch != nil {
				//log.Printf("Channel start send to kill %d", rf.me)
				ch <- struct{}{}
			}
			ch = make(chan struct{}, 1)
			go rf.newElection(ch)
		}
		rf.reset = false
		rf.mu.Unlock()
	}
}

//
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

func check(rf *Raft) {
	for {
		time.Sleep(time.Second / 2)
		rf.mu.Lock()
		log.Printf("Unlocked mutex %d", rf.me)
		rf.mu.Unlock()
	}
}

func initLeader(rf *Raft) {
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log) + 1
	}

	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.status = 1
	rf.reset = false
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.msgApply = applyCh
	initLeader(rf)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	//go check(rf)

	return rf
}
