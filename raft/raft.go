// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"log"
	"errors"
	"math/rand"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, _, err := c.Storage.InitialState()
	if err != nil {
		panic(err.Error())
	}

	r := &Raft{
		id: c.ID,
		Term: hardState.Term,
		Vote: hardState.Vote,
		RaftLog: newLog(c.Storage),
		Prs: make(map[uint64]*Progress),
		State: StateFollower,
		votes: make(map[uint64]bool),
		Lead: None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout: c.ElectionTick,
	}

	for _, id := range c.peers {
		if id != r.id {
			r.Prs[id] = &Progress{}
		}
	}

	r.resetElectionElased()

	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	next := r.Prs[to].Next
	prevLogTerm, err := r.RaftLog.Term(next - 1)
	lastIndex := r.RaftLog.LastIndex()
	firstIndex := r.RaftLog.FirstIndex() // needed for snapshot

	if err != nil || next < firstIndex {
	 // TODO: fill later for 2C
		return true
	}

	entriesToSend := r.RaftLog.GetEntries(next, lastIndex + 1)
	entries := make([]*pb.Entry, 0)
	for _, ent := range entriesToSend {
		entries = append(entries, &pb.Entry{
			EntryType: ent.EntryType,
			Term:      ent.Term,
			Index:     ent.Index,
			Data:      ent.Data,
		})
	}

	m := pb.Message {
		MsgType: pb.MessageType_MsgAppend,
		To: to,
		From: r.id,
		Term: r.Term,
		LogTerm: prevLogTerm,
		Index: next - 1,
		Entries: entries,
		Commit: r.RaftLog.committed,
	}

	r.msgs = append(r.msgs, m)
	return true
}

func (r *Raft) sendAppendResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	m := pb.Message {
		MsgType: pb.MessageType_MsgHeartbeat,
		To: to,
		From: r.id,
		Term: r.Term,
		Commit: r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// sendRequestVote sends a requestVote RPC to the given peer.
func (r *Raft) sendRequestVote(to uint64) {
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, err := r.RaftLog.Term(lastLogIndex)
	if err != nil {
		panic(err.Error())
	}

	m := pb.Message {
		MsgType: pb.MessageType_MsgRequestVote,
		To: to,
		From: r.id,
		Term: r.Term,
		LogTerm: lastLogTerm,
		Index: lastLogIndex,
		Commit: r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) sendVoteResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed++
	switch r.State {
	case StateFollower:
		if r.electionElapsed >= r.electionTimeout {
			r.resetElectionElased()
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			if err != nil {
				panic(err.Error())	
			}
		}
	case StateCandidate:
		if r.electionElapsed >= r.electionTimeout {
			r.resetElectionElased()
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			if err != nil {
				panic(err.Error())	
			}
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.resetElectionElased()
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
			if err != nil {
				panic(err.Error())
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = None
	r.heartbeatElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term += 1
	r.Vote = r.id
	r.State = StateCandidate
	r.votes = make(map[uint64]bool)
	r.Lead = None
	r.votes[r.id] = true
	r.heartbeatElapsed = 0
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0
	
	lastIndex := r.RaftLog.LastIndex()
	noop := &pb.Entry{Term: r.Term, Index: lastIndex + 1}
	r.RaftLog.AppendEntries(noop)

	for peer := range r.Prs {
		if peer != r.id {
			r.Prs[peer].Next = lastIndex + 1
			r.Prs[peer].Match = 0
			r.sendAppend(peer)
		}
	}
	r.printRaftState("become leader")
}

// this should only be called: 
// (1) get an AppendEntries/heartbeat RPC from the current leader
// (2) starting an election
// (3) grant a vote to another peer
func (r *Raft) resetElectionElased() {
	r.electionElapsed = -rand.Intn(r.electionTimeout)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		err := r.FollowerStep(m)
		return err
	case StateCandidate:
		err := r.CandidateStep(m)
		return err 
	case StateLeader:
		err := r.LeaderStep(m)
		return err
	}
	return nil
}

func (r *Raft) FollowerStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgBeat: 
	case pb.MessageType_MsgPropose: 
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse: 
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse: 
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

func (r *Raft) CandidateStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse: 
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

func (r *Raft) LeaderStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		for peer := range r.Prs {
			if peer != r.id {
				r.sendHeartbeat(peer)
			}
		}
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartBeatResponse(m)
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

func (r *Raft) startElection() {
	r.becomeCandidate()
	r.printRaftState("start election")
	for peer := range r.Prs {
		if peer != r.id {
			r.sendRequestVote(peer)
		}
	}

	if len(r.Prs) == 0 {
		r.becomeLeader()
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true)
		return
	}
	
	r.becomeFollower(m.Term, m.From)
	r.Vote = m.From
	r.resetElectionElased()

	term, err := r.RaftLog.Term(m.Index) 
	if err != nil || term != m.LogTerm {
		r.sendAppendResponse(m.From, true)
		return
	}

	if len(m.Entries) > 0 {
		lastIndex, index := r.RaftLog.LastIndex(), 0
		for mIndex, mEntry := range m.Entries {
			if mEntry.Index > lastIndex {
				index = mIndex
				break
			}
			term, _ := r.RaftLog.Term(mEntry.Index)
			if term != mEntry.Term {
				r.RaftLog.RemoveEntriesFrom(mEntry.Index)
				break
			}
			index = mIndex
		}
		
		if m.Entries[index].Index > r.RaftLog.LastIndex() {
			r.RaftLog.AppendEntries(m.Entries[index:]...)
		}
	}

	if m.Commit > r.RaftLog.committed {
		lastNewIndex := m.Index
		if len(m.Entries) > 0 {
			lastNewIndex = m.Entries[len(m.Entries) - 1].Index
		}
		r.RaftLog.committed = min(m.Commit, lastNewIndex)
	}

	r.sendAppendResponse(m.From, false)
}


// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	r.becomeFollower(m.Term, m.From)
	r.Vote = m.From
	r.resetElectionElased()

	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.sendAppendResponse(m.From, true)
		return
	}
	
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
	r.sendHeartbeatResponse(m.From, false)
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term < r.Term {
		r.sendVoteResponse(m.From, true)
		return
	}

	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)

	if lastTerm > m.LogTerm || (lastTerm == m.LogTerm && lastIndex > m.Index) {
		r.sendVoteResponse(m.From, true)
		return
	}

	if r.Vote == None || r.Vote == m.From {
		r.Vote = m.From
		r.resetElectionElased()
		r.sendVoteResponse(m.From, false)
	} else {
		r.sendVoteResponse(m.From, true)
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}

	if m.Term < r.Term {
		return
	}

	if !m.Reject {
		r.Prs[m.From].Next = m.Index + 1
		r.Prs[m.From].Match = m.Index
	} else if r.Prs[m.From].Next > 0 {
		r.Prs[m.From].Next -= 1
		r.sendAppend(m.From)
		return
	}
	r.advanceCommit()
}

func (r *Raft) advanceCommit() {
	lastIndex := r.RaftLog.LastIndex()
	for i := r.RaftLog.committed; i <= lastIndex; i += 1 {
		term, _ := r.RaftLog.Term(i)
		if term != r.Term {
			continue
		}
		
		n := 0
		for _, p := range r.Prs {
			if p.Match >= i {
				n += 1
			}
		}
		
		if n > len(r.Prs) / 2 && r.RaftLog.committed < i {
			r.RaftLog.committed = i
		}
	}
}


// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartBeatResponse(m pb.Message) {
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
}

func (r *Raft) handleVoteResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		return
	}

	if m.Term < r.Term {
		return 
	}

	r.votes[m.From] = !m.Reject
	n := 0
	for _, granted := range r.votes {
		if granted {
			n += 1
		}
		if n > len(r.Prs) / 2 {
			r.becomeLeader()
		}
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) printRaftState(prefix string) {
	DPrintf("Raft %v: %v Term:%v, State:%v, Lead:%v, Vote:%v, votes:%v, Prs:%v", r.id, prefix, r.Term, r.State, r.Lead, r.Vote, r.votes, r.Prs)
}

func printMessage(m pb.Message, prefix string) {
	DPrintf("Raft %v: %v m:{Type:%v, To:%v, From:%v, Term:%v, LogTerm:%v, Index:%v, Commit:%v, Reject:%v, Entries:%v}", m.From, prefix, m.MsgType, m.To, m.From, m.Term, m.LogTerm, m.Index, m.Commit, m.Reject, m.Entries)
}