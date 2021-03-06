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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	first uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hardState, _, _ := storage.InitialState()
	first, _ := storage.FirstIndex()
	last, _ := storage.LastIndex()
	entries := make([]pb.Entry, 0)
	if first <= last {
		entries, _ = storage.Entries(first, last + 1)
	}

	return &RaftLog {
		storage: storage,
		committed: hardState.Commit, 
		applied: first - 1,
		stabled: last,
		entries: entries,
		first: first,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 && l.stabled >= l.first - 1 {
		return l.entries[l.stabled - l.first + 1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 && l.applied >= l.first - 1 {
		return l.entries[l.applied - l.first + 1 : min(l.committed - l.first + 1, uint64(len(l.entries)))]
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return l.stabled
	}
	return l.entries[len(l.entries) - 1].Index
}

func (l *RaftLog) FirstIndex() uint64 {
	return l.first
}


// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 && i >= l.first {
		if i > l.LastIndex() {
			return 0, ErrUnavailable
		}
		return l.entries[i - l.first].Term, nil
	}
	return l.storage.Term(i)
}

// the remaining array will not have no
func (l *RaftLog) RemoveEntriesFrom(lo uint64) {
	l.stabled = min(l.stabled, lo - 1)
	if lo - l.first >= uint64(len(l.entries)) {
		return
	}
	l.entries = l.entries[:lo - l.first]
}

func (l *RaftLog) AppendEntries(entries ...*pb.Entry) {
	for _, e := range entries {
		l.entries = append(l.entries, pb.Entry{
			EntryType: e.EntryType,
			Term: e.Term,
			Index: e.Index,
			Data: e.Data,
		})
	}
}

func (l *RaftLog) GetEntries(lo uint64, hi uint64) []pb.Entry {
	if lo >= l.first && hi - l.first <= uint64(len(l.entries)) {
		return l.entries[lo - l.first : hi - l.first]
	}
	entries, _ := l.storage.Entries(lo, hi)
	return entries
}