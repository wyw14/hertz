/*
 * Copyright 2026 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package idempotency

import (
	"bytes"
	"context"
	"sync"
	"time"
)

// entryState is the lifecycle state of a single (namespace, idempotency-key)
// coordination entry.
type entryState uint8

const (
	// stateInflight means the leader handler is still executing. Followers
	// must wait for the definite terminal state.
	stateInflight entryState = iota
	// stateCompleted means the leader produced a complete, buffered,
	// replayable response stored on the entry.
	stateCompleted
	// stateFailed means the leader explicitly failed (handler error return
	// path, panic or leader-disconnect Fail policy). Nothing is cached and
	// the entry is removed from the map, but waiting followers are still
	// woken with an explicit terminal.
	stateFailed
	// stateNotReplayable means the leader produced a response that cannot be
	// safely replayed (streaming/hijacked/over response budget/store budget).
	// Nothing is cached and the entry is removed; followers receive an
	// explicit terminal instead of waiting forever or re-executing.
	stateNotReplayable

	// stateWaiterCanceled is never stored on an entry. It is returned to a
	// follower whose own context was canceled while waiting.
	stateWaiterCanceled entryState = 255
)

// cachedResponse is the immutable, independently owned snapshot of a
// completed buffered response. Every byte slice/string it holds is copied at
// capture time, so recycling the leader's pooled RequestContext can never
// corrupt what is later replayed to followers.
type cachedResponse struct {
	status  int
	headers [][2]string
	body    []byte
	// size is body length plus the stored header bytes; it counts towards
	// the store-wide total byte budget.
	size int
}

// waiter tracks one follower blocked on an inflight entry.
type waiter struct {
	canceled bool
}

// entry coordinates one leader and its followers. An entry object is never
// reused across generations: a fresh object is created whenever a key becomes
// leader again, which structurally prevents a late completion from an older
// generation from overwriting a newer key holder.
type entry struct {
	cond *sync.Cond

	gen         uint64
	fingerprint []byte
	state       entryState

	waiters map[*waiter]struct{}

	result      *cachedResponse
	completedAt time.Time
}

// leaderToken captures the exact (map key, entry, generation) a leader owns.
// Every terminal publication proves it still owns that generation before
// touching the entry.
type leaderToken struct {
	composite string
	e         *entry
	gen       uint64
}

type decisionKind uint8

const (
	decLeader decisionKind = iota
	decFollower
	decReplay
	decConflict
	decRejected
)

// decision is the outcome of store.begin.
type decision struct {
	kind   decisionKind
	token  leaderToken
	e      *entry
	w      *waiter
	result *cachedResponse
	// status is the HTTP status to return for decRejected.
	status int
}

// store is the in-memory coordination table. A single mutex protects the map
// and all entry states; per-entry sync.Cond instances share that mutex, which
// makes wait/signal/broadcast and map bookkeeping impossible to interleave
// incorrectly.
type store struct {
	mu sync.Mutex

	entries    map[string]*entry
	totalBytes int
	gen        uint64

	lim *limits
	now func() time.Time
}

// limits groups the hard ceilings enforced by the store.
type limits struct {
	maxEntries           int
	maxWaiters           int
	maxBytes             int
	maxResponseBodyBytes int
	completedTTL         time.Duration
}

func newStore(lim *limits, now func() time.Time) *store {
	if now == nil {
		now = time.Now
	}
	return &store{
		entries: make(map[string]*entry),
		lim:     lim,
		now:     now,
	}
}

// begin classifies an arriving request:
//   - a fresh key with capacity available becomes the leader (runs handler);
//   - an inflight key with the same fingerprint joins as a follower (within
//     the per-entry waiter cap) or is rejected (different fingerprint / too
//     many waiters);
//   - a completed key within TTL is an immediate replay;
//   - a completed key past TTL is purged and the request becomes a new
//     generation leader;
//   - when the entry table is full, expired/oldest completed entries are
//     evicted; if only in-flight entries remain, the request is rejected
//     instead of evicting an in-flight entry (which would allow a duplicate
//     concurrent handler execution).
func (s *store) begin(composite string, fp []byte) decision {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()

	if e, ok := s.entries[composite]; ok {
		switch e.state {
		case stateCompleted:
			if now.Sub(e.completedAt) >= s.lim.completedTTL {
				s.removeCompletedLocked(composite, e)
				// Fall through and become a new-generation leader.
			} else if !bytes.Equal(e.fingerprint, fp) {
				return decision{kind: decConflict}
			} else {
				return decision{kind: decReplay, result: e.result}
			}
		case stateInflight:
			if !bytes.Equal(e.fingerprint, fp) {
				return decision{kind: decConflict}
			}
			if len(e.waiters) >= s.lim.maxWaiters {
				return decision{kind: decRejected, status: statusTooManyWaiters}
			}
			w := &waiter{}
			e.waiters[w] = struct{}{}
			return decision{kind: decFollower, e: e, w: w}
		default:
			// Terminal entries are removed atomically with their state
			// transition; defensively drop anything unexpected.
			s.deleteIfSameLocked(composite, e)
		}
	}

	s.purgeExpiredLocked(now)
	for len(s.entries) >= s.lim.maxEntries {
		if !s.evictOldestCompletedLocked(nil) {
			return decision{kind: decRejected, status: statusStoreFull}
		}
	}

	s.gen++
	e := &entry{
		cond:        sync.NewCond(&s.mu),
		gen:         s.gen,
		fingerprint: fp,
		state:       stateInflight,
		waiters:     make(map[*waiter]struct{}),
	}
	s.entries[composite] = e
	return decision{
		kind:  decLeader,
		token: leaderToken{composite: composite, e: e, gen: e.gen},
	}
}

// await blocks the follower until the entry reaches a definite terminal state
// or the follower's own context is canceled. A follower cancellation never
// touches the leader: it only removes this one waiter.
func (s *store) await(c context.Context, e *entry, w *waiter) entryState {
	stop := make(chan struct{})
	defer close(stop)

	if c.Done() != nil {
		go func() {
			select {
			case <-c.Done():
				s.cancelWaiter(e, w)
			case <-stop:
			}
		}()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if e.state != stateInflight {
			return e.state
		}
		if w.canceled {
			return stateWaiterCanceled
		}
		e.cond.Wait()
	}
}

// cancelWaiter detaches a single follower whose request context ended. The
// leader and every other waiter are unaffected.
func (s *store) cancelWaiter(e *entry, w *waiter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.state != stateInflight {
		return
	}
	if _, ok := e.waiters[w]; !ok {
		return
	}
	delete(e.waiters, w)
	w.canceled = true
	// Wakes the canceled waiter so it can observe its own flag; other
	// waiters simply re-check the state and keep waiting.
	e.cond.Broadcast()
}

// completeSuccess publishes a buffered response. Publication is rejected if
// the caller no longer owns the current generation (e.g. a LeaderFail
// disconnect already terminated it), or if it cannot fit the store-wide byte
// budget after every possible completed-entry eviction; in the latter case
// the leader terminates as explicitly not-replayable rather than caching a
// partial/over-budget result.
func (s *store) completeSuccess(t leaderToken, res *cachedResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[t.composite]
	if !ok || e != t.e || e.gen != t.gen || e.state != stateInflight {
		// Stale generation: discard the late completion entirely.
		return
	}
	if res.size > s.lim.maxResponseBodyBytes {
		s.terminateLocked(t.composite, e, stateNotReplayable)
		return
	}

	now := s.now()
	s.purgeExpiredLocked(now)
	for s.totalBytes+res.size > s.lim.maxBytes {
		if !s.evictOldestCompletedLocked(e) {
			// Cannot free enough room; never pretend an over-budget
			// response is replayable.
			s.terminateLocked(t.composite, e, stateNotReplayable)
			return
		}
	}

	e.result = res
	e.state = stateCompleted
	e.completedAt = now
	e.waiters = nil
	s.totalBytes += res.size
	e.cond.Broadcast()
}

// completeFailure publishes an explicit non-cached terminal (leader panic,
// handler failure or leader-disconnect Fail policy). The entry is removed so
// that a later retry with the same key may execute again, while already
// waiting followers are woken with the definite failure state.
func (s *store) completeFailure(t leaderToken) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.terminateIfOwnerLocked(t, stateFailed)
}

// completeTerminal publishes an explicit non-cached terminal state (for
// example not-replayable). The entry is removed after waking followers.
func (s *store) completeTerminal(t leaderToken, st entryState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.terminateIfOwnerLocked(t, st)
}

// leaderDisconnected applies the LeaderFail policy: the current generation
// becomes an explicit failure immediately. The leader goroutine cannot be
// killed in Go; when its handler eventually returns, completeSuccess /
// completeFailure finds it no longer owns the generation and is discarded.
func (s *store) leaderDisconnected(t leaderToken) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.terminateIfOwnerLocked(t, stateFailed)
}

// terminateIfOwnerLocked transitions the entry only when the token still
// identifies the current generation.
func (s *store) terminateIfOwnerLocked(t leaderToken, st entryState) {
	e, ok := s.entries[t.composite]
	if !ok || e != t.e || e.gen != t.gen || e.state != stateInflight {
		return
	}
	s.terminateLocked(t.composite, e, st)
}

// terminateLocked marks the terminal state, removes the entry from the map
// (terminal states are never cached/served to future arrivals) and wakes all
// current followers holding the pointer.
func (s *store) terminateLocked(composite string, e *entry, st entryState) {
	if cur, ok := s.entries[composite]; ok && cur == e {
		delete(s.entries, composite)
	}
	e.state = st
	e.waiters = nil
	e.cond.Broadcast()
}

func (s *store) deleteIfSameLocked(composite string, e *entry) {
	cur, ok := s.entries[composite]
	if !ok || cur != e {
		return
	}
	if e.state == stateCompleted && e.result != nil {
		s.totalBytes -= e.result.size
		if s.totalBytes < 0 {
			s.totalBytes = 0
		}
	}
	delete(s.entries, composite)
}

func (s *store) removeCompletedLocked(composite string, e *entry) {
	if e.state != stateCompleted {
		return
	}
	s.deleteIfSameLocked(composite, e)
}

// purgeExpiredLocked removes every completed entry whose TTL, counted from
// its definite completion time, has elapsed.
func (s *store) purgeExpiredLocked(now time.Time) {
	for k, e := range s.entries {
		if e.state == stateCompleted && now.Sub(e.completedAt) >= s.lim.completedTTL {
			s.deleteIfSameLocked(k, e)
		}
	}
}

// evictOldestCompletedLocked removes the completed entry with the earliest
// completion time. Entries in any non-completed state are never evicted.
// keep (when non-nil) is never evicted (used to protect the currently
// completing entry). It reports whether something was evicted.
func (s *store) evictOldestCompletedLocked(keep *entry) bool {
	var oldestKey string
	var oldest *entry
	for k, e := range s.entries {
		if e.state != stateCompleted || e == keep {
			continue
		}
		if oldest == nil || e.completedAt.Before(oldest.completedAt) {
			oldest, oldestKey = e, k
		}
	}
	if oldest == nil {
		return false
	}
	s.deleteIfSameLocked(oldestKey, oldest)
	return true
}

// stats is a point-in-time snapshot used for observability and tests.
type stats struct {
	Entries    int
	TotalBytes int
}

func (s *store) stats() stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return stats{Entries: len(s.entries), TotalBytes: s.totalBytes}
}

// waitersCount reports how many followers are currently registered on key.
func (s *store) waitersCount(composite string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[composite]; ok {
		return len(e.waiters)
	}
	return 0
}

// stateOf returns the current state of key, or -1 if absent. Test helper.
func (s *store) stateOf(composite string) entryState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[composite]; ok {
		return e.state
	}
	return entryState(254)
}
