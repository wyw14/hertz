/*
 * Copyright 2023 CloudWeGo Authors
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

package route

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/hertz/internal/bytesconv"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/network"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/cloudwego/hertz/pkg/protocol/http1"
	"github.com/cloudwego/hertz/pkg/route/param"
)

// defaultMaxDrainBodyBytes bounds how many unread request body bytes are
// consumed after a rejection when MaxDrainBytes is left zero.
const defaultMaxDrainBodyBytes int64 = 4 << 20

// ConcurrencyLimitConfig configures HTTP/1.1 route-level admission control.
//
// At most MaxActive requests for the route may own the full HTTP exchange
// (handler chain, streaming request/response body and final flush) at the
// same time. Further requests enter a bounded FIFO queue of size MaxPending;
// they wait at most MaxWaitDuration before being rejected.
type ConcurrencyLimitConfig struct {
	// MaxActive is the maximum number of concurrently admitted requests.
	// A value <= 0 disables the limit (equivalent to ResetConcurrencyLimit).
	MaxActive int

	// MaxPending is the bounded FIFO wait queue. 0 means requests that cannot
	// be admitted immediately are rejected without waiting.
	MaxPending int

	// MaxWaitDuration bounds how long a request waits in the queue. Zero means
	// it waits until admitted, the engine shuts down or the request context is
	// canceled / the connection is observed closed.
	MaxWaitDuration time.Duration

	// RejectStatusCode is sent when the queue is full, the queue body budget is
	// exhausted or the wait deadline expires. Defaults to 503.
	RejectStatusCode int

	// MaxQueuedBodyBytes is the aggregate budget, in bytes, for request bodies
	// declared by requests currently waiting. A request whose reservation
	// would exceed the remaining budget is rejected. Fixed-length bodies
	// reserve Content-Length bytes; chunked bodies, whose length is unknown,
	// reserve MaxRequestBodySize. Zero disables the aggregate budget; queued
	// bodies remain bounded per request by MaxRequestBodySize and are not read
	// until the request is admitted.
	MaxQueuedBodyBytes int64

	// MaxDrainBytes bounds how many bytes of an unread request body are
	// consumed after a rejection/timeout so the keep-alive connection can serve
	// the next request. Larger or chunked bodies and drain errors make the
	// server close the connection instead. Zero defaults to MaxRequestBodySize
	// (falling back to 4MiB when that is unset).
	MaxDrainBytes int64
}

// ConcurrencyStats reports the explicit counters shared by the wait queue and
// the in-flight requests of one generation.
type ConcurrencyStats struct {
	// Generation is the policy generation. Updating a policy starts a new
	// generation; requests admitted earlier keep accounting on the old one.
	Generation uint64

	// Active is the number of permits covering in-flight HTTP exchanges.
	Active int

	// Pending is the number of requests waiting in the FIFO queue.
	Pending int

	// QueuedBodyBytes is the sum of reserved body bytes over Pending requests.
	QueuedBodyBytes int64
}

// ConcurrencyLimit is a live, replaceable handle to a route's admission
// policy. Replacing the policy publishes a new generation; permits issued by
// older generations remain owned by those generations.
type ConcurrencyLimit struct {
	method string
	path   string

	mu  sync.RWMutex
	gen *concurrencyGeneration
}

// Stats returns the counters of the currently published generation.
func (l *ConcurrencyLimit) Stats() ConcurrencyStats {
	l.mu.RLock()
	gen := l.gen
	l.mu.RUnlock()
	if gen == nil {
		return ConcurrencyStats{}
	}
	gen.mu.Lock()
	stats := ConcurrencyStats{
		Generation:      gen.id,
		Active:          gen.active,
		Pending:         len(gen.queued),
		QueuedBodyBytes: gen.queuedBody,
	}
	gen.mu.Unlock()
	return stats
}

func (l *ConcurrencyLimit) current() *concurrencyGeneration {
	l.mu.RLock()
	gen := l.gen
	l.mu.RUnlock()
	return gen
}

func (l *ConcurrencyLimit) publish(cfg ConcurrencyLimitConfig) *concurrencyGeneration {
	l.mu.Lock()
	id := uint64(1)
	if l.gen != nil {
		id = l.gen.id + 1
	}
	gen := &concurrencyGeneration{cfg: cfg, id: id}
	l.gen = gen
	l.mu.Unlock()
	return gen
}

func (l *ConcurrencyLimit) clear() {
	l.mu.Lock()
	l.gen = nil
	l.mu.Unlock()
}

// concurrencyWaiter is one FIFO queue entry.
type concurrencyWaiter struct {
	// ch is closed when an active slot is handed over to this waiter.
	ch      chan struct{}
	reserve int64
}

// concurrencyGeneration is an immutable policy snapshot with its own counters.
// It stays reachable as long as permits or waiters of that generation exist.
type concurrencyGeneration struct {
	cfg ConcurrencyLimitConfig
	id  uint64

	mu sync.Mutex

	// active counts permits covering in-flight exchanges.
	active int
	// queued is the strictly-FIFO wait queue.
	queued []*concurrencyWaiter
	// queuedBody is the sum of reserves over queued.
	queuedBody int64
}

// concurrencyPermit is released exactly once; sync.Once inside guards double
// release from write failures, panics, timeouts, shutdown and pool resets.
type concurrencyPermit struct {
	gen  *concurrencyGeneration
	once sync.Once
}

func (p *concurrencyPermit) Release() {
	p.once.Do(p.release)
}

func (p *concurrencyPermit) release() {
	g := p.gen
	g.mu.Lock()
	if len(g.queued) > 0 {
		// Hand the active slot directly to the head waiter. The active count
		// stays constant: ownership moves from the finished exchange to it.
		w := g.queued[0]
		g.queued = g.queued[1:]
		g.queuedBody -= w.reserve
		g.mu.Unlock()
		close(w.ch)
		return
	}
	g.active--
	g.mu.Unlock()
}

func (g *concurrencyGeneration) rejection(canceled bool) http1.Admission {
	code := g.cfg.RejectStatusCode
	if code == 0 {
		code = consts.StatusServiceUnavailable
	}
	return http1.Admission{
		Rejected:      true,
		StatusCode:    code,
		MaxDrainBytes: g.cfg.MaxDrainBytes,
		Canceled:      canceled,
	}
}

// acquire implements the FIFO admission algorithm:
// active slot -> queue within MaxPending/body budget -> reject.
func (g *concurrencyGeneration) acquire(c context.Context, shutdown <-chan struct{}, req *protocol.Request, maxRequestBodySize int) http1.Admission {
	reserve := bodyReservation(req, maxRequestBodySize)

	g.mu.Lock()
	if g.active < g.cfg.MaxActive {
		g.active++
		g.mu.Unlock()
		return http1.Admission{Permit: &concurrencyPermit{gen: g}}
	}

	if g.cfg.MaxPending <= 0 ||
		len(g.queued) >= g.cfg.MaxPending ||
		(g.cfg.MaxQueuedBodyBytes > 0 && g.queuedBody+reserve > g.cfg.MaxQueuedBodyBytes) {
		g.mu.Unlock()
		return g.rejection(false)
	}

	w := &concurrencyWaiter{ch: make(chan struct{}), reserve: reserve}
	g.queued = append(g.queued, w)
	g.queuedBody += reserve
	g.mu.Unlock()

	return g.wait(c, shutdown, w)
}

// wait blocks until the slot is handed over, the wait deadline passes, the
// request context is canceled (which includes observed peer disconnect), or
// the engine shuts down. It performs no connection reads while waiting.
func (g *concurrencyGeneration) wait(c context.Context, shutdown <-chan struct{}, w *concurrencyWaiter) http1.Admission {
	var timeout <-chan time.Time
	if g.cfg.MaxWaitDuration > 0 {
		timer := time.NewTimer(g.cfg.MaxWaitDuration)
		defer timer.Stop()
		timeout = timer.C
	}

	select {
	case <-w.ch:
		return http1.Admission{Permit: &concurrencyPermit{gen: g}}
	case <-timeout:
	case <-c.Done():
	case <-shutdown:
	}

	// The select may race with a slot hand-over. Re-check the queue under the
	// lock: still queued means we own no slot and leave; missing means the slot
	// was already transferred to us and we must admit the request.
	canceled := c.Err() != nil
	g.mu.Lock()
	for i, q := range g.queued {
		if q == w {
			g.queued = append(g.queued[:i], g.queued[i+1:]...)
			g.queuedBody -= w.reserve
			g.mu.Unlock()
			return g.rejection(canceled)
		}
	}
	g.mu.Unlock()
	return http1.Admission{Permit: &concurrencyPermit{gen: g}}
}

// bodyReservation computes the queue body budget reservation of one request
// from its headers; the request body itself is not read while queued.
func bodyReservation(req *protocol.Request, maxRequestBodySize int) int64 {
	cl := req.Header.ContentLength()
	if cl >= 0 {
		return int64(cl)
	}
	// Chunked bodies have unknown length: pessimistically reserve the
	// configured per-request upper bound so they cannot hide behind an
	// unknown length. Bodies without any length framing need no reservation.
	if cl == -1 && maxRequestBodySize > 0 {
		return int64(maxRequestBodySize)
	}
	return 0
}

// SetConcurrencyLimit installs or replaces the HTTP/1.1 admission policy of
// the route identified by method and path (the path as registered, e.g.
// "/users/:id"). Replacing a policy publishes a new generation; requests
// already admitted or queued stay on the generation they entered.
//
// The route must already be registered. Calling with MaxActive <= 0 removes
// the current policy (see ResetConcurrencyLimit).
func (engine *Engine) SetConcurrencyLimit(method, path string, cfg ConcurrencyLimitConfig) (*ConcurrencyLimit, error) {
	if cfg.MaxActive <= 0 {
		engine.ResetConcurrencyLimit(method, path)
		return nil, nil
	}
	if !engine.routeRegistered(method, path) {
		return nil, fmt.Errorf("concurrency limit: route not registered: %s %s", method, path)
	}
	if cfg.RejectStatusCode == 0 {
		cfg.RejectStatusCode = consts.StatusServiceUnavailable
	}
	if cfg.MaxDrainBytes == 0 {
		if engine.options.MaxRequestBodySize > 0 {
			cfg.MaxDrainBytes = int64(engine.options.MaxRequestBodySize)
		} else {
			cfg.MaxDrainBytes = defaultMaxDrainBodyBytes
		}
	}

	key := concurrencyKey(method, path)
	engine.concurrencyMu.Lock()
	limit := engine.concurrencyLimits[key]
	if limit == nil {
		limit = &ConcurrencyLimit{method: method, path: path}
		engine.concurrencyLimits[key] = limit
	}
	engine.concurrencyMu.Unlock()

	limit.publish(cfg)
	return limit, nil
}

// ResetConcurrencyLimit detaches the current policy of the route. Requests
// already admitted or waiting keep their generation's counters and permits
// until they finish; only newly matched requests become unlimited.
func (engine *Engine) ResetConcurrencyLimit(method, path string) {
	key := concurrencyKey(method, path)
	engine.concurrencyMu.Lock()
	limit := engine.concurrencyLimits[key]
	delete(engine.concurrencyLimits, key)
	engine.concurrencyMu.Unlock()
	if limit != nil {
		limit.clear()
	}
}

// GetConcurrencyLimit returns the live handle of a route's policy, or nil if
// the route currently has no policy.
func (engine *Engine) GetConcurrencyLimit(method, path string) *ConcurrencyLimit {
	engine.concurrencyMu.RLock()
	limit := engine.concurrencyLimits[concurrencyKey(method, path)]
	engine.concurrencyMu.RUnlock()
	return limit
}

func concurrencyKey(method, path string) string {
	return method + "\n" + path
}

func (engine *Engine) routeRegistered(method, path string) bool {
	for _, r := range engine.Routes() {
		if r.Method == method && r.Path == path {
			return true
		}
	}
	return false
}

// matchConcurrencyLimit resolves the request to its currently published policy
// generation, mirroring the path normalization used by Engine.ServeHTTP.
func (engine *Engine) matchConcurrencyLimit(req *protocol.Request) *ConcurrencyLimit {
	method := bytesconv.B2s(req.Header.Method())

	// Keep aligned with the host gate in Engine.ServeHTTP: such requests do
	// not reach a route handler.
	if len(req.Header.Host()) == 0 && req.Header.IsHTTP11() && method != consts.MethodConnect {
		return nil
	}

	// Parse the request target locally so the admission probe does not flip
	// the request's parsedURI flag that ServeHTTP uses for race detection.
	var target protocol.URI
	target.Parse(req.Header.Host(), req.Header.RequestURI())
	rPath := bytesconv.B2s(target.Path())
	unescape := false
	if engine.options.UseRawPath {
		rPath = bytesconv.B2s(target.PathOriginal())
		unescape = engine.options.UnescapePathValues
	}
	if engine.options.RemoveExtraSlash {
		rPath = utils.CleanPath(rPath)
	}
	if len(rPath) == 0 || rPath[0] != '/' {
		return nil
	}

	router := engine.trees.get(method)
	if router == nil {
		return nil
	}
	params := make(param.Params, 0, engine.maxParams)
	value := router.find(rPath, &params, unescape)
	if value.handlers == nil || value.fullPath == "" {
		return nil
	}

	engine.concurrencyMu.RLock()
	limit := engine.concurrencyLimits[concurrencyKey(method, value.fullPath)]
	engine.concurrencyMu.RUnlock()
	return limit
}

// Acquire implements http1.AdmissionController. It is called after the request
// headers are parsed and before the request body is read or the handler runs.
func (engine *Engine) Acquire(c context.Context, _ network.Conn, req *protocol.Request) http1.Admission {
	limit := engine.matchConcurrencyLimit(req)
	if limit == nil {
		return http1.Admission{}
	}
	gen := limit.current()
	if gen == nil {
		return http1.Admission{}
	}
	if atomic.LoadUint32(&engine.status) != statusRunning {
		return gen.rejection(false)
	}
	return gen.acquire(c, engine.shutdownCh, req, engine.options.MaxRequestBodySize)
}

var _ http1.AdmissionController = (*Engine)(nil)
