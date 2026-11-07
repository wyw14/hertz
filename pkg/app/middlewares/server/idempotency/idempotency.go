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

// Package idempotency provides an explicitly opt-in server middleware that
// collapses concurrent duplicate requests onto a single handler execution
// and replays the leader's complete buffered response to the waiting
// followers.
//
// # Scope and identity
//
// The middleware must be attached deliberately to the routes that support
// it; it is never installed globally. A request participates only when it
// carries the idempotency key header AND the application-provided trusted
// NamespaceFunc resolves a non-empty namespace (typically derived from an
// authenticated tenant/identity). There is deliberately no default global
// namespace: two tenants reusing the same idempotency key can never share an
// entry.
//
// Requests without a key, or without a resolvable namespace, pass through to
// the handler with no deduplication.
//
// # Fingerprint and conflicts
//
// The coordination key is (namespace, idempotency-key). In addition, every
// participating request is fingerprinted over its method, canonical target
// (normalized path + order-independent query), the caller-designated
// business headers and a digest of the complete request body. A follower
// presenting the same key with a different fingerprint receives 409 Conflict
// and its handler is never executed. Streaming request bodies that cannot be
// fully materialized within the configured byte budget are rejected before
// entering the handler.
//
// # Leader / followers and terminal states
//
// The first request for a key is the leader and runs the handler exactly
// once. Concurrent duplicates wait for a definite result. A follower's
// cancellation never cancels the leader. Leader panic, explicit failure, or
// the LeaderFail disconnect policy produce an explicit non-cached terminal:
// waiting followers receive a definite response, the handler is not run a
// second time, and no entry can ever block waiters forever.
//
// Only complete, buffered responses within the response budget are cached
// after the handler finishes: status code, replay-safe headers and the full
// body are copied. Streaming/hijacked responses, hop-by-hop headers,
// Content-Length and caller-designated private headers are never replayed.
// An over-budget response is delivered to the leader's own client normally
// but is not cached; followers get an explicit 503, never a half body or a
// false replay claim.
//
// # Limits and TTL
//
// The store enforces hard ceilings on entry count, cached total bytes and
// per-entry waiters. Expired or oldest completed entries may be evicted; an
// in-flight entry is never evicted - when no slot is available the new
// request is rejected instead of allowing duplicate concurrent execution.
// Completed entries live for a bounded TTL counted from the definite
// completion time. Entry generations prevent a stale completion from an old
// leader from overwriting a newer holder of the same key.
//
// IMPORTANT: replaying identical response bytes does not roll back any
// business side effect performed by the leader. The middleware only
// deduplicates execution and response delivery; side-effect compensation is
// the caller's responsibility.
package idempotency

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

const (
	defaultKeyHeader            = "Idempotency-Key"
	defaultReplayHeader         = "Idempotency-Replayed"
	defaultMaxEntries           = 1024
	defaultMaxWaiters           = 128
	defaultMaxRequestBodyBytes  = 1 << 20  // 1 MiB
	defaultMaxResponseBodyBytes = 1 << 20  // 1 MiB
	defaultMaxBytes             = 16 << 20 // 16 MiB
	defaultCompletedTTL         = 5 * time.Minute
)

// HTTP statuses used for explicit, non-replayable terminal responses.
const (
	statusFingerprintTooLarge = consts.StatusRequestEntityTooLarge
	statusFingerprintRead     = consts.StatusBadRequest
	statusFingerprintMismatch = consts.StatusConflict
	statusTooManyWaiters      = consts.StatusTooManyRequests
	statusStoreFull           = consts.StatusServiceUnavailable
	statusNotReplayable       = consts.StatusServiceUnavailable
	statusLeaderFailed        = consts.StatusInternalServerError
	statusWaiterCanceled      = consts.StatusRequestTimeout
)

// NamespaceFunc derives the trusted namespace for a request from an
// authenticated identity established earlier in the handler chain (for
// example a tenant ID). Returning ok=false or an empty namespace opts the
// request out of idempotency; it must never fall back to a global namespace.
type NamespaceFunc func(c context.Context, ctx *app.RequestContext) (namespace string, ok bool)

// LeaderDisconnectPolicy defines what happens to coordination state when a
// leader's client connection ends before the leader completes.
type LeaderDisconnectPolicy int

const (
	// LeaderContinue (default) lets the leader run the handler to completion
	// and publish its result; followers keep waiting for it.
	LeaderContinue LeaderDisconnectPolicy = iota
	// LeaderFail publishes an explicit failure terminal to followers as soon
	// as the leader's context ends. The leader goroutine itself cannot be
	// interrupted; its late completion is discarded by the generation check.
	LeaderFail
)

// PanicHandler observes a recovered leader panic. It may customize the
// leader response (status/body); the coordination entry is always finalized
// as a non-cached failure regardless.
type PanicHandler func(c context.Context, ctx *app.RequestContext, recovered interface{})

type options struct {
	namespace NamespaceFunc

	keyHeader    string
	replayHeader string

	// businessHeaderNames are the request header names (lower-cased, sorted)
	// included in the fingerprint.
	businessHeaderNames []string

	// privateResponseHeaders are lower-cased response header names that must
	// never be replayed to followers, in addition to the always-excluded
	// hop-by-hop/framing headers.
	privateResponseHeaders map[string]struct{}

	maxEntries           int
	maxWaiters           int
	maxRequestBodyBytes  int
	maxResponseBodyBytes int
	maxBytes             int
	completedTTL         time.Duration

	leaderDisconnect LeaderDisconnectPolicy
	panicHandler     PanicHandler
	now              func() time.Time
}

// Option customizes the idempotency middleware.
type Option func(*options)

// WithNamespace is mandatory: it supplies the trusted callback that maps a
// request to its tenant/identity namespace.
func WithNamespace(f NamespaceFunc) Option {
	return func(o *options) { o.namespace = f }
}

// WithKeyHeader overrides the default "Idempotency-Key" request header name.
func WithKeyHeader(name string) Option {
	return func(o *options) { o.keyHeader = name }
}

// WithReplayHeader overrides the default "Idempotency-Replayed" response
// header name. The leader's buffered response carries "false"; replayed
// follower responses carry "true".
func WithReplayHeader(name string) Option {
	return func(o *options) { o.replayHeader = name }
}

// WithBusinessHeaders designates the request headers (case-insensitive) that
// participate in the request fingerprint.
func WithBusinessHeaders(names ...string) Option {
	return func(o *options) {
		for _, n := range names {
			if n = strings.TrimSpace(strings.ToLower(n)); n != "" {
				o.businessHeaderNames = append(o.businessHeaderNames, n)
			}
		}
	}
}

// WithPrivateResponseHeaders designates response headers (case-insensitive)
// that are per-client/private and must never be replayed. Hop-by-hop and
// framing headers are always excluded.
func WithPrivateResponseHeaders(names ...string) Option {
	return func(o *options) {
		if o.privateResponseHeaders == nil {
			o.privateResponseHeaders = make(map[string]struct{})
		}
		for _, n := range names {
			if n = strings.TrimSpace(strings.ToLower(n)); n != "" {
				o.privateResponseHeaders[n] = struct{}{}
			}
		}
	}
}

// WithMaxEntries limits the number of coordination entries (in-flight and
// completed) held at once.
func WithMaxEntries(n int) Option {
	return func(o *options) { o.maxEntries = n }
}

// WithMaxWaiters limits followers allowed to wait on a single in-flight key.
func WithMaxWaiters(n int) Option {
	return func(o *options) { o.maxWaiters = n }
}

// WithMaxRequestBodyBytes bounds how much of a streaming request body is
// buffered to compute the fingerprint. Larger streaming requests are
// rejected before the handler runs.
func WithMaxRequestBodyBytes(n int) Option {
	return func(o *options) { o.maxRequestBodyBytes = n }
}

// WithMaxResponseBodyBytes bounds the response body size that may be cached
// for replay. Larger buffered responses are delivered to the leader but not
// cached.
func WithMaxResponseBodyBytes(n int) Option {
	return func(o *options) { o.maxResponseBodyBytes = n }
}

// WithMaxBytes bounds the total bytes cached across all completed entries.
func WithMaxBytes(n int) Option {
	return func(o *options) { o.maxBytes = n }
}

// WithCompletedTTL sets how long a completed entry remains replayable,
// counted from the definite completion time.
func WithCompletedTTL(d time.Duration) Option {
	return func(o *options) { o.completedTTL = d }
}

// WithLeaderDisconnectPolicy selects the behavior when a leader client
// disconnects before completion (default LeaderContinue).
func WithLeaderDisconnectPolicy(p LeaderDisconnectPolicy) Option {
	return func(o *options) { o.leaderDisconnect = p }
}

// WithPanicHandler installs an observer/customizer for recovered leader
// panics.
func WithPanicHandler(f PanicHandler) Option {
	return func(o *options) { o.panicHandler = f }
}

// withNow injects the clock used for TTL decisions; intended for testing.
func withNow(f func() time.Time) Option {
	return func(o *options) { o.now = f }
}

// Idempotency is the constructed, explicitly enabled middleware. One instance
// owns one in-memory store and is safe for concurrent use; attach it only to
// the routes that opt into idempotency.
type Idempotency struct {
	cfg   *options
	store *store
}

// New builds the middleware. WithNamespace is required. A single
// Idempotency instance shares its coordination store across all routes it is
// attached to; namespaces still isolate tenants within that store.
func New(opts ...Option) (*Idempotency, error) {
	cfg := &options{
		keyHeader:              defaultKeyHeader,
		replayHeader:           defaultReplayHeader,
		maxEntries:             defaultMaxEntries,
		maxWaiters:             defaultMaxWaiters,
		maxRequestBodyBytes:    defaultMaxRequestBodyBytes,
		maxResponseBodyBytes:   defaultMaxResponseBodyBytes,
		maxBytes:               defaultMaxBytes,
		completedTTL:           defaultCompletedTTL,
		leaderDisconnect:       LeaderContinue,
		now:                    time.Now,
		privateResponseHeaders: map[string]struct{}{},
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.namespace == nil {
		return nil, errors.New("idempotency: WithNamespace is required; a shared default namespace is not allowed")
	}
	if cfg.maxEntries <= 0 || cfg.maxWaiters < 0 || cfg.maxRequestBodyBytes <= 0 ||
		cfg.maxResponseBodyBytes <= 0 || cfg.maxBytes <= 0 || cfg.completedTTL <= 0 {
		return nil, errors.New("idempotency: capacities and TTL must be positive")
	}
	if cfg.maxResponseBodyBytes > cfg.maxBytes {
		return nil, errors.New("idempotency: MaxResponseBodyBytes must not exceed MaxBytes")
	}
	sort.Strings(cfg.businessHeaderNames)
	if len(cfg.businessHeaderNames) > 1 {
		deduped := cfg.businessHeaderNames[:1]
		for _, n := range cfg.businessHeaderNames[1:] {
			if n != deduped[len(deduped)-1] {
				deduped = append(deduped, n)
			}
		}
		cfg.businessHeaderNames = deduped
	}

	lim := &limits{
		maxEntries:           cfg.maxEntries,
		maxWaiters:           cfg.maxWaiters,
		maxBytes:             cfg.maxBytes,
		maxResponseBodyBytes: cfg.maxResponseBodyBytes,
		completedTTL:         cfg.completedTTL,
	}
	return &Idempotency{cfg: cfg, store: newStore(lim, cfg.now)}, nil
}

// Middleware returns the Hertz handler to attach to selected routes.
func (m *Idempotency) Middleware() app.HandlerFunc { return m.handle }

// handle is the actual middleware entry point.
func (m *Idempotency) handle(c context.Context, ctx *app.RequestContext) {
	key := strings.TrimSpace(string(ctx.GetHeader(m.cfg.keyHeader)))
	if key == "" {
		// Caller did not opt this request in.
		ctx.Next(c)
		return
	}
	namespace, ok := m.cfg.namespace(c, ctx)
	if !ok || namespace == "" {
		// Trusted identity unavailable: never fall back to a global key.
		ctx.Next(c)
		return
	}

	fp, err := m.fingerprint(ctx)
	if err != nil {
		if errors.Is(err, errBodyTooLarge) {
			m.reject(ctx, statusFingerprintTooLarge, "request body is too large to fingerprint within the configured budget")
			return
		}
		m.reject(ctx, statusFingerprintRead, "request body could not be read for fingerprinting")
		return
	}

	dec := m.store.begin(namespace+"\x00"+key, fp)
	switch dec.kind {
	case decReplay:
		m.replay(ctx, dec.result)
	case decConflict:
		m.reject(ctx, statusFingerprintMismatch, "idempotency key was already used with a different request fingerprint")
	case decRejected:
		m.reject(ctx, dec.status, "idempotency capacity limit reached")
	case decFollower:
		m.serveFollower(c, ctx, dec)
	case decLeader:
		m.serveLeader(c, ctx, dec.token)
	}
}

// serveFollower waits on the inflight leader and applies the definite outcome
// without ever executing the business handler.
func (m *Idempotency) serveFollower(c context.Context, ctx *app.RequestContext, dec decision) {
	switch m.store.await(c, dec.e, dec.w) {
	case stateCompleted:
		m.replay(ctx, dec.e.result)
	case stateFailed:
		m.reject(ctx, statusLeaderFailed, "the original idempotent request failed before producing a response")
	case stateNotReplayable:
		m.reject(ctx, statusNotReplayable, "the original idempotent request produced a response that cannot be replayed")
	case stateWaiterCanceled:
		m.reject(ctx, statusWaiterCanceled, "the wait for the idempotent result was canceled")
	default:
		m.reject(ctx, statusLeaderFailed, "idempotent request ended in an unknown terminal state")
	}
}

// serveLeader executes the handler exactly once and publishes the definite
// terminal state.
func (m *Idempotency) serveLeader(c context.Context, ctx *app.RequestContext, t leaderToken) {
	stopWatch := make(chan struct{})
	if m.cfg.leaderDisconnect == LeaderFail && c.Done() != nil {
		go func() {
			select {
			case <-c.Done():
				m.store.leaderDisconnected(t)
			case <-stopWatch:
			}
		}()
	}

	var recovered interface{}
	var panicked bool
	func() {
		defer func() {
			if r := recover(); r != nil {
				recovered = r
				panicked = true
			}
		}()
		ctx.Next(c)
	}()
	close(stopWatch)

	if panicked {
		// Explicit, non-cached terminal for every waiter; the entry is
		// removed so a later retry may execute again.
		m.store.completeFailure(t)
		ctx.Response.Reset()
		ctx.SetStatusCode(statusLeaderFailed)
		ctx.SetContentType("text/plain; charset=utf-8")
		if m.cfg.panicHandler != nil {
			m.cfg.panicHandler(c, ctx, recovered)
		}
		if ctx.Response.StatusCode() == 0 {
			ctx.SetStatusCode(statusLeaderFailed)
		}
		ctx.Response.Header.Set(m.cfg.replayHeader, "false")
		ctx.Abort()
		return
	}

	result, replayable := m.capture(ctx)
	if !replayable {
		// Streaming/hijacked/over-budget: the leader's own response is
		// delivered untouched, but followers get an explicit terminal and
		// nothing partial is cached.
		m.store.completeTerminal(t, stateNotReplayable)
		return
	}

	m.store.completeSuccess(t, result)
	ctx.Response.Header.Set(m.cfg.replayHeader, "false")
}

// capture snapshots a complete buffered response after the handler returns.
// It returns ok=false for anything that cannot be replayed faithfully:
// hijacked connections, custom hijack writers, body streams, or bodies over
// the per-response budget. Every retained byte is copied.
func (m *Idempotency) capture(ctx *app.RequestContext) (*cachedResponse, bool) {
	if ctx.Hijacked() ||
		ctx.Response.GetHijackWriter() != nil ||
		ctx.Response.IsBodyStream() {
		return nil, false
	}

	body := ctx.Response.BodyBytes()
	if len(body) > m.cfg.maxResponseBodyBytes {
		return nil, false
	}

	deny := m.buildDenyHeaders(ctx)

	var pairs [][2]string
	size := len(body)
	ctx.Response.Header.VisitAll(func(key, value []byte) {
		if _, blocked := deny[strings.ToLower(string(key))]; blocked {
			return
		}
		// Independent copies: the request context and its buffers are
		// recycled after the request finishes.
		k := string(key)
		v := string(value)
		pairs = append(pairs, [2]string{k, v})
		size += len(k) + len(v)
	})

	bodyCopy := make([]byte, len(body))
	copy(bodyCopy, body)

	status := ctx.Response.StatusCode()
	if status == 0 {
		status = consts.StatusOK
	}
	return &cachedResponse{status: status, headers: pairs, body: bodyCopy, size: size}, true
}

// buildDenyHeaders returns the lower-cased set of response header names that
// must not be replayed: hop-by-hop/framing headers, headers listed by the
// response's own Connection header, caller-designated private headers and the
// replay marker itself.
func (m *Idempotency) buildDenyHeaders(ctx *app.RequestContext) map[string]struct{} {
	deny := map[string]struct{}{
		"connection":          {},
		"keep-alive":          {},
		"proxy-connection":    {},
		"proxy-authenticate":  {},
		"proxy-authorization": {},
		"te":                  {},
		"trailer":             {},
		"transfer-encoding":   {},
		"upgrade":             {},
		// Framing is recomputed by Hertz when the follower's response is
		// written; the leader's length must not be replayed verbatim.
		"content-length":                    {},
		strings.ToLower(m.cfg.replayHeader): {},
	}
	for n := range m.cfg.privateResponseHeaders {
		deny[n] = struct{}{}
	}
	// RFC 7230: headers named by Connection are hop-by-hop for this message.
	for _, connVal := range ctx.Response.Header.PeekAll("Connection") {
		for _, token := range strings.Split(string(connVal), ",") {
			if token = strings.TrimSpace(strings.ToLower(token)); token != "" {
				deny[token] = struct{}{}
			}
		}
	}
	return deny
}

// replay writes the cached response into the follower's response without
// executing the handler chain.
func (m *Idempotency) replay(ctx *app.RequestContext, res *cachedResponse) {
	ctx.Response.Reset()
	ctx.SetStatusCode(res.status)
	for _, kv := range res.headers {
		ctx.Response.Header.Add(kv[0], kv[1])
	}
	ctx.Response.SetBody(res.body)
	ctx.Response.Header.Set(m.cfg.replayHeader, "true")
	ctx.Abort()
}

// reject writes an explicit terminal response (never a cached replay) and
// aborts the handler chain.
func (m *Idempotency) reject(ctx *app.RequestContext, status int, msg string) {
	ctx.Response.Reset()
	ctx.SetStatusCode(status)
	ctx.SetContentType("text/plain; charset=utf-8")
	ctx.SetBodyString(msg)
	ctx.Response.Header.Set(m.cfg.replayHeader, "false")
	ctx.Abort()
}
