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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/network"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/cloudwego/hertz/pkg/route"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

const (
	hdrKey    = "Idempotency-Key"
	hdrTenant = "X-Tenant"
	hdrReplay = "Idempotency-Replayed"
)

func tenantNamespace(c context.Context, ctx *app.RequestContext) (string, bool) {
	ns := strings.TrimSpace(string(ctx.GetHeader(hdrTenant)))
	return ns, ns != ""
}

func newTestIdempotency(t *testing.T, opts ...Option) *Idempotency {
	t.Helper()
	all := append([]Option{WithNamespace(tenantNamespace)}, opts...)
	m, err := New(all...)
	require.NoError(t, err)
	return m
}

func newRequestCtx(method, target string, body []byte, headers ...[2]string) *app.RequestContext {
	ctx := app.NewContext(0)
	ctx.Request.SetMethod(method)
	ctx.Request.SetRequestURI(target)
	for _, h := range headers {
		ctx.Request.Header.Set(h[0], h[1])
	}
	if body != nil {
		ctx.Request.SetBodyRaw(body)
	}
	return ctx
}

func keyReq(ns, key, target, body string, extra ...[2]string) *app.RequestContext {
	hs := append([][2]string{{hdrKey, key}, {hdrTenant, ns}}, extra...)
	return newRequestCtx(consts.MethodPost, target, []byte(body), hs...)
}

// invoke runs the middleware directly in front of a single business handler,
// mirroring how the engine starts the chain (index == -1, middleware calls
// Next to reach the handler).
func invoke(m *Idempotency, c context.Context, ctx *app.RequestContext, h app.HandlerFunc) {
	ctx.SetHandlers(app.HandlersChain{h})
	m.Middleware()(c, ctx)
}

type respSnapshot struct {
	status int
	body   string
	replay string
}

func snapshot(ctx *app.RequestContext) respSnapshot {
	return respSnapshot{
		status: ctx.Response.StatusCode(),
		body:   string(ctx.Response.Body()),
		replay: string(ctx.Response.Header.Peek(hdrReplay)),
	}
}

func fire(m *Idempotency, c context.Context, ctx *app.RequestContext, h app.HandlerFunc) <-chan respSnapshot {
	ch := make(chan respSnapshot, 1)
	go func() {
		invoke(m, c, ctx, h)
		ch <- snapshot(ctx)
	}()
	return ch
}

// gate lets a leader handler block inside the handler until the test releases
// it, while signaling once the leader has truly entered the handler.
type gate struct {
	started chan struct{}
	release chan struct{}
	calls   int32
	once    sync.Once
}

func newGate() *gate {
	return &gate{started: make(chan struct{}), release: make(chan struct{})}
}

func (g *gate) handler(build func(ctx *app.RequestContext)) app.HandlerFunc {
	return func(c context.Context, ctx *app.RequestContext) {
		atomic.AddInt32(&g.calls, 1)
		g.once.Do(func() { close(g.started) })
		<-g.release
		if build != nil {
			build(ctx)
		}
	}
}

func (g *gate) waitLeaderIn(t *testing.T) {
	t.Helper()
	select {
	case <-g.started:
	case <-time.After(2 * time.Second):
		t.Fatal("leader never entered the handler")
	}
}

func (g *gate) doRelease() { close(g.release) }

func waitWaiters(t *testing.T, m *Idempotency, composite string, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return m.store.waitersCount(composite) == n
	}, 2*time.Second, time.Millisecond, "expected %d waiters on %q", n, composite)
}

func ck(ns, key string) string { return ns + "\x00" + key }

// ---------------------------------------------------------------------------
// 1. Concurrent identical requests: handler runs exactly once
// ---------------------------------------------------------------------------

func TestConcurrentSameKey_LeaderExecutesOnce(t *testing.T) {
	m := newTestIdempotency(t)
	g := newGate()
	h := g.handler(func(ctx *app.RequestContext) {
		ctx.SetStatusCode(consts.StatusOK)
		ctx.Response.Header.Set("X-Biz", "v1")
		ctx.SetBodyString(`{"order":"ORD-1"}`)
	})

	leader := keyReq("tenantA", "k1", "/create?z=1", `{"a":1}`)
	leaderRes := fire(m, context.Background(), leader, h)
	g.waitLeaderIn(t)

	const followers = 8
	chs := make([]<-chan respSnapshot, followers)
	for i := 0; i < followers; i++ {
		fctx := keyReq("tenantA", "k1", "/create?z=1", `{"a":1}`)
		chs[i] = fire(m, context.Background(), fctx, h)
	}
	waitWaiters(t, m, ck("tenantA", "k1"), followers)

	g.doRelease()

	leaderSnap := <-leaderRes
	require.Equal(t, consts.StatusOK, leaderSnap.status)
	require.Equal(t, `{"order":"ORD-1"}`, leaderSnap.body)

	leaders := 0
	replays := 0
	for i := 0; i < followers; i++ {
		s := <-chs[i]
		require.Equal(t, consts.StatusOK, s.status, "follower %d", i)
		require.Equal(t, `{"order":"ORD-1"}`, s.body, "follower %d", i)
		require.Equal(t, "true", s.replay, "follower %d", i)
		replays++
	}
	if leaderSnap.replay == "true" {
		replays++
	} else {
		leaders++
		require.Equal(t, "false", leaderSnap.replay)
	}
	require.Equal(t, 1, leaders, "exactly one non-replayed leader")
	require.Equal(t, followers, replays)
	require.Equal(t, int32(1), atomic.LoadInt32(&g.calls), "business handler must run exactly once")
	require.Equal(t, stateCompleted, m.store.stateOf(ck("tenantA", "k1")))
}

// ---------------------------------------------------------------------------
// 2. Same key, different fingerprint -> 409, second handler never runs
// ---------------------------------------------------------------------------

func TestDifferentFingerprint_Conflict(t *testing.T) {
	m := newTestIdempotency(t)
	g := newGate()
	h := g.handler(func(ctx *app.RequestContext) {
		ctx.SetStatusCode(consts.StatusOK)
		ctx.SetBodyString("ok")
	})

	leader := keyReq("tenantA", "k2", "/pay", `{"amount":100}`)
	leaderRes := fire(m, context.Background(), leader, h)
	g.waitLeaderIn(t)

	// Different body while in-flight.
	dup := keyReq("tenantA", "k2", "/pay", `{"amount":200}`)
	dupRes := <-fire(m, context.Background(), dup, h)
	require.Equal(t, consts.StatusConflict, dupRes.status)
	require.Equal(t, "false", dupRes.replay)
	require.Equal(t, int32(1), atomic.LoadInt32(&g.calls), "conflict must not run handler")

	g.doRelease()
	require.Equal(t, consts.StatusOK, (<-leaderRes).status)

	// After completion, a different fingerprint still conflicts and still
	// never executes the handler.
	post := keyReq("tenantA", "k2", "/pay", `{"amount":999}`)
	invoke(m, context.Background(), post, h)
	require.Equal(t, consts.StatusConflict, post.Response.StatusCode())
	require.Equal(t, int32(1), atomic.LoadInt32(&g.calls))
}

// ---------------------------------------------------------------------------
// 3. Canonical target: query order irrelevant; method/path part of fingerprint
// ---------------------------------------------------------------------------

func TestCanonicalTarget(t *testing.T) {
	m := newTestIdempotency(t)
	h := func(c context.Context, ctx *app.RequestContext) {
		ctx.SetStatusCode(consts.StatusOK)
		ctx.SetBodyString("canonical")
	}

	leader := keyReq("tenantA", "k3", "/p?a=1&b=2", "")
	invoke(m, context.Background(), leader, h)
	require.Equal(t, consts.StatusOK, leader.Response.StatusCode())

	// Reordered identical query parameters -> same canonical target.
	same := keyReq("tenantA", "k3", "/p?b=2&a=1", "")
	invoke(m, context.Background(), same, h)
	require.Equal(t, consts.StatusOK, same.Response.StatusCode())
	require.Equal(t, "true", string(same.Response.Header.Peek(hdrReplay)))

	// Different path -> conflict.
	diffPath := keyReq("tenantA", "k3", "/q?a=1&b=2", "")
	invoke(m, context.Background(), diffPath, h)
	require.Equal(t, consts.StatusConflict, diffPath.Response.StatusCode())

	// Different method -> conflict.
	diffMethod := newRequestCtx(consts.MethodPut, "/p?a=1&b=2", nil,
		[2]string{hdrKey, "k3"}, [2]string{hdrTenant, "tenantA"})
	invoke(m, context.Background(), diffMethod, h)
	require.Equal(t, consts.StatusConflict, diffMethod.Response.StatusCode())
}

// ---------------------------------------------------------------------------
// 4. Designated business headers participate in the fingerprint
// ---------------------------------------------------------------------------

func TestBusinessHeadersInFingerprint(t *testing.T) {
	m := newTestIdempotency(t, WithBusinessHeaders("X-Variant"))
	h := func(c context.Context, ctx *app.RequestContext) {
		ctx.SetStatusCode(consts.StatusOK)
		ctx.SetBodyString("v")
	}

	leader := keyReq("tenantA", "k4", "/p", "", [2]string{"X-Variant", "a"})
	invoke(m, context.Background(), leader, h)
	require.Equal(t, consts.StatusOK, leader.Response.StatusCode())

	same := keyReq("tenantA", "k4", "/p", "", [2]string{"X-Variant", "a"})
	invoke(m, context.Background(), same, h)
	require.Equal(t, "true", string(same.Response.Header.Peek(hdrReplay)))

	missing := keyReq("tenantA", "k4", "/p", "")
	invoke(m, context.Background(), missing, h)
	require.Equal(t, consts.StatusConflict, missing.Response.StatusCode())

	changed := keyReq("tenantA", "k4", "/p", "", [2]string{"X-Variant", "b"})
	invoke(m, context.Background(), changed, h)
	require.Equal(t, consts.StatusConflict, changed.Response.StatusCode())
}

// ---------------------------------------------------------------------------
// 5. Tenant isolation: same key, different namespace -> independent entries
// ---------------------------------------------------------------------------

func TestTenantIsolation(t *testing.T) {
	m := newTestIdempotency(t)
	var calls int32
	h := func(c context.Context, ctx *app.RequestContext) {
		atomic.AddInt32(&calls, 1)
		ctx.SetStatusCode(consts.StatusOK)
		ctx.SetBodyString("result-" + string(ctx.GetHeader(hdrTenant)))
	}

	a1 := keyReq("tenantA", "shared-key", "/p", "body")
	invoke(m, context.Background(), a1, h)
	b1 := keyReq("tenantB", "shared-key", "/p", "body")
	invoke(m, context.Background(), b1, h)

	require.Equal(t, int32(2), atomic.LoadInt32(&calls))
	require.Equal(t, "result-tenantA", string(a1.Response.Body()))
	require.Equal(t, "result-tenantB", string(b1.Response.Body()))
	require.Equal(t, "false", string(a1.Response.Header.Peek(hdrReplay)))
	require.Equal(t, "false", string(b1.Response.Header.Peek(hdrReplay)))

	// Each tenant replays its own result; no cross-tenant collision.
	a2 := keyReq("tenantA", "shared-key", "/p", "body")
	invoke(m, context.Background(), a2, h)
	require.Equal(t, "true", string(a2.Response.Header.Peek(hdrReplay)))
	require.Equal(t, "result-tenantA", string(a2.Response.Body()))

	b2 := keyReq("tenantB", "shared-key", "/p", "body")
	invoke(m, context.Background(), b2, h)
	require.Equal(t, "true", string(b2.Response.Header.Peek(hdrReplay)))
	require.Equal(t, "result-tenantB", string(b2.Response.Body()))

	require.Equal(t, int32(2), atomic.LoadInt32(&calls), "replays must not re-execute handler")
}

// ---------------------------------------------------------------------------
// 6. Follower cancellation detaches only that waiter; leader unaffected
// ---------------------------------------------------------------------------

func TestFollowerCancel_DoesNotCancelLeader(t *testing.T) {
	m := newTestIdempotency(t)
	g := newGate()
	h := g.handler(func(ctx *app.RequestContext) {
		ctx.SetStatusCode(consts.StatusOK)
		ctx.SetBodyString("done")
	})

	leader := keyReq("tenantA", "k6", "/p", "b")
	leaderRes := fire(m, context.Background(), leader, h)
	g.waitLeaderIn(t)

	follower1Ctx, cancel1 := context.WithCancel(context.Background())
	f1 := keyReq("tenantA", "k6", "/p", "b")
	f1Res := fire(m, follower1Ctx, f1, h)
	waitWaiters(t, m, ck("tenantA", "k6"), 1)

	cancel1()
	s := <-f1Res
	require.Equal(t, consts.StatusRequestTimeout, s.status)

	// Leader still in-flight.
	require.Equal(t, stateInflight, m.store.stateOf(ck("tenantA", "k6")))
	require.Equal(t, int32(1), atomic.LoadInt32(&g.calls))

	// Another follower can join and receive the real replay.
	f2 := keyReq("tenantA", "k6", "/p", "b")
	f2Res := fire(m, context.Background(), f2, h)
	waitWaiters(t, m, ck("tenantA", "k6"), 1)

	g.doRelease()
	require.Equal(t, "done", (<-leaderRes).body)
	s2 := <-f2Res
	require.Equal(t, consts.StatusOK, s2.status)
	require.Equal(t, "true", s2.replay)
	require.Equal(t, "done", s2.body)
	require.Equal(t, int32(1), atomic.LoadInt32(&g.calls))
	require.Equal(t, 0, m.store.waitersCount(ck("tenantA", "k6")))
}

// ---------------------------------------------------------------------------
// 7. Leader panic: explicit 500 terminal to everyone, retry is allowed
// ---------------------------------------------------------------------------

func TestLeaderPanic_ExplicitTerminalAndRetry(t *testing.T) {
	m := newTestIdempotency(t)
	g := newGate()
	panicH := g.handler(func(ctx *app.RequestContext) {
		panic("boom")
	})

	leader := keyReq("tenantA", "k7", "/p", "b")
	leaderRes := fire(m, context.Background(), leader, panicH)
	g.waitLeaderIn(t)

	f1 := keyReq("tenantA", "k7", "/p", "b")
	f1Res := fire(m, context.Background(), f1, panicH)
	f2 := keyReq("tenantA", "k7", "/p", "b")
	f2Res := fire(m, context.Background(), f2, panicH)
	waitWaiters(t, m, ck("tenantA", "k7"), 2)

	g.doRelease()

	for name, ch := range map[string]<-chan respSnapshot{
		"leader": leaderRes, "follower1": f1Res, "follower2": f2Res,
	} {
		s := <-ch
		require.Equal(t, consts.StatusInternalServerError, s.status, name)
		require.Equal(t, "false", s.replay, name)
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&g.calls))
	require.Equal(t, entryState(254), m.store.stateOf(ck("tenantA", "k7")), "failed entry must not linger")

	// A later retry with the same key is allowed to execute again.
	var retries int32
	okH := func(c context.Context, ctx *app.RequestContext) {
		atomic.AddInt32(&retries, 1)
		ctx.SetStatusCode(consts.StatusOK)
		ctx.SetBodyString("recovered")
	}
	retry := keyReq("tenantA", "k7", "/p", "b")
	invoke(m, context.Background(), retry, okH)
	require.Equal(t, consts.StatusOK, retry.Response.StatusCode())
	require.Equal(t, "recovered", string(retry.Response.Body()))
	require.Equal(t, int32(1), atomic.LoadInt32(&retries))
}

// ---------------------------------------------------------------------------
// 8. MaxEntries: in-flight entries are never evicted; completed ones are
// ---------------------------------------------------------------------------

func TestMaxEntries_InFlightRejectedCompletedEvicted(t *testing.T) {
	m := newTestIdempotency(t, WithMaxEntries(1))
	g := newGate()
	h := g.handler(func(ctx *app.RequestContext) {
		ctx.SetStatusCode(consts.StatusOK)
		ctx.SetBodyString("k1")
	})

	k1 := keyReq("tenantA", "k1", "/p", "b")
	k1Res := fire(m, context.Background(), k1, h)
	g.waitLeaderIn(t)

	// Store full with an in-flight entry: new key is rejected, not executed,
	// and the in-flight entry is not evicted (which would duplicate work).
	k2 := keyReq("tenantA", "k2", "/p", "b")
	invoke(m, context.Background(), k2, h)
	require.Equal(t, consts.StatusServiceUnavailable, k2.Response.StatusCode())
	require.Equal(t, stateInflight, m.store.stateOf(ck("tenantA", "k1")))

	g.doRelease()
	require.Equal(t, consts.StatusOK, (<-k1Res).status)

	// k1 now completed: k2 can evict it and run.
	k2b := keyReq("tenantA", "k2", "/p", "b")
	invoke(m, context.Background(), k2b, h)
	require.Equal(t, consts.StatusOK, k2b.Response.StatusCode())
	require.Equal(t, int32(2), atomic.LoadInt32(&g.calls))

	// k1's cached entry was evicted: the same key executes again.
	k1b := keyReq("tenantA", "k1", "/p", "b")
	invoke(m, context.Background(), k1b, h)
	require.Equal(t, consts.StatusOK, k1b.Response.StatusCode())
	require.Equal(t, "false", string(k1b.Response.Header.Peek(hdrReplay)))
	require.Equal(t, int32(3), atomic.LoadInt32(&g.calls))
}

// ---------------------------------------------------------------------------
// 9. MaxWaiters: excess followers rejected without executing the handler
// ---------------------------------------------------------------------------

func TestMaxWaiters_Reject(t *testing.T) {
	m := newTestIdempotency(t, WithMaxWaiters(1))
	g := newGate()
	h := g.handler(func(ctx *app.RequestContext) {
		ctx.SetStatusCode(consts.StatusOK)
		ctx.SetBodyString("ok")
	})

	leader := keyReq("tenantA", "k9", "/p", "b")
	leaderRes := fire(m, context.Background(), leader, h)
	g.waitLeaderIn(t)

	f1 := keyReq("tenantA", "k9", "/p", "b")
	f1Res := fire(m, context.Background(), f1, h)
	waitWaiters(t, m, ck("tenantA", "k9"), 1)

	f2 := keyReq("tenantA", "k9", "/p", "b")
	invoke(m, context.Background(), f2, h)
	require.Equal(t, consts.StatusTooManyRequests, f2.Response.StatusCode())
	require.Equal(t, int32(1), atomic.LoadInt32(&g.calls))

	g.doRelease()
	require.Equal(t, consts.StatusOK, (<-leaderRes).status)
	s1 := <-f1Res
	require.Equal(t, consts.StatusOK, s1.status)
	require.Equal(t, "true", s1.replay)
	require.Equal(t, int32(1), atomic.LoadInt32(&g.calls))
}

// ---------------------------------------------------------------------------
// 10. Response over budget: leader succeeds, result is not cached
// ---------------------------------------------------------------------------

func TestMaxResponseBodyBytes_NotReplayable(t *testing.T) {
	m := newTestIdempotency(t, WithMaxResponseBodyBytes(8))
	g := newGate()
	bigBody := "01234567890123456789" // 20 bytes
	h := g.handler(func(ctx *app.RequestContext) {
		ctx.SetStatusCode(consts.StatusOK)
		ctx.SetBodyString(bigBody)
	})

	leader := keyReq("tenantA", "k10", "/p", "b")
	leaderRes := fire(m, context.Background(), leader, h)
	g.waitLeaderIn(t)

	f1 := keyReq("tenantA", "k10", "/p", "b")
	f1Res := fire(m, context.Background(), f1, h)
	waitWaiters(t, m, ck("tenantA", "k10"), 1)

	g.doRelease()
	ls := <-leaderRes
	require.Equal(t, consts.StatusOK, ls.status)
	require.Equal(t, bigBody, ls.body, "leader must receive its full response")
	require.Equal(t, entryState(254), m.store.stateOf(ck("tenantA", "k10")))

	fs := <-f1Res
	require.Equal(t, consts.StatusServiceUnavailable, fs.status, "follower must not get a half/false replay")
	require.NotEqual(t, bigBody, fs.body)

	// Not cached: a later request executes again.
	var calls int32
	h2 := func(c context.Context, ctx *app.RequestContext) {
		atomic.AddInt32(&calls, 1)
		ctx.SetStatusCode(consts.StatusOK)
		ctx.SetBodyString("again")
	}
	r := keyReq("tenantA", "k10", "/p", "b")
	invoke(m, context.Background(), r, h2)
	require.Equal(t, consts.StatusOK, r.Response.StatusCode())
	require.Equal(t, int32(1), atomic.LoadInt32(&calls))
}

// ---------------------------------------------------------------------------
// 11. Completed TTL counted from definite completion
// ---------------------------------------------------------------------------

func TestCompletedTTL(t *testing.T) {
	var clock atomic.Value
	now := time.Unix(1700000000, 0)
	clock.Store(now)
	m := newTestIdempotency(t, WithCompletedTTL(50*time.Millisecond), withNow(func() time.Time {
		return clock.Load().(time.Time)
	}))

	var calls int32
	h := func(c context.Context, ctx *app.RequestContext) {
		atomic.AddInt32(&calls, 1)
		ctx.SetStatusCode(consts.StatusOK)
		ctx.SetBodyString("ttl")
	}

	leader := keyReq("tenantA", "k11", "/p", "b")
	invoke(m, context.Background(), leader, h)

	within := keyReq("tenantA", "k11", "/p", "b")
	invoke(m, context.Background(), within, h)
	require.Equal(t, "true", string(within.Response.Header.Peek(hdrReplay)))
	require.Equal(t, int32(1), atomic.LoadInt32(&calls))

	clock.Store(now.Add(51 * time.Millisecond))

	expired := keyReq("tenantA", "k11", "/p", "b")
	invoke(m, context.Background(), expired, h)
	require.Equal(t, consts.StatusOK, expired.Response.StatusCode())
	require.Equal(t, "false", string(expired.Response.Header.Peek(hdrReplay)))
	require.Equal(t, "ttl", string(expired.Response.Body()))
	require.Equal(t, int32(2), atomic.LoadInt32(&calls), "entry must re-execute after completion TTL")
}

// ---------------------------------------------------------------------------
// 12. Streaming request body: rejected over budget, buffered when within
// ---------------------------------------------------------------------------

func TestStreamRequestBodyBudget(t *testing.T) {
	m := newTestIdempotency(t, WithMaxRequestBodyBytes(8))
	var gotBody string
	var calls int32
	h := func(c context.Context, ctx *app.RequestContext) {
		atomic.AddInt32(&calls, 1)
		gotBody = string(ctx.GetRawData())
		ctx.SetStatusCode(consts.StatusOK)
	}

	// Declared over-budget stream: rejected before the handler runs.
	big := newRequestCtx(consts.MethodPost, "/p", nil,
		[2]string{hdrKey, "k12"}, [2]string{hdrTenant, "tenantA"})
	big.Request.SetBodyStream(strings.NewReader("0123456789"), 10)
	invoke(m, context.Background(), big, h)
	require.Equal(t, consts.StatusRequestEntityTooLarge, big.Response.StatusCode())
	require.Equal(t, int32(0), atomic.LoadInt32(&calls))

	// Within-budget stream: fingerprinted, buffered back, handler sees body.
	small := newRequestCtx(consts.MethodPost, "/p", nil,
		[2]string{hdrKey, "k12b"}, [2]string{hdrTenant, "tenantA"})
	small.Request.SetBodyStream(strings.NewReader("abc"), 3)
	invoke(m, context.Background(), small, h)
	require.Equal(t, consts.StatusOK, small.Response.StatusCode())
	require.Equal(t, int32(1), atomic.LoadInt32(&calls))
	require.Equal(t, "abc", gotBody)
	require.False(t, small.Request.IsBodyStream(), "fingerprinted stream body must be restored as buffered")
}

// ---------------------------------------------------------------------------
// 13. Streaming response: never cached; followers get explicit 503
// ---------------------------------------------------------------------------

func TestStreamResponse_NotReplayable(t *testing.T) {
	m := newTestIdempotency(t)
	g := newGate()
	h := g.handler(func(ctx *app.RequestContext) {
		ctx.SetStatusCode(consts.StatusOK)
		ctx.SetBodyStream(bytes.NewReader([]byte("stream-body")), 11)
	})

	leader := keyReq("tenantA", "k13", "/p", "b")
	leaderRes := fire(m, context.Background(), leader, h)
	g.waitLeaderIn(t)

	f1 := keyReq("tenantA", "k13", "/p", "b")
	f1Res := fire(m, context.Background(), f1, h)
	waitWaiters(t, m, ck("tenantA", "k13"), 1)

	g.doRelease()
	ls := <-leaderRes
	require.Equal(t, consts.StatusOK, ls.status)
	fs := <-f1Res
	require.Equal(t, consts.StatusServiceUnavailable, fs.status)
	require.Equal(t, int32(1), atomic.LoadInt32(&g.calls))
	require.Equal(t, entryState(254), m.store.stateOf(ck("tenantA", "k13")))

	// Fresh request re-executes.
	r := keyReq("tenantA", "k13", "/p", "b")
	invoke(m, context.Background(), r, h)
	require.Equal(t, consts.StatusOK, r.Response.StatusCode())
	require.Equal(t, int32(2), atomic.LoadInt32(&g.calls))
}

// ---------------------------------------------------------------------------
// 14. Hijacked response: never cached
// ---------------------------------------------------------------------------

func TestHijackResponse_NotReplayable(t *testing.T) {
	m := newTestIdempotency(t)
	g := newGate()
	h := g.handler(func(ctx *app.RequestContext) {
		ctx.SetStatusCode(consts.StatusOK)
		ctx.Hijack(func(c network.Conn) {})
	})

	leader := keyReq("tenantA", "k14", "/p", "b")
	leaderRes := fire(m, context.Background(), leader, h)
	g.waitLeaderIn(t)

	f1 := keyReq("tenantA", "k14", "/p", "b")
	f1Res := fire(m, context.Background(), f1, h)
	waitWaiters(t, m, ck("tenantA", "k14"), 1)

	g.doRelease()
	ls := <-leaderRes
	require.Equal(t, consts.StatusOK, ls.status)
	fs := <-f1Res
	require.Equal(t, consts.StatusServiceUnavailable, fs.status)
}

// ---------------------------------------------------------------------------
// 15. Response header filtering: hop-by-hop / Connection tokens / private
// ---------------------------------------------------------------------------

func TestResponseHeaderFiltering(t *testing.T) {
	m := newTestIdempotency(t, WithPrivateResponseHeaders("X-Secret"))
	h := func(c context.Context, ctx *app.RequestContext) {
		ctx.SetStatusCode(consts.StatusCreated)
		ctx.Response.Header.Set("X-Biz", "keep")
		ctx.Response.Header.Set("X-Secret", "drop-me")
		ctx.Response.Header.Add("Set-Cookie", "a=1; Path=/")
		ctx.Response.Header.Add("Set-Cookie", "b=2; Path=/")
		ctx.Response.Header.Set("X-Conn-Token", "hop")
		ctx.Response.Header.Set("Connection", "X-Conn-Token")
		ctx.SetBodyString("h")
	}

	leader := keyReq("tenantA", "k15", "/p", "b")
	invoke(m, context.Background(), leader, h)

	dup := keyReq("tenantA", "k15", "/p", "b")
	invoke(m, context.Background(), dup, h)
	require.Equal(t, consts.StatusCreated, dup.Response.StatusCode())
	require.Equal(t, "true", string(dup.Response.Header.Peek(hdrReplay)))
	require.Equal(t, "keep", string(dup.Response.Header.Peek("X-Biz")))
	require.Empty(t, dup.Response.Header.Peek("X-Secret"))
	require.Empty(t, dup.Response.Header.Peek("X-Conn-Token"))
	require.Empty(t, dup.Response.Header.Peek("Connection"))
	require.Empty(t, dup.Response.Header.Peek("Content-Length"))
	var cookies []string
	dup.Response.Header.VisitAllCookie(func(key, value []byte) {
		cookies = append(cookies, string(value))
	})
	require.ElementsMatch(t, []string{"a=1; Path=/", "b=2; Path=/"}, cookies)
	require.Equal(t, "h", string(dup.Response.Body()))
}

// ---------------------------------------------------------------------------
// 16. Pooled context recycling never corrupts cached replies
// ---------------------------------------------------------------------------

func TestPooledContextRecycling_DeepCopied(t *testing.T) {
	m := newTestIdempotency(t)
	h := func(c context.Context, ctx *app.RequestContext) {
		ctx.SetStatusCode(consts.StatusOK)
		ctx.Response.Header.Set("X-Biz", "v1")
		ctx.SetBodyString("persisted-body")
	}

	leader := keyReq("tenantA", "k16", "/p", "b")
	invoke(m, context.Background(), leader, h)
	require.Equal(t, "persisted-body", string(leader.Response.Body()))

	// Simulate the server recycling the leader's pooled context.
	leader.Reset()

	dup := keyReq("tenantA", "k16", "/p", "b")
	invoke(m, context.Background(), dup, h)
	require.Equal(t, "persisted-body", string(dup.Response.Body()))
	require.Equal(t, "v1", string(dup.Response.Header.Peek("X-Biz")))
	require.Equal(t, "true", string(dup.Response.Header.Peek(hdrReplay)))
}

// ---------------------------------------------------------------------------
// 17. LeaderFail policy: disconnect fails waiters; stale completion discarded
// ---------------------------------------------------------------------------

func TestLeaderFailPolicy_DisconnectDiscardsStaleCompletion(t *testing.T) {
	m := newTestIdempotency(t, WithLeaderDisconnectPolicy(LeaderFail))
	g := newGate()
	h := g.handler(func(ctx *app.RequestContext) {
		ctx.SetStatusCode(consts.StatusOK)
		ctx.SetBodyString("late")
	})

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leader := keyReq("tenantA", "k17", "/p", "b")
	leaderRes := fire(m, leaderCtx, leader, h)
	g.waitLeaderIn(t)

	f1 := keyReq("tenantA", "k17", "/p", "b")
	f1Res := fire(m, context.Background(), f1, h)
	waitWaiters(t, m, ck("tenantA", "k17"), 1)

	// Leader's client gives up before completion.
	cancelLeader()
	fs := <-f1Res
	require.Equal(t, consts.StatusInternalServerError, fs.status, "follower must get an explicit failure terminal")
	require.Equal(t, entryState(254), m.store.stateOf(ck("tenantA", "k17")))

	// The leader goroutine eventually finishes: its completion must be
	// discarded (no cached bytes from the dead generation).
	g.doRelease()
	<-leaderRes
	require.Equal(t, 0, m.store.stats().TotalBytes)
	require.Equal(t, int32(1), atomic.LoadInt32(&g.calls))

	// A new request starts a fresh generation and executes normally.
	var calls2 int32
	h2 := func(c context.Context, ctx *app.RequestContext) {
		atomic.AddInt32(&calls2, 1)
		ctx.SetStatusCode(consts.StatusOK)
		ctx.SetBodyString("fresh")
	}
	r := keyReq("tenantA", "k17", "/p", "b")
	invoke(m, context.Background(), r, h2)
	require.Equal(t, consts.StatusOK, r.Response.StatusCode())
	require.Equal(t, "fresh", string(r.Response.Body()))
	require.Equal(t, int32(1), atomic.LoadInt32(&calls2))
}

// ---------------------------------------------------------------------------
// 17b. LeaderContinue (default): a leader disconnect does not fail the cohort
// ---------------------------------------------------------------------------

func TestLeaderContinuePolicy_Default(t *testing.T) {
	m := newTestIdempotency(t) // default LeaderContinue
	g := newGate()
	h := g.handler(func(ctx *app.RequestContext) {
		ctx.SetStatusCode(consts.StatusOK)
		ctx.SetBodyString("finished-anyway")
	})

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	defer cancelLeader()
	leader := keyReq("tenantA", "k17b", "/p", "b")
	leaderRes := fire(m, leaderCtx, leader, h)
	g.waitLeaderIn(t)

	f1 := keyReq("tenantA", "k17b", "/p", "b")
	f1Res := fire(m, context.Background(), f1, h)
	waitWaiters(t, m, ck("tenantA", "k17b"), 1)

	// Leader client disappears before completion: cohort keeps waiting.
	cancelLeader()
	// Give the (nonexistent, under Continue) disconnect hook a chance to run.
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, stateInflight, m.store.stateOf(ck("tenantA", "k17b")))

	g.doRelease()
	ls := <-leaderRes
	fs := <-f1Res
	require.Equal(t, consts.StatusOK, ls.status)
	require.Equal(t, consts.StatusOK, fs.status)
	require.Equal(t, "true", fs.replay)
	require.Equal(t, "finished-anyway", fs.body)
	require.Equal(t, int32(1), atomic.LoadInt32(&g.calls))
}

// ---------------------------------------------------------------------------
// 17c. High-volume mixed traffic: per-key exactly-once, no cross-key mixing
// ---------------------------------------------------------------------------

func TestStressManyKeysAndFollowers(t *testing.T) {
	m := newTestIdempotency(t, WithMaxEntries(4096), WithMaxWaiters(64))
	const keys = 80
	const followers = 6

	var calls int32
	h := func(c context.Context, ctx *app.RequestContext) {
		key := string(ctx.GetHeader(hdrKey))
		atomic.AddInt32(&calls, 1)
		// Small stagger to widen the in-flight overlap window.
		time.Sleep(2 * time.Millisecond)
		ctx.SetStatusCode(consts.StatusOK)
		ctx.SetBodyString("result-for-" + key)
	}

	var wg sync.WaitGroup
	errs := make(chan string, keys*(followers+1))
	for k := 0; k < keys; k++ {
		key := "stress-key-" + string(rune('a'+k%26)) + "-" + strconv.Itoa(k)
		for i := 0; i < followers+1; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx := keyReq("tenantA", key, "/p", "payload")
				invoke(m, context.Background(), ctx, h)
				if ctx.Response.StatusCode() != consts.StatusOK {
					errs <- "unexpected status"
					return
				}
				if got, want := string(ctx.Response.Body()), "result-for-"+key; got != want {
					errs <- "body cross-contamination: got " + got + " want " + want
				}
			}()
		}
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	require.Equal(t, int32(keys), atomic.LoadInt32(&calls), "handler runs exactly once per key")
}

// ---------------------------------------------------------------------------
// 18. Bypass: missing key or unresolved namespace runs handler normally
// ---------------------------------------------------------------------------

func TestBypass_NoKeyOrNamespace(t *testing.T) {
	m := newTestIdempotency(t)
	var calls int32
	h := func(c context.Context, ctx *app.RequestContext) {
		atomic.AddInt32(&calls, 1)
		ctx.SetStatusCode(consts.StatusOK)
		ctx.SetBodyString("normal")
	}

	// No idempotency key.
	noKey := newRequestCtx(consts.MethodPost, "/p", []byte("b"), [2]string{hdrTenant, "tenantA"})
	invoke(m, context.Background(), noKey, h)
	require.Equal(t, consts.StatusOK, noKey.Response.StatusCode())
	require.Empty(t, noKey.Response.Header.Peek(hdrReplay))

	// Namespace callback cannot resolve identity -> no shared global key.
	noNs := newRequestCtx(consts.MethodPost, "/p", []byte("b"), [2]string{hdrKey, "k18"})
	invoke(m, context.Background(), noNs, h)
	require.Equal(t, consts.StatusOK, noNs.Response.StatusCode())
	require.Empty(t, noNs.Response.Header.Peek(hdrReplay))

	require.Equal(t, int32(2), atomic.LoadInt32(&calls))
	require.Equal(t, 0, m.store.stats().Entries)
}

// ---------------------------------------------------------------------------
// 18b. End-to-end through the real route engine
// ---------------------------------------------------------------------------

func TestEngineIntegration(t *testing.T) {
	engine := route.NewEngine(config.NewOptions(nil))
	m := newTestIdempotency(t)

	var calls int32
	gate := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	engine.POST("/create", m.Middleware(), func(c context.Context, ctx *app.RequestContext) {
		atomic.AddInt32(&calls, 1)
		once.Do(func() { close(started) })
		<-gate
		ctx.SetStatusCode(consts.StatusCreated)
		ctx.Response.Header.Set("X-Biz", "eng")
		ctx.SetBodyString("created")
	})

	headers := func() []ut.Header {
		return []ut.Header{
			{Key: hdrKey, Value: "e1"},
			{Key: hdrTenant, Value: "t1"},
		}
	}
	perform := func(body string) <-chan *ut.ResponseRecorder {
		ch := make(chan *ut.ResponseRecorder, 1)
		go func() {
			ch <- ut.PerformRequest(engine, consts.MethodPost, "/create",
				&ut.Body{Body: strings.NewReader(body), Len: len(body)}, headers()...)
		}()
		return ch
	}

	leaderCh := perform("payload")
	<-started
	followerCh := perform("payload")
	waitWaiters(t, m, ck("t1", "e1"), 1)
	close(gate)

	leader := (<-leaderCh).Result()
	follower := (<-followerCh).Result()
	require.Equal(t, consts.StatusCreated, leader.StatusCode())
	require.Equal(t, consts.StatusCreated, follower.StatusCode())
	require.Equal(t, "created", string(follower.Body()))
	require.Equal(t, "eng", string(follower.Header.Peek("X-Biz")))
	require.Equal(t, "true", string(follower.Header.Peek(hdrReplay)))
	require.Equal(t, "false", string(leader.Header.Peek(hdrReplay)))
	require.Equal(t, int32(1), atomic.LoadInt32(&calls))

	// Sequential replay after completion still hits the cache.
	w := ut.PerformRequest(engine, consts.MethodPost, "/create",
		&ut.Body{Body: strings.NewReader("payload"), Len: 7}, headers()...)
	require.Equal(t, consts.StatusCreated, w.Result().StatusCode())
	require.Equal(t, "true", string(w.Result().Header.Peek(hdrReplay)))
	require.Equal(t, int32(1), atomic.LoadInt32(&calls))

	// Different fingerprint via the real engine: 409, no second execution.
	conflict := ut.PerformRequest(engine, consts.MethodPost, "/create",
		&ut.Body{Body: strings.NewReader("tampered"), Len: 8}, headers()...)
	require.Equal(t, consts.StatusConflict, conflict.Result().StatusCode())
	require.Equal(t, int32(1), atomic.LoadInt32(&calls))
}

// ---------------------------------------------------------------------------
// 19. Construction validation
// ---------------------------------------------------------------------------

func TestNewValidation(t *testing.T) {
	_, err := New()
	require.Error(t, err)

	_, err = New(WithNamespace(tenantNamespace), WithMaxEntries(0))
	require.Error(t, err)

	_, err = New(WithNamespace(tenantNamespace),
		WithMaxResponseBodyBytes(100), WithMaxBytes(50))
	require.Error(t, err)

	_, err = New(WithNamespace(tenantNamespace), WithCompletedTTL(0))
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// 20. Fingerprint unit-level checks
// ---------------------------------------------------------------------------

func TestFingerprintComposition(t *testing.T) {
	m := newTestIdempotency(t, WithBusinessHeaders("X-Variant"))

	base := keyReq("tenantA", "k", "/p?a=1&b=2", "body", [2]string{"X-Variant", "v"})
	baseFP, err := m.fingerprint(base)
	require.NoError(t, err)

	eq := func(req *app.RequestContext) []byte {
		fp, err := m.fingerprint(req)
		require.NoError(t, err)
		return fp
	}
	require.Equal(t, baseFP, eq(keyReq("tenantA", "k", "/p?b=2&a=1", "body", [2]string{"X-Variant", "v"})))
	require.NotEqual(t, baseFP, eq(keyReq("tenantA", "k", "/p?a=1&b=3", "body", [2]string{"X-Variant", "v"})))
	require.NotEqual(t, baseFP, eq(keyReq("tenantA", "k", "/p?a=1&b=2", "body2", [2]string{"X-Variant", "v"})))
	require.NotEqual(t, baseFP, eq(keyReq("tenantA", "k", "/p?a=1&b=2", "body", [2]string{"X-Variant", "w"})))
	require.NotEqual(t, baseFP, eq(keyReq("tenantA", "k", "/p?a=1&b=2", "body")))

	putReq := newRequestCtx(consts.MethodPut, "/p?a=1&b=2", []byte("body"),
		[2]string{hdrKey, "k"}, [2]string{hdrTenant, "tenantA"}, [2]string{"X-Variant", "v"})
	require.NotEqual(t, baseFP, eq(putReq))

	pathReq := keyReq("tenantA", "k", "/q?a=1&b=2", "body", [2]string{"X-Variant", "v"})
	require.NotEqual(t, baseFP, eq(pathReq))

	// Buffered body over budget is rejected for fingerprinting.
	small := newTestIdempotency(t, WithMaxRequestBodyBytes(4))
	big := keyReq("tenantA", "k", "/p", "12345")
	_, err = small.fingerprint(big)
	require.ErrorIs(t, err, errBodyTooLarge)
}
