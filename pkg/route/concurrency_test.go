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
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/hertz/internal/testutils"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/network"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/cloudwego/hertz/pkg/protocol/http1"
)

// concurrencyTestEnv wires one engine with the real (standard library)
// TCP transporter and a limited /limited route plus an unlimited /open route.
type concurrencyTestEnv struct {
	t       *testing.T
	engine  *Engine
	ln      net.Listener
	addr    string
	gate    chan struct{}
	stop    chan struct{}
	inFly   int32
	peak    int32
	stopped int32
}

func newConcurrencyTestEnv(t *testing.T, extraOpts ...config.Option) *concurrencyTestEnv {
	t.Helper()
	ln := testutils.NewTestListener(t)
	opts := append([]config.Option{
		{F: func(o *config.Options) {
			o.Listener = ln
			o.Addr = ln.Addr().String()
			o.TransporterNewer = standard.NewTransporter
			o.ExitWaitTimeout = 5 * time.Second
		}},
	}, extraOpts...)
	opt := config.NewOptions(opts)
	e := NewEngine(opt)

	env := &concurrencyTestEnv{
		t:      t,
		engine: e,
		ln:     ln,
		addr:   ln.Addr().String(),
		gate:   make(chan struct{}),
		stop:   make(chan struct{}),
	}

	e.GET("/limited", func(c context.Context, ctx *app.RequestContext) {
		cur := atomic.AddInt32(&env.inFly, 1)
		for {
			p := atomic.LoadInt32(&env.peak)
			if cur <= p || atomic.CompareAndSwapInt32(&env.peak, p, cur) {
				break
			}
		}
		select {
		case <-env.gate:
		case <-env.stop:
		}
		atomic.AddInt32(&env.inFly, -1)
		ctx.String(consts.StatusOK, "ok")
	})
	e.POST("/limited", func(c context.Context, ctx *app.RequestContext) {
		cur := atomic.AddInt32(&env.inFly, 1)
		for {
			p := atomic.LoadInt32(&env.peak)
			if cur <= p || atomic.CompareAndSwapInt32(&env.peak, p, cur) {
				break
			}
		}
		select {
		case <-env.gate:
		case <-env.stop:
		}
		atomic.AddInt32(&env.inFly, -1)
		ctx.String(consts.StatusOK, "ok")
	})
	e.GET("/open", func(c context.Context, ctx *app.RequestContext) {
		ctx.String(consts.StatusOK, "open")
	})

	go func() {
		_ = e.Run()
		atomic.StoreInt32(&env.stopped, 1)
	}()
	testutils.WaitEngineRunning(e)
	return env
}

func (env *concurrencyTestEnv) close() {
	select {
	case <-env.stop:
	default:
		close(env.stop)
	}
	if atomic.LoadInt32(&env.stopped) == 0 {
		_ = env.engine.Close()
	}
}

func (env *concurrencyTestEnv) release() {
	select {
	case <-env.gate:
		// already closed
	default:
		close(env.gate)
	}
}

func (env *concurrencyTestEnv) limit() *ConcurrencyLimit {
	l := env.engine.GetConcurrencyLimit(consts.MethodGet, "/limited")
	if l == nil {
		env.t.Fatalf("limit missing")
	}
	return l
}

func waitStats(t *testing.T, l *ConcurrencyLimit, want func(ConcurrencyStats) bool) ConcurrencyStats {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s := l.Stats()
		if want(s) {
			return s
		}
		time.Sleep(5 * time.Millisecond)
	}
	s := l.Stats()
	t.Fatalf("wait stats timeout, got %+v", s)
	return s
}

func httpGet(t *testing.T, addr, path string) *http.Response {
	t.Helper()
	resp, err := http.Get("http://" + addr + path)
	if err != nil {
		t.Fatalf("GET %s: %s", path, err)
	}
	return resp
}

func dialRaw(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %s", err)
	}
	return c, bufio.NewReader(c)
}

func readResponse(t *testing.T, r *bufio.Reader) *http.Response {
	t.Helper()
	resp, err := http.ReadResponse(r, nil)
	if err != nil {
		t.Fatalf("read response: %s", err)
	}
	return resp
}

// TestConcurrencyLimitMaxActiveAndQueueFull checks the shared counters: one
// active exchange owns the only slot, one request waits FIFO, further requests
// get the configured rejection status; the handler never runs concurrently
// beyond MaxActive and every counter returns to zero afterwards.
func TestConcurrencyLimitMaxActiveAndQueueFull(t *testing.T) {
	env := newConcurrencyTestEnv(t)
	defer env.close()

	l, err := env.engine.SetConcurrencyLimit(consts.MethodGet, "/limited", ConcurrencyLimitConfig{
		MaxActive:        1,
		MaxPending:       1,
		MaxWaitDuration:  10 * time.Second,
		RejectStatusCode: consts.StatusServiceUnavailable,
	})
	if err != nil {
		t.Fatal(err)
	}

	c1, r1 := dialRaw(t, env.addr)
	defer c1.Close()
	fmt.Fprintf(c1, "GET /limited HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Active == 1 })

	c2, r2 := dialRaw(t, env.addr)
	defer c2.Close()
	fmt.Fprintf(c2, "GET /limited HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Pending == 1 })

	// Third request cannot even queue.
	c3, r3 := dialRaw(t, env.addr)
	defer c3.Close()
	fmt.Fprintf(c3, "GET /limited HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	resp3 := readResponse(t, r3)
	if resp3.StatusCode != consts.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d", resp3.StatusCode)
	}

	// Unlimited route is unaffected while the limit is saturated.
	respOpen := httpGet(t, env.addr, "/open")
	if respOpen.StatusCode != consts.StatusOK {
		t.Fatalf("open route: %d", respOpen.StatusCode)
	}

	env.release()

	resp1 := readResponse(t, r1)
	resp2 := readResponse(t, r2)
	if resp1.StatusCode != consts.StatusOK || resp2.StatusCode != consts.StatusOK {
		t.Fatalf("queued/active responses: %d %d", resp1.StatusCode, resp2.StatusCode)
	}
	if peak := atomic.LoadInt32(&env.peak); peak != 1 {
		t.Fatalf("handler concurrency peak = %d, want 1", peak)
	}

	final := waitStats(t, l, func(s ConcurrencyStats) bool { return s.Active == 0 && s.Pending == 0 })
	if final.Active != 0 || final.Pending != 0 {
		t.Fatalf("final counters = %+v", final)
	}
}

// TestConcurrencyLimitWaitDeadline verifies FIFO waiting expires with the
// configured status and the request leaves the queue.
func TestConcurrencyLimitWaitDeadline(t *testing.T) {
	env := newConcurrencyTestEnv(t)
	defer env.close()

	l, err := env.engine.SetConcurrencyLimit(consts.MethodGet, "/limited", ConcurrencyLimitConfig{
		MaxActive:        1,
		MaxPending:       1,
		MaxWaitDuration:  200 * time.Millisecond,
		RejectStatusCode: consts.StatusTooManyRequests,
	})
	if err != nil {
		t.Fatal(err)
	}

	holder, hr := dialRaw(t, env.addr)
	defer holder.Close()
	fmt.Fprintf(holder, "GET /limited HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Active == 1 })

	waiter, wr := dialRaw(t, env.addr)
	defer waiter.Close()
	fmt.Fprintf(waiter, "GET /limited HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Pending == 1 })

	resp := readResponse(t, wr)
	if resp.StatusCode != consts.StatusTooManyRequests {
		t.Fatalf("want 429 after deadline, got %d", resp.StatusCode)
	}
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Pending == 0 })

	env.release()
	if r := readResponse(t, hr); r.StatusCode != consts.StatusOK {
		t.Fatalf("holder response: %d", r.StatusCode)
	}
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Active == 0 })
}

// slowReader emits chunks with delays; the HTTP/1.1 server pumps it inside
// writeResponse, i.e. after the handler returned but before the exchange ends.
type slowReader struct {
	chunks [][]byte
	delay  time.Duration
	i      int
}

func (r *slowReader) Read(p []byte) (int, error) {
	if r.i >= len(r.chunks) {
		return 0, io.EOF
	}
	time.Sleep(r.delay)
	n := copy(p, r.chunks[r.i])
	r.i++
	return n, nil
}

// TestConcurrencyLimitBodyStreamHoldsPermit proves that a handler returning
// early after SetBodyStream does NOT release the permit until the whole stream
// is sent and flushed.
func TestConcurrencyLimitBodyStreamHoldsPermit(t *testing.T) {
	env := newConcurrencyTestEnv(t)
	defer env.close()

	streamStarted := make(chan struct{})
	env.engine.GET("/stream", func(c context.Context, ctx *app.RequestContext) {
		close(streamStarted)
		sr := &slowReader{
			chunks: [][]byte{[]byte("aaaa"), []byte("bbbb"), []byte("cccc"), []byte("dddd")},
			delay:  150 * time.Millisecond,
		}
		// Handler returns immediately; the server keeps pumping the stream.
		ctx.SetBodyStream(sr, -1)
	})
	l, err := env.engine.SetConcurrencyLimit(consts.MethodGet, "/stream", ConcurrencyLimitConfig{
		MaxActive:       1,
		MaxPending:      0,
		MaxWaitDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	sc, sr := dialRaw(t, env.addr)
	defer sc.Close()
	fmt.Fprintf(sc, "GET /stream HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	<-streamStarted

	// While the body stream is being pumped, the slot stays occupied.
	foundActive := false
	for i := 0; i < 20; i++ {
		if l.Stats().Active == 1 {
			foundActive = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !foundActive {
		t.Fatal("permit released before stream finished")
	}

	cc, cr := dialRaw(t, env.addr)
	defer cc.Close()
	fmt.Fprintf(cc, "GET /stream HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	rejected := readResponse(t, cr)
	if rejected.StatusCode != consts.StatusServiceUnavailable {
		t.Fatalf("streaming exchange should occupy slot, got %d", rejected.StatusCode)
	}
	if l.Stats().Active != 1 {
		t.Fatalf("active = %d while stream in flight", l.Stats().Active)
	}

	resp := readResponse(t, sr)
	if resp.StatusCode != consts.StatusOK {
		t.Fatalf("stream response: %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "aaaabbbbccccdddd" {
		t.Fatalf("stream body = %q", body)
	}
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Active == 0 })
}

// TestConcurrencyLimitBodyNotReadWhileQueued proves queued requests do not
// read their request body first: headers are in, the body (100KB) is not sent,
// and the request is already counted as pending.
func TestConcurrencyLimitBodyNotReadWhileQueued(t *testing.T) {
	env := newConcurrencyTestEnv(t)
	defer env.close()

	env.engine.POST("/upload", func(c context.Context, ctx *app.RequestContext) {
		<-env.gate
		ctx.String(consts.StatusOK, "uploaded")
	})
	ul, err := env.engine.SetConcurrencyLimit(consts.MethodPost, "/upload", ConcurrencyLimitConfig{
		MaxActive:       1,
		MaxPending:      2,
		MaxWaitDuration: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Occupy the /upload slot with a zero-length POST on another connection.
	holder, _ := dialRaw(t, env.addr)
	defer holder.Close()
	fmt.Fprintf(holder, "POST /upload HTTP/1.1\r\nHost: h\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
	waitStats(t, ul, func(s ConcurrencyStats) bool { return s.Active == 1 })

	// Send headers advertising a 100KB body, but no body bytes yet. A
	// read-before-admit implementation would block on Peek(100000) and never
	// enter the queue.
	up, ur := dialRaw(t, env.addr)
	defer up.Close()
	fmt.Fprintf(up, "POST /upload HTTP/1.1\r\nHost: h\r\nContent-Length: 100000\r\nConnection: close\r\n\r\n")
	waitStats(t, ul, func(s ConcurrencyStats) bool { return s.Pending == 1 })

	// Now send the body and release the active slot; the queued request is
	// admitted, reads the bounded body and completes normally.
	bigBody := make([]byte, 100000)
	for i := range bigBody {
		bigBody[i] = 'x'
	}
	if _, err := up.Write(bigBody); err != nil {
		t.Fatal(err)
	}
	env.release()
	resp := readResponse(t, ur)
	if resp.StatusCode != consts.StatusOK {
		t.Fatalf("queued upload after admit: %d", resp.StatusCode)
	}
	waitStats(t, ul, func(s ConcurrencyStats) bool { return s.Active == 0 && s.Pending == 0 })
}

// TestConcurrencyLimitRejectDrainKeepsAlive verifies that after a rejection
// the unread body is drained so the next pipelined keep-alive request is
// parsed correctly; bodies beyond the drain budget close the connection.
func TestConcurrencyLimitRejectDrainKeepsAlive(t *testing.T) {
	env := newConcurrencyTestEnv(t)
	defer env.close()

	l, err := env.engine.SetConcurrencyLimit(consts.MethodPost, "/limited", ConcurrencyLimitConfig{
		MaxActive:       1,
		MaxPending:      0,
		MaxDrainBytes:   1024,
		MaxWaitDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	holder, _ := dialRaw(t, env.addr)
	defer holder.Close()
	fmt.Fprintf(holder, "POST /limited HTTP/1.1\r\nHost: h\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Active == 1 })

	// One connection, two pipelined requests: rejected POST with 5 body bytes
	// followed by an unlimited GET.
	conn, r := dialRaw(t, env.addr)
	defer conn.Close()
	pipelined := "POST /limited HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\nhello" +
		"GET /open HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n"
	if _, err := conn.Write([]byte(pipelined)); err != nil {
		t.Fatal(err)
	}
	first := readResponse(t, r)
	if first.StatusCode != consts.StatusServiceUnavailable {
		t.Fatalf("rejected POST status = %d", first.StatusCode)
	}
	second := readResponse(t, r)
	if second.StatusCode != consts.StatusOK {
		t.Fatalf("pipelined next request must parse after bounded drain, got %d", second.StatusCode)
	}

	// Body larger than the drain budget: connection is closed instead.
	conn2, r2 := dialRaw(t, env.addr)
	defer conn2.Close()
	if _, err := env.engine.SetConcurrencyLimit(consts.MethodPost, "/limited", ConcurrencyLimitConfig{
		MaxActive:       1,
		MaxPending:      0,
		MaxDrainBytes:   4,
		MaxWaitDuration: time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	// New generation needs an active holder again (previous holder may still
	// be finishing); occupy a slot on the updated policy.
	holder2, _ := dialRaw(t, env.addr)
	defer holder2.Close()
	fmt.Fprintf(holder2, "POST /limited HTTP/1.1\r\nHost: h\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Active == 1 && s.Generation == 2 })
	if _, err := conn2.Write([]byte("POST /limited HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello")); err != nil {
		t.Fatal(err)
	}
	resp := readResponse(t, r2)
	if resp.StatusCode != consts.StatusServiceUnavailable {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if !resp.Close {
		t.Fatal("oversized unread body must force connection close")
	}
	// Server must actually close the connection.
	buf := make([]byte, 16)
	conn2.SetReadDeadline(time.Now().Add(3 * time.Second))
	if n, _ := conn2.Read(buf); n != 0 {
		t.Fatalf("expected EOF, got %d bytes", n)
	}
}

// TestConcurrencyLimitQueueCancel removes a waiter when its context is
// canceled (which is also how an observed connection close is delivered when
// sense-client-disconnection is enabled), without adding extra reads.
func TestConcurrencyLimitQueueCancel(t *testing.T) {
	var cancels sync.Map
	opt := config.NewOptions([]config.Option{{F: func(o *config.Options) {
		o.OnConnect = func(ctx context.Context, conn network.Conn) context.Context {
			cctx, cancel := context.WithCancel(ctx)
			cancels.Store(conn.RemoteAddr().String(), cancel)
			return cctx
		}
	}}})
	ln := testutils.NewTestListener(t)
	opt.Listener = ln
	opt.Addr = ln.Addr().String()
	opt.TransporterNewer = standard.NewTransporter
	e := NewEngine(opt)
	gate := make(chan struct{})
	e.GET("/limited", func(c context.Context, ctx *app.RequestContext) {
		<-gate
		ctx.String(consts.StatusOK, "ok")
	})
	l, err := e.SetConcurrencyLimit(consts.MethodGet, "/limited", ConcurrencyLimitConfig{
		MaxActive:       1,
		MaxPending:      5,
		MaxWaitDuration: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	go e.Run()
	testutils.WaitEngineRunning(e)
	defer e.Close()
	defer close(gate)

	holder, _ := dialRaw(t, ln.Addr().String())
	defer holder.Close()
	fmt.Fprintf(holder, "GET /limited HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Active == 1 })

	waiter, _ := dialRaw(t, ln.Addr().String())
	fmt.Fprintf(waiter, "GET /limited HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Pending == 1 })

	// Client goes away and the request context is canceled (observed close).
	serverSideKey := waiter.LocalAddr().String()
	_ = waiter.Close()
	if v, ok := cancels.Load(serverSideKey); ok {
		v.(context.CancelFunc)()
	}
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Pending == 0 })
	if s := l.Stats(); s.Active != 1 {
		t.Fatalf("active exchange must be unaffected by cancel, %+v", s)
	}
}

// TestConcurrencyLimitWriteFailure releases the permit when the client stops
// reading a streaming response and the write fails, with no leak.
func TestConcurrencyLimitWriteFailure(t *testing.T) {
	env := newConcurrencyTestEnv(t)
	defer env.close()

	env.engine.GET("/big", func(c context.Context, ctx *app.RequestContext) {
		ctx.SetBodyStream(&endlessReader{delay: time.Millisecond}, -1)
	})
	l, err := env.engine.SetConcurrencyLimit(consts.MethodGet, "/big", ConcurrencyLimitConfig{
		MaxActive:       1,
		MaxPending:      0,
		MaxWaitDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	conn, r := dialRaw(t, env.addr)
	fmt.Fprintf(conn, "GET /big HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	// Read just the status line, then abandon the connection.
	br := readResponse(t, r)
	if br.StatusCode != consts.StatusOK {
		t.Fatalf("status %d", br.StatusCode)
	}
	_ = conn.Close()

	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Active == 0 })
}

type endlessReader struct {
	delay time.Duration
}

func (r *endlessReader) Read(p []byte) (int, error) {
	time.Sleep(r.delay)
	n := len(p)
	if n > 4096 {
		n = 4096
	}
	for i := 0; i < n; i++ {
		p[i] = 'z'
	}
	return n, nil
}

// TestConcurrencyLimitShutdown wakes queued requests and lets all permits
// return during graceful shutdown.
func TestConcurrencyLimitShutdown(t *testing.T) {
	ln := testutils.NewTestListener(t)
	opt := config.NewOptions(nil)
	opt.Listener = ln
	opt.Addr = ln.Addr().String()
	opt.TransporterNewer = standard.NewTransporter
	opt.ExitWaitTimeout = 5 * time.Second
	e := NewEngine(opt)
	gate := make(chan struct{})
	e.GET("/limited", func(c context.Context, ctx *app.RequestContext) {
		<-gate
		ctx.String(consts.StatusOK, "ok")
	})
	l, err := e.SetConcurrencyLimit(consts.MethodGet, "/limited", ConcurrencyLimitConfig{
		MaxActive:       1,
		MaxPending:      5,
		MaxWaitDuration: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	runErr := make(chan error, 1)
	go func() { runErr <- e.Run() }()
	testutils.WaitEngineRunning(e)

	holder, _ := dialRaw(t, ln.Addr().String())
	defer holder.Close()
	fmt.Fprintf(holder, "GET /limited HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Active == 1 })

	waiter, wr := dialRaw(t, ln.Addr().String())
	defer waiter.Close()
	fmt.Fprintf(waiter, "GET /limited HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Pending == 1 })

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- e.Shutdown(context.Background())
	}()

	// Queued request is rejected promptly due to shutdown.
	resp := readResponse(t, wr)
	if resp.StatusCode != consts.StatusServiceUnavailable {
		t.Fatalf("shutdown reject status = %d", resp.StatusCode)
	}
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Pending == 0 })

	// Let the in-flight exchange finish; its permit must return.
	close(gate)
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Active == 0 })

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("shutdown: %s", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("shutdown timeout")
	}
	select {
	case <-runErr:
	case <-time.After(time.Second):
		t.Fatal("engine Run did not return")
	}
}

// TestConcurrencyLimitGenerationOwnership checks that updating the policy
// publishes a new generation while requests admitted under the old one keep
// owning its permits: a new request is admitted concurrently under the new
// generation.
func TestConcurrencyLimitGenerationOwnership(t *testing.T) {
	env := newConcurrencyTestEnv(t)
	defer env.close()

	l1, err := env.engine.SetConcurrencyLimit(consts.MethodGet, "/limited", ConcurrencyLimitConfig{
		MaxActive:       1,
		MaxPending:      0,
		MaxWaitDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if l1.Stats().Generation != 1 {
		t.Fatalf("first generation = %d", l1.Stats().Generation)
	}

	a, ar := dialRaw(t, env.addr)
	defer a.Close()
	fmt.Fprintf(a, "GET /limited HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	waitStats(t, l1, func(s ConcurrencyStats) bool { return s.Active == 1 })

	// Publish generation 2 with its own independent counters while A is still
	// in flight under generation 1.
	l2, err := env.engine.SetConcurrencyLimit(consts.MethodGet, "/limited", ConcurrencyLimitConfig{
		MaxActive:       1,
		MaxPending:      0,
		MaxWaitDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if l2.Stats().Generation != 2 {
		t.Fatalf("second generation = %d", l2.Stats().Generation)
	}
	if s := l2.Stats(); s.Active != 0 {
		t.Fatalf("new generation must start empty, %+v", s)
	}

	// B is admitted under generation 2 even though A still owns gen1.
	b, br := dialRaw(t, env.addr)
	defer b.Close()
	fmt.Fprintf(b, "GET /limited HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	waitStats(t, l2, func(s ConcurrencyStats) bool { return s.Active == 1 })
	if peak := atomic.LoadInt32(&env.peak); peak < 2 {
		t.Fatalf("two generations must run concurrently, peak = %d", peak)
	}

	env.release()
	if r := readResponse(t, ar); r.StatusCode != consts.StatusOK {
		t.Fatalf("A response %d", r.StatusCode)
	}
	if r := readResponse(t, br); r.StatusCode != consts.StatusOK {
		t.Fatalf("B response %d", r.StatusCode)
	}
	waitStats(t, l2, func(s ConcurrencyStats) bool { return s.Active == 0 })
}

// TestConcurrencyLimitFIFO checks strict FIFO ordering of queued requests.
func TestConcurrencyLimitFIFO(t *testing.T) {
	env := newConcurrencyTestEnv(t)
	defer env.close()

	var (
		orderMu  sync.Mutex
		order    []int
		seq      int32
		itemGate = make(chan struct{})
	)
	env.engine.GET("/item", func(c context.Context, ctx *app.RequestContext) {
		id := int(atomic.AddInt32(&seq, 1))
		orderMu.Lock()
		order = append(order, id)
		orderMu.Unlock()
		<-itemGate
		ctx.String(consts.StatusOK, "x")
	})
	il, err := env.engine.SetConcurrencyLimit(consts.MethodGet, "/item", ConcurrencyLimitConfig{
		MaxActive:       1,
		MaxPending:      4,
		MaxWaitDuration: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	start := func() {
		conn, _ := dialRaw(t, env.addr)
		go func() {
			defer conn.Close()
			fmt.Fprintf(conn, "GET /item HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
			r := bufio.NewReader(conn)
			if resp, e := http.ReadResponse(r, nil); e == nil {
				io.Copy(io.Discard, resp.Body)
			}
		}()
	}

	start() // active
	waitStats(t, il, func(s ConcurrencyStats) bool { return s.Active == 1 })
	for i := 0; i < 3; i++ {
		start()
		waitStats(t, il, func(s ConcurrencyStats) bool { return s.Pending == i+1 })
	}

	close(itemGate)
	waitStats(t, il, func(s ConcurrencyStats) bool { return s.Active == 0 && s.Pending == 0 })

	orderMu.Lock()
	defer orderMu.Unlock()
	if len(order) != 4 {
		t.Fatalf("executed %d handlers, want 4: %v", len(order), order)
	}
	for i, v := range order {
		if v != i+1 {
			t.Fatalf("FIFO order = %v", order)
		}
	}
}

// TestConcurrencyLimitHijackRefused verifies hijack without explicit lease
// transfer is refused on a limited route and the permit still returns.
func TestConcurrencyLimitHijackRefused(t *testing.T) {
	env := newConcurrencyTestEnv(t)
	defer env.close()

	var hijackRan int32
	env.engine.GET("/ws", func(c context.Context, ctx *app.RequestContext) {
		ctx.Hijack(func(c network.Conn) {
			atomic.StoreInt32(&hijackRan, 1)
		})
		ctx.Data(consts.StatusOK, "text/plain", []byte("upgrade"))
	})
	if _, err := env.engine.SetConcurrencyLimit(consts.MethodGet, "/ws", ConcurrencyLimitConfig{
		MaxActive:       1,
		MaxPending:      0,
		MaxWaitDuration: time.Second,
	}); err != nil {
		t.Fatal(err)
	}

	conn, r := dialRaw(t, env.addr)
	defer conn.Close()
	fmt.Fprintf(conn, "GET /ws HTTP/1.1\r\nHost: h\r\nConnection: keep-alive\r\n\r\n")
	resp := readResponse(t, r)
	if resp.StatusCode != consts.StatusOK {
		t.Fatalf("upgrade response %d", resp.StatusCode)
	}
	// The server must close the connection and never run the hijack handler.
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 16)
	if n, _ := conn.Read(buf); n != 0 {
		t.Fatalf("expected connection close after refused hijack, got %d bytes", n)
	}
	if atomic.LoadInt32(&hijackRan) != 0 {
		t.Fatal("hijack handler ran without lease transfer")
	}
	waitStats(t, env.engine.GetConcurrencyLimit(consts.MethodGet, "/ws"),
		func(s ConcurrencyStats) bool { return s.Active == 0 })
}

// TestConcurrencyLimitHijackLeaseTransfer verifies the explicit lease is held
// across the hijack handler and released when it returns.
func TestConcurrencyLimitHijackLeaseTransfer(t *testing.T) {
	env := newConcurrencyTestEnv(t)
	defer env.close()

	hijackGate := make(chan struct{})
	env.engine.GET("/ws2", func(c context.Context, ctx *app.RequestContext) {
		ctx.TransferConcurrencyPermit()
		ctx.Hijack(func(c network.Conn) {
			<-hijackGate
			// keep-alive on the hijacked conn until gate opens
			c.Write([]byte("HIJACKED"))
		})
		ctx.Data(consts.StatusOK, "text/plain", []byte("upgrade"))
	})
	l, err := env.engine.SetConcurrencyLimit(consts.MethodGet, "/ws2", ConcurrencyLimitConfig{
		MaxActive:       1,
		MaxPending:      0,
		MaxWaitDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	conn, r := dialRaw(t, env.addr)
	defer conn.Close()
	fmt.Fprintf(conn, "GET /ws2 HTTP/1.1\r\nHost: h\r\n\r\n")
	resp := readResponse(t, r)
	if resp.StatusCode != consts.StatusOK {
		t.Fatalf("upgrade response %d", resp.StatusCode)
	}
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Active == 1 })

	// While the hijack handler blocks, the route still owns its only slot.
	other, orr := dialRaw(t, env.addr)
	defer other.Close()
	fmt.Fprintf(other, "GET /ws2 HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	if rr := readResponse(t, orr); rr.StatusCode != consts.StatusServiceUnavailable {
		t.Fatalf("hijack lease should hold slot, got %d", rr.StatusCode)
	}

	close(hijackGate)
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Active == 0 })

	// After the hijack handler returns, requests are admitted again.
	again, arr := dialRaw(t, env.addr)
	defer again.Close()
	fmt.Fprintf(again, "GET /ws2 HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	if rr := readResponse(t, arr); rr.StatusCode != consts.StatusOK {
		t.Fatalf("post-hijack request %d", rr.StatusCode)
	}
}

// TestConcurrencyLimitStressPeak fires 100 simultaneous exchanges at
// MaxActive=10/MaxPending=50 and verifies the observed full-exchange peak is
// exactly 10, 60 requests are served and 40 rejected, and all counters settle.
func TestConcurrencyLimitStressPeak(t *testing.T) {
	env := newConcurrencyTestEnv(t)
	defer env.close()

	l, err := env.engine.SetConcurrencyLimit(consts.MethodGet, "/limited", ConcurrencyLimitConfig{
		MaxActive:       10,
		MaxPending:      50,
		MaxWaitDuration: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	const total = 100
	var wg sync.WaitGroup
	var statusMu sync.Mutex
	ok200, rejected503 := 0, 0
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, r := dialRaw(t, env.addr)
			defer conn.Close()
			fmt.Fprintf(conn, "GET /limited HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
			resp := readResponse(t, r)
			io.Copy(io.Discard, resp.Body)
			statusMu.Lock()
			if resp.StatusCode == consts.StatusOK {
				ok200++
			} else if resp.StatusCode == consts.StatusServiceUnavailable {
				rejected503++
			}
			statusMu.Unlock()
		}()
	}

	// The gate keeps admitted exchanges blocked. Wait until 40 clients have
	// received rejections: with the 10+50 admitted/queued this proves all 100
	// requests reached admission while Active+Pending stays pinned at 60.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		statusMu.Lock()
		done := rejected503
		statusMu.Unlock()
		if done == total-60 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	statusMu.Lock()
	gotRejected := rejected503
	statusMu.Unlock()
	if gotRejected != 40 {
		env.release()
		wg.Wait()
		t.Fatalf("rejected before release = %d, want 40", gotRejected)
	}
	if s := l.Stats(); s.Active != 10 || s.Pending != 50 {
		env.release()
		wg.Wait()
		t.Fatalf("system counters under load = %+v, want Active=10 Pending=50", s)
	}
	if peak := atomic.LoadInt32(&env.peak); peak != 10 {
		t.Fatalf("full-exchange peak = %d, want 10", peak)
	}

	env.release()
	wg.Wait()

	statusMu.Lock()
	defer statusMu.Unlock()
	if ok200 != 60 || rejected503 != 40 {
		t.Fatalf("admission accounting: 200=%d 503=%d, want 60/40", ok200, rejected503)
	}
	if peak := atomic.LoadInt32(&env.peak); peak != 10 {
		t.Fatalf("final peak = %d, want 10", peak)
	}
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Active == 0 && s.Pending == 0 })
}

// TestConcurrencyLimitDirectAcquire exercises the admission state machine
// directly (queue body budget, exact-once release, rejection status).
func TestConcurrencyLimitDirectAcquire(t *testing.T) {
	opt := config.NewOptions(nil)
	e := NewEngine(opt)
	e.GET("/p", func(c context.Context, ctx *app.RequestContext) {})
	if _, err := e.SetConcurrencyLimit(consts.MethodGet, "/p", ConcurrencyLimitConfig{
		MaxActive:          1,
		MaxPending:         2,
		MaxQueuedBodyBytes: 10,
		MaxDrainBytes:      64,
		RejectStatusCode:   consts.StatusTooManyRequests,
		MaxWaitDuration:    time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.Init(); err != nil {
		t.Fatal(err)
	}
	if err := e.MarkAsRunning(); err != nil {
		t.Fatal(err)
	}
	l := e.GetConcurrencyLimit(consts.MethodGet, "/p")

	mkReq := func(cl int) *protocol.Request {
		req := &protocol.Request{}
		req.Header.SetMethod(consts.MethodGet)
		req.Header.SetHost("h")
		req.SetRequestURI("/p")
		if cl >= 0 {
			req.Header.SetContentLength(cl)
		}
		return req
	}

	a := e.Acquire(context.Background(), nil, mkReq(0))
	if a.Rejected || a.Permit == nil {
		t.Fatal("first acquire must admit")
	}
	// A second acquire queues (blocks until hand-off); run it concurrently.
	w1Ch := make(chan http1.Admission, 1)
	go func() {
		w1Ch <- e.Acquire(context.Background(), nil, mkReq(10))
	}()
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Pending == 1 && s.QueuedBodyBytes == 10 })

	// One more byte over budget: rejected synchronously with configured status.
	rj := e.Acquire(context.Background(), nil, mkReq(1))
	if !rj.Rejected || rj.StatusCode != consts.StatusTooManyRequests {
		t.Fatalf("budget overflow reject = %+v", rj)
	}

	// Exact-once release: releasing A transfers the slot to w1; a second
	// Release is a no-op and must not corrupt counters.
	a.Permit.Release()
	a.Permit.Release()
	w1 := <-w1Ch
	if w1.Rejected || w1.Permit == nil {
		t.Fatalf("queued acquire must be handed over, got %+v", w1)
	}
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Pending == 0 && s.Active == 1 })

	w1.Permit.Release()
	w1.Permit.Release()
	if s := l.Stats(); s.Active != 0 || s.Pending != 0 {
		t.Fatalf("counters after release = %+v", s)
	}
}

// TestConcurrencyLimitSetValidation covers API validation and Reset.
func TestConcurrencyLimitSetValidation(t *testing.T) {
	opt := config.NewOptions(nil)
	e := NewEngine(opt)
	if _, err := e.SetConcurrencyLimit(consts.MethodGet, "/missing", ConcurrencyLimitConfig{MaxActive: 1}); err == nil {
		t.Fatal("setting a limit on an unregistered route must fail")
	}
	e.GET("/r", func(c context.Context, ctx *app.RequestContext) {})
	l, err := e.SetConcurrencyLimit(consts.MethodGet, "/r", ConcurrencyLimitConfig{MaxActive: 2})
	if err != nil {
		t.Fatal(err)
	}
	if l == nil {
		t.Fatal("limit handle nil")
	}
	if got := e.GetConcurrencyLimit(consts.MethodGet, "/r"); got != l {
		t.Fatal("handle mismatch")
	}
	e.ResetConcurrencyLimit(consts.MethodGet, "/r")
	if e.GetConcurrencyLimit(consts.MethodGet, "/r") != nil {
		t.Fatal("reset must detach the policy")
	}
	// MaxActive<=0 disables.
	if _, err := e.SetConcurrencyLimit(consts.MethodGet, "/r", ConcurrencyLimitConfig{MaxActive: 0}); err != nil {
		t.Fatal(err)
	}
	if e.GetConcurrencyLimit(consts.MethodGet, "/r") != nil {
		t.Fatal("MaxActive<=0 must remove the policy")
	}
}

// TestConcurrencyLimitPanicReleasesPermit ensures a recovered panicking
// handler cannot leak the slot (engine.PanicHandler path: the exchange still
// completes and the permit returns at its end).
func TestConcurrencyLimitPanicReleasesPermit(t *testing.T) {
	ln := testutils.NewTestListener(t)
	opt := config.NewOptions(nil)
	opt.Listener = ln
	opt.Addr = ln.Addr().String()
	opt.TransporterNewer = standard.NewTransporter
	e := NewEngine(opt)
	recoveredGate := make(chan struct{})
	e.PanicHandler = func(c context.Context, ctx *app.RequestContext) {
		ctx.String(consts.StatusInternalServerError, "panic")
		close(recoveredGate)
	}
	e.GET("/panic", func(c context.Context, ctx *app.RequestContext) {
		panic("boom")
	})
	l, err := e.SetConcurrencyLimit(consts.MethodGet, "/panic", ConcurrencyLimitConfig{
		MaxActive:       1,
		MaxPending:      0,
		MaxWaitDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	go e.Run()
	testutils.WaitEngineRunning(e)
	defer e.Close()

	conn, r := dialRaw(t, ln.Addr().String())
	defer conn.Close()
	fmt.Fprintf(conn, "GET /panic HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	resp := readResponse(t, r)
	if resp.StatusCode != consts.StatusInternalServerError {
		t.Fatalf("panic response %d", resp.StatusCode)
	}
	<-recoveredGate
	waitStats(t, l, func(s ConcurrencyStats) bool { return s.Active == 0 })
}
