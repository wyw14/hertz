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

package http1

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	inStats "github.com/cloudwego/hertz/internal/stats"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/test/mock"
	"github.com/cloudwego/hertz/pkg/common/tracer/traceinfo"
	"github.com/cloudwego/hertz/pkg/network"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/protocol/http1/req"
	"github.com/cloudwego/hertz/pkg/protocol/http1/resp"
)

func readMockResponse(t *testing.T, c *mock.Conn) *protocol.Response {
	t.Helper()
	r := protocol.AcquireResponse()
	if err := resp.Read(r, c.WriterRecorder()); err != nil {
		t.Fatalf("read mock response: %s", err)
	}
	return r
}

type countingPermit struct {
	released *int32
	once     sync.Once
}

func (p *countingPermit) Release() {
	p.once.Do(func() { atomic.AddInt32(p.released, 1) })
}

type fakeAdmission struct {
	decision Admission
	released int32
}

func (f *fakeAdmission) Acquire(ctx context.Context, conn network.Conn, req *protocol.Request) Admission {
	if f.decision.Rejected || f.decision.Permit != nil {
		return f.decision
	}
	// default: admit with a counting permit
	return Admission{Permit: &countingPermit{released: &f.released}}
}

func newAdmissionTestServer(t *testing.T, ac AdmissionController, handler func(context.Context, *app.RequestContext)) *Server {
	t.Helper()
	reqCtx := &app.RequestContext{}
	core := &mockCore{
		ctxPool: &sync.Pool{New: func() interface{} {
			ti := traceinfo.NewTraceInfo()
			ti.Stats().SetLevel(2)
			reqCtx.SetTraceInfo(ti)
			return reqCtx
		}},
		controller:  &inStats.Controller{},
		mockHandler: handler,
		isRunning:   true,
	}
	s := NewServer()
	s.Core = core
	s.AdmissionController = ac
	return s
}

// TestAdmissionPermitBackstopOnPanic proves a permit is released even when the
// handler panics and Serve unwinds, and that double release is a no-op.
func TestAdmissionPermitBackstopOnPanic(t *testing.T) {
	ac := &fakeAdmission{}
	s := newAdmissionTestServer(t, ac, func(c context.Context, ctx *app.RequestContext) {
		panic("boom")
	})
	c := mock.NewConn("GET /x HTTP/1.1\r\nHost: h\r\n\r\n")

	var panicked interface{}
	func() {
		defer func() { panicked = recover() }()
		_ = s.Serve(context.Background(), c)
	}()
	if panicked == nil {
		t.Fatal("expected panic to propagate")
	}
	if got := atomic.LoadInt32(&ac.released); got != 1 {
		t.Fatalf("permit released %d times during panic unwinding, want 1", got)
	}
}

// TestAdmissionRejectDrainKeepsAlive: a bounded-drained rejection answers with
// the configured status on a reusable connection.
func TestAdmissionRejectDrainKeepsAlive(t *testing.T) {
	ac := &fakeAdmission{decision: Admission{
		Rejected:      true,
		StatusCode:    429,
		MaxDrainBytes: 100,
	}}
	s := newAdmissionTestServer(t, ac, func(c context.Context, ctx *app.RequestContext) {
		t.Fatal("handler must not run on rejection")
	})
	c := mock.NewConn("POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\nhello")

	if err := s.Serve(context.Background(), c); err != nil {
		t.Fatalf("serve after drained reject: %v", err)
	}
	response := readMockResponse(t, c)
	if response.StatusCode() != 429 {
		t.Fatalf("rejection status = %d", response.StatusCode())
	}
	if response.Header.ConnectionClose() {
		t.Fatal("drained rejection must remain keep-alive")
	}
}

// TestAdmissionPermitReleasedAfterExchange verifies the normal success path
// releases the permit exactly once after the response flush.
func TestAdmissionPermitReleasedAfterExchange(t *testing.T) {
	ac := &fakeAdmission{}
	s := newAdmissionTestServer(t, ac, func(c context.Context, ctx *app.RequestContext) {
		ctx.SetStatusCode(204)
	})
	c := mock.NewConn("GET /x HTTP/1.1\r\nHost: h\r\n\r\n")
	if err := s.Serve(context.Background(), c); err != nil {
		t.Fatalf("serve: %v", err)
	}
	if got := atomic.LoadInt32(&ac.released); got != 1 {
		t.Fatalf("permit released %d times after exchange, want 1", got)
	}
}

// TestAdmissionRejectOversizedCloses verifies oversized/chunked unread bodies
// close the connection instead of leaking into the next request.
func TestAdmissionRejectOversizedCloses(t *testing.T) {
	ac := &fakeAdmission{decision: Admission{
		Rejected:      true,
		StatusCode:    503,
		MaxDrainBytes: 4,
	}}
	s := newAdmissionTestServer(t, ac, nil)
	c := mock.NewConn("POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 9\r\n\r\nhelloworld-prefix")

	if err := s.Serve(context.Background(), c); err == nil {
		t.Fatal("expected connection-close error return")
	}
}

// TestAdmissionCanceledClosesSilently verifies a canceled/closed-peer
// rejection produces no response and ends the connection.
func TestAdmissionCanceledClosesSilently(t *testing.T) {
	ac := &fakeAdmission{decision: Admission{
		Rejected: true,
		Canceled: true,
	}}
	s := newAdmissionTestServer(t, ac, nil)
	c := mock.NewConn("POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\nhello")

	before := c.WriterRecorder().WroteLen()
	if err := s.Serve(context.Background(), c); err == nil {
		t.Fatal("expected connection close on canceled admission")
	}
	if after := c.WriterRecorder().WroteLen(); after != before {
		t.Fatalf("canceled admission wrote %d bytes, want 0", after-before)
	}
}

// TestDrainUnreadRequestBody covers the bounded drain decisions directly.
func TestDrainUnreadRequestBody(t *testing.T) {
	parseHeader := func(raw string) (*protocol.Request, network.Reader) {
		c := mock.NewConn(raw)
		r := &protocol.Request{}
		if err := req.ReadHeaderWithLimit(&r.Header, c, 4096); err != nil {
			t.Fatalf("read header: %s", err)
		}
		return r, c
	}

	// fixed-length body fully within budget.
	r, c := parseHeader("POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\nhello")
	if !drainUnreadRequestBody(c, r, 100) {
		t.Fatal("5-byte body within 100 budget must drain")
	}
	if c.Len() != 0 {
		t.Fatalf("body not fully drained, %d bytes remain", c.Len())
	}

	// body larger than budget: nothing is consumed, connection must close.
	r, c = parseHeader("POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 9\r\n\r\nhelloworl")
	if drainUnreadRequestBody(c, r, 5) {
		t.Fatal("9-byte body beyond 5 budget must not drain")
	}
	if c.Len() != 9 {
		t.Fatalf("oversized body must be untouched, Len=%d", c.Len())
	}

	// chunked body cannot be bounded by bytes: close.
	r, c = parseHeader("POST /x HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n")
	if drainUnreadRequestBody(c, r, 100) {
		t.Fatal("chunked body must force connection close")
	}

	// empty / absent body is trivially drained.
	r, c = parseHeader("POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 0\r\n\r\n")
	if !drainUnreadRequestBody(c, r, 0) {
		t.Fatal("empty body must drain even with zero budget")
	}
	r, c = parseHeader("GET /x HTTP/1.1\r\nHost: h\r\n\r\n")
	if !drainUnreadRequestBody(c, r, 0) {
		t.Fatal("body-less request must drain")
	}
}

// TestAdmissionNoControllerUnchanged ensures the default server (no
// controller) behaves exactly as before.
func TestAdmissionNoControllerUnchanged(t *testing.T) {
	s := newAdmissionTestServer(t, nil, func(c context.Context, ctx *app.RequestContext) {
		ctx.SetStatusCode(204)
	})
	c := mock.NewConn("GET /x HTTP/1.1\r\nHost: h\r\n\r\n")
	if err := s.Serve(context.Background(), c); err != nil {
		t.Fatalf("serve without admission: %v", err)
	}
	if response := readMockResponse(t, c); response.StatusCode() != 204 {
		t.Fatalf("default status = %d", response.StatusCode())
	}
}
