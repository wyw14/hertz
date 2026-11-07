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

package server

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/hertz/internal/testutils"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server/render"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

const (
	etagJSON     = `"res-v3-json"`
	etagJSONGzip = `"res-v3-json-gzip"`
	etagJSONV2   = `"res-v3-json-v2"`
	etagXML      = `"res-v3-xml"`
)

type negotiateFixture struct {
	plainBody []byte
	v2Body    []byte
	gzipBody  []byte
	xmlBody   []byte

	callsPlain int32
	callsGzip  int32
	callsV2    int32
	callsXML   int32

	set *render.RepresentationSet
}

func newNegotiateFixture(t *testing.T) *negotiateFixture {
	t.Helper()
	f := &negotiateFixture{
		plainBody: []byte(`{"hello":"world"}`),
		v2Body:    []byte(`{"hello":"world","v":2}`),
		xmlBody:   []byte(`<hello>world</hello>`),
	}
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	if _, err := zw.Write(f.plainBody); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	f.gzipBody = gz.Bytes()

	bodyRenderer := func(calls *int32, body []byte) render.RepresentationRenderer {
		return render.RepresentationFunc(func(resp *protocol.Response) error {
			atomic.AddInt32(calls, 1)
			resp.AppendBody(body)
			return nil
		})
	}

	var err error
	f.set, err = render.NewRepresentationSet(
		render.Representation{
			MediaType:     "application/json; charset=utf-8",
			ETag:          etagJSON,
			ContentLength: len(f.plainBody),
			QS:            0.8,
			Renderer:      bodyRenderer(&f.callsPlain, f.plainBody),
		},
		render.Representation{
			MediaType:     "application/json; charset=utf-8",
			Encoding:      "gzip",
			ETag:          etagJSONGzip,
			ContentLength: len(f.gzipBody),
			// Server prefers the coded variant when the client rates both
			// codings equally.
			QS:       0.9,
			Renderer: bodyRenderer(&f.callsGzip, f.gzipBody),
		},
		render.Representation{
			MediaType:     "application/json; charset=utf-8; version=2",
			ETag:          etagJSONV2,
			ContentLength: len(f.v2Body),
			QS:            0.8,
			Renderer:      bodyRenderer(&f.callsV2, f.v2Body),
		},
		render.Representation{
			MediaType:     "application/xml",
			ETag:          etagXML,
			ContentLength: len(f.xmlBody),
			Renderer:      bodyRenderer(&f.callsXML, f.xmlBody),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// renderCounters is a point-in-time snapshot of renderer invocations.
type renderCounters struct {
	plain, gzip, v2, xml int32
}

func (f *negotiateFixture) snapshot() renderCounters {
	return renderCounters{
		plain: atomic.LoadInt32(&f.callsPlain),
		gzip:  atomic.LoadInt32(&f.callsGzip),
		v2:    atomic.LoadInt32(&f.callsV2),
		xml:   atomic.LoadInt32(&f.callsXML),
	}
}

func (f *negotiateFixture) startServer(t *testing.T) (*Hertz, net.Listener) {
	t.Helper()
	ln := testutils.NewTestListener(t)
	h := Default(WithListener(ln), WithTransport(standard.NewTransporter))

	handler := func(c context.Context, ctx *app.RequestContext) {
		if err := ctx.Negotiate(f.set); err != nil {
			t.Errorf("negotiate: %s", err)
		}
	}
	varyHandler := func(c context.Context, ctx *app.RequestContext) {
		// Middleware-style pre-existing Vary fields, including a token the
		// negotiator itself wants to add.
		ctx.Response.Header.Add("Vary", "X-Debug")
		ctx.Response.Header.Add("Vary", "accept, X-Other")
		if err := ctx.Negotiate(f.set); err != nil {
			t.Errorf("negotiate: %s", err)
		}
	}
	varyStarHandler := func(c context.Context, ctx *app.RequestContext) {
		ctx.Header("Vary", "*")
		if err := ctx.Negotiate(f.set); err != nil {
			t.Errorf("negotiate: %s", err)
		}
	}

	h.GET("/r", handler)
	h.HEAD("/r", handler)
	h.POST("/r", handler)
	h.GET("/vary", varyHandler)
	h.HEAD("/vary", varyHandler)
	h.GET("/varystar", varyStarHandler)

	go h.Spin()
	testutils.WaitEngineRunning(h)
	return h, ln
}

func negotiateHTTPClient() *http.Client {
	// Disable transparent gzip so Content-Encoding/ETag/body are observed
	// exactly as sent.
	tr := &http.Transport{
		DisableCompression: true,
		DisableKeepAlives:  false,
	}
	return &http.Client{Transport: tr, Timeout: 5 * time.Second}
}

func doRequest(t *testing.T, hc *http.Client, method, url string, headers map[string]string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, body
}

func varySet(t *testing.T, resp *http.Response) map[string]bool {
	t.Helper()
	values := resp.Header.Values("Vary")
	if len(values) == 0 {
		return map[string]bool{}
	}
	set := make(map[string]bool)
	for _, v := range values {
		for _, item := range strings.Split(v, ",") {
			set[strings.ToLower(strings.TrimSpace(item))] = true
		}
	}
	return set
}

func TestNegotiation_RealServer_QWildcardAndParams(t *testing.T) {
	f := newNegotiateFixture(t)
	h, ln := f.startServer(t)
	defer ln.Close()
	defer h.Close()
	hc := negotiateHTTPClient()
	u := "http://" + ln.Addr().String() + "/r"

	// Explicit range outranks the wildcard despite ordering.
	resp, body := doRequest(t, hc, "GET", u, map[string]string{
		"Accept": "*/*;q=0.1, application/xml;q=0.9",
	})
	if resp.StatusCode != consts.StatusOK || string(body) != string(f.xmlBody) {
		t.Fatalf("xml case: status=%d body=%q", resp.StatusCode, body)
	}
	if resp.Header.Get("Etag") != etagXML {
		t.Fatalf("xml etag=%q", resp.Header.Get("Etag"))
	}

	// type/* wildcard and concrete type with q.
	resp, body = doRequest(t, hc, "GET", u, map[string]string{
		"Accept":          "application/*;q=0.1, application/json;q=0.9",
		"Accept-Encoding": "identity",
	})
	if resp.StatusCode != consts.StatusOK || string(body) != string(f.plainBody) {
		t.Fatalf("json case: status=%d body=%q", resp.StatusCode, body)
	}

	// Media type parameter selects the versioned representation.
	resp, body = doRequest(t, hc, "GET", u, map[string]string{
		"Accept": "application/json; version=2",
	})
	if resp.StatusCode != consts.StatusOK || string(body) != string(f.v2Body) {
		t.Fatalf("v2 case: status=%d body=%q", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "version=2") {
		t.Fatalf("v2 content-type=%q", ct)
	}

	// A constrained charset that no representation carries falls through
	// to xml instead of returning json.
	resp, _ = doRequest(t, hc, "GET", u, map[string]string{
		"Accept": `application/json; charset="us-ascii", application/xml`,
	})
	if resp.StatusCode != consts.StatusOK || resp.Header.Get("Etag") != etagXML {
		t.Fatalf("charset fallthrough: status=%d etag=%q", resp.StatusCode, resp.Header.Get("Etag"))
	}

	// Nothing acceptable.
	resp, _ = doRequest(t, hc, "GET", u, map[string]string{"Accept": "image/webp"})
	if resp.StatusCode != consts.StatusNotAcceptable {
		t.Fatalf("406 case: status=%d", resp.StatusCode)
	}
}

func TestNegotiation_RealServer_CodingPriorityAndETags(t *testing.T) {
	f := newNegotiateFixture(t)
	h, ln := f.startServer(t)
	defer ln.Close()
	defer h.Close()
	hc := negotiateHTTPClient()
	u := "http://" + ln.Addr().String() + "/r"

	// gzip explicitly preferred; the coded body has its own validator and
	// length, never the strong ETag of the raw JSON.
	before := f.snapshot()
	resp, body := doRequest(t, hc, "GET", u, map[string]string{
		"Accept":          "application/json",
		"Accept-Encoding": "gzip, identity",
	})
	after := f.snapshot()
	if resp.StatusCode != consts.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("encoding=%q", resp.Header.Get("Content-Encoding"))
	}
	if resp.Header.Get("Etag") != etagJSONGzip {
		t.Fatalf("etag=%q, coded bytes must not reuse the raw JSON validator", resp.Header.Get("Etag"))
	}
	if int(resp.ContentLength) != len(f.gzipBody) {
		t.Fatalf("content-length=%d, want coded length %d", resp.ContentLength, len(f.gzipBody))
	}
	if len(body) != len(f.gzipBody) {
		t.Fatalf("wire body length=%d", len(body))
	}
	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := ioutil.ReadAll(zr)
	if err != nil || !bytes.Equal(plain, f.plainBody) {
		t.Fatalf("gunzip mismatch: plain=%q err=%v", plain, err)
	}
	if after.gzip-before.gzip != 1 || after.plain-before.plain != 0 {
		t.Fatalf("renderer call delta gzip=%d plain=%d, want exactly one selected render",
			after.gzip-before.gzip, after.plain-before.plain)
	}

	// identity explicitly refused but gzip allowed -> gzip is selected.
	resp, _ = doRequest(t, hc, "GET", u, map[string]string{
		"Accept":          "application/json",
		"Accept-Encoding": "identity;q=0, gzip",
	})
	if resp.StatusCode != consts.StatusOK || resp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("identity refused: status=%d encoding=%q", resp.StatusCode, resp.Header.Get("Content-Encoding"))
	}

	// gzip refused via q=0 (not string matching), identity is served.
	resp, body = doRequest(t, hc, "GET", u, map[string]string{
		"Accept":          "application/json",
		"Accept-Encoding": "gzip;q=0",
	})
	if resp.StatusCode != consts.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if ce := resp.Header.Get("Content-Encoding"); ce != "" {
		t.Fatalf("identity response must not carry Content-Encoding, got %q", ce)
	}
	if resp.Header.Get("Etag") != etagJSON || !bytes.Equal(body, f.plainBody) {
		t.Fatalf("identity etag/body mismatch")
	}

	// Everything refused.
	resp, _ = doRequest(t, hc, "GET", u, map[string]string{
		"Accept":          "application/json",
		"Accept-Encoding": "gzip;q=0, identity;q=0",
	})
	if resp.StatusCode != consts.StatusNotAcceptable {
		t.Fatalf("status=%d, want 406", resp.StatusCode)
	}

	// Missing Accept-Encoding: anything is acceptable, server weight
	// prefers the gzip variant.
	resp, _ = doRequest(t, hc, "GET", u, map[string]string{"Accept": "application/json"})
	if resp.StatusCode != consts.StatusOK || resp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("missing header: status=%d encoding=%q", resp.StatusCode, resp.Header.Get("Content-Encoding"))
	}
}

func TestNegotiation_RealServer_ConditionalHead(t *testing.T) {
	f := newNegotiateFixture(t)
	h, ln := f.startServer(t)
	defer ln.Close()
	defer h.Close()
	hc := negotiateHTTPClient()
	u := "http://" + ln.Addr().String() + "/r"
	gzipHeaders := map[string]string{
		"Accept":          "application/json",
		"Accept-Encoding": "gzip",
	}

	// Conditional GET matching the selected coded representation -> 304.
	before := f.snapshot()
	resp, body := doRequest(t, hc, "GET", u, mergeHeaders(gzipHeaders, map[string]string{
		"If-None-Match": etagJSONGzip,
	}))
	after := f.snapshot()
	if resp.StatusCode != consts.StatusNotModified {
		t.Fatalf("GET status=%d, want 304", resp.StatusCode)
	}
	if len(body) != 0 {
		t.Fatalf("304 body=%q", body)
	}
	if resp.Header.Get("Etag") != etagJSONGzip {
		t.Fatalf("304 etag=%q", resp.Header.Get("Etag"))
	}
	if resp.Header.Get("Content-Encoding") != "" {
		t.Fatalf("304 must not describe a transferred entity coding, got %q", resp.Header.Get("Content-Encoding"))
	}
	if after.gzip != before.gzip {
		t.Fatal("304 GET must not invoke the body renderer")
	}

	// Weak comparison: a weak marker matches the strong selected validator.
	resp, _ = doRequest(t, hc, "GET", u, mergeHeaders(gzipHeaders, map[string]string{
		"If-None-Match": `W/` + etagJSONGzip,
	}))
	if resp.StatusCode != consts.StatusNotModified {
		t.Fatalf("weak INM status=%d, want 304", resp.StatusCode)
	}

	// A validator for a non-selected representation does not match.
	resp, body = doRequest(t, hc, "GET", u, mergeHeaders(gzipHeaders, map[string]string{
		"If-None-Match": etagJSON,
	}))
	if resp.StatusCode != consts.StatusOK || !bytes.Equal(body, f.gzipBody) {
		t.Fatalf("mismatched INM: status=%d bodyLen=%d", resp.StatusCode, len(body))
	}

	// Conditional HEAD matching -> 304 with no renderer call.
	before = f.snapshot()
	resp, _ = doRequest(t, hc, "HEAD", u, mergeHeaders(gzipHeaders, map[string]string{
		"If-None-Match": etagJSONGzip,
	}))
	after = f.snapshot()
	if resp.StatusCode != consts.StatusNotModified {
		t.Fatalf("HEAD status=%d, want 304", resp.StatusCode)
	}
	if after.gzip != before.gzip {
		t.Fatal("304 HEAD must not invoke the body renderer")
	}

	// Plain HEAD 200: metadata length without rendering.
	before = f.snapshot()
	resp, _ = doRequest(t, hc, "HEAD", u, gzipHeaders)
	after = f.snapshot()
	if resp.StatusCode != consts.StatusOK {
		t.Fatalf("HEAD 200 status=%d", resp.StatusCode)
	}
	if int(resp.ContentLength) != len(f.gzipBody) {
		t.Fatalf("HEAD content-length=%d, want coded length %d", resp.ContentLength, len(f.gzipBody))
	}
	if resp.Header.Get("Content-Encoding") != "gzip" || resp.Header.Get("Etag") != etagJSONGzip {
		t.Fatalf("HEAD headers enc=%q etag=%q", resp.Header.Get("Content-Encoding"), resp.Header.Get("Etag"))
	}
	if after.gzip != before.gzip {
		t.Fatal("HEAD 200 must not invoke the body renderer")
	}

	// POST with a matching validator must not be answered 304.
	before = f.snapshot()
	resp, body = doRequest(t, hc, "POST", u, mergeHeaders(map[string]string{
		"Accept":          "application/json",
		"Accept-Encoding": "identity",
	}, map[string]string{"If-None-Match": etagJSON}))
	after = f.snapshot()
	if resp.StatusCode != consts.StatusOK {
		t.Fatalf("POST status=%d, other methods must not get 304", resp.StatusCode)
	}
	if !bytes.Equal(body, f.plainBody) || after.plain-before.plain != 1 {
		t.Fatalf("POST must render the identity entity once, delta=%d", after.plain-before.plain)
	}
}

func TestNegotiation_RealServer_VaryMerge(t *testing.T) {
	f := newNegotiateFixture(t)
	h, ln := f.startServer(t)
	defer ln.Close()
	defer h.Close()
	hc := negotiateHTTPClient()

	resp, _ := doRequest(t, hc, "GET", "http://"+ln.Addr().String()+"/vary", map[string]string{
		"Accept": "application/xml",
	})
	set := varySet(t, resp)
	for _, want := range []string{"accept", "accept-encoding", "x-debug", "x-other"} {
		if !set[want] {
			t.Fatalf("Vary merged set=%v missing %q", set, want)
		}
	}
	if len(resp.Header.Values("Vary")) != 1 {
		t.Fatalf("expected a single merged Vary header, got %v", resp.Header.Values("Vary"))
	}

	// Existing wildcard must survive untouched.
	resp, _ = doRequest(t, hc, "GET", "http://"+ln.Addr().String()+"/varystar", nil)
	if got := resp.Header.Get("Vary"); got != "*" {
		t.Fatalf("Vary=%q, want *", got)
	}
}

func TestNegotiation_RealServer_KeepAliveNoStateLeak(t *testing.T) {
	f := newNegotiateFixture(t)
	h, ln := f.startServer(t)
	defer ln.Close()
	defer h.Close()

	conn, err := net.DialTimeout("tcp", ln.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Two sequential requests on the SAME connection with different
	// negotiation inputs; the responses must not cross over.
	cases := []struct {
		acceptEncoding string
		ifNoneMatch    string
		wantStatus     int
		wantEncoding   string
		wantEtag       string
		wantBody       []byte
	}{
		{
			acceptEncoding: "identity",
			ifNoneMatch:    etagJSON,
			wantStatus:     consts.StatusNotModified,
			wantEtag:       etagJSON,
		},
		{
			acceptEncoding: "gzip",
			wantStatus:     consts.StatusOK,
			wantEncoding:   "gzip",
			wantEtag:       etagJSONGzip,
			wantBody:       f.gzipBody,
		},
	}

	reader := bufio.NewReader(conn)
	for i, tc := range cases {
		req := "GET /r HTTP/1.1\r\nHost: example\r\nAccept: application/json\r\nAccept-Encoding: " +
			tc.acceptEncoding + "\r\n"
		if tc.ifNoneMatch != "" {
			req += "If-None-Match: " + tc.ifNoneMatch + "\r\n"
		}
		req += "\r\n"
		if _, err := conn.Write([]byte(req)); err != nil {
			t.Fatal(err)
		}

		statusLine, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		headers := map[string]string{}
		var contentLength int
		connection := ""
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			line = strings.TrimRight(line, "\r\n")
			if line == "" {
				break
			}
			idx := strings.IndexByte(line, ':')
			if idx < 0 {
				t.Fatalf("malformed header line %q", line)
			}
			k := strings.TrimSpace(line[:idx])
			v := strings.TrimSpace(line[idx+1:])
			switch strings.ToLower(k) {
			case "content-length":
				fmt.Sscanf(v, "%d", &contentLength)
			case "connection":
				connection = strings.ToLower(v)
			}
			headers[k] = v
		}
		body := make([]byte, contentLength)
		if _, err := io.ReadFull(reader, body); err != nil {
			t.Fatal(err)
		}

		if !strings.Contains(statusLine, fmt.Sprintf(" %d ", tc.wantStatus)) {
			t.Fatalf("request %d status=%q want %d", i, statusLine, tc.wantStatus)
		}
		if connection == "close" {
			t.Fatalf("request %d unexpectedly closed the connection", i)
		}
		if tc.wantEncoding == "" {
			if _, ok := headers["Content-Encoding"]; ok {
				t.Fatalf("request %d unexpected Content-Encoding", i)
			}
		} else if headers["Content-Encoding"] != tc.wantEncoding {
			t.Fatalf("request %d encoding=%q want %q", i, headers["Content-Encoding"], tc.wantEncoding)
		}
		if headers["Etag"] != tc.wantEtag {
			t.Fatalf("request %d etag=%q want %q", i, headers["Etag"], tc.wantEtag)
		}
		if tc.wantBody != nil && !bytes.Equal(body, tc.wantBody) {
			t.Fatalf("request %d body mismatch", i)
		}
	}
}

func TestNegotiation_RealServer_ConcurrentRace(t *testing.T) {
	f := newNegotiateFixture(t)
	h, ln := f.startServer(t)
	defer ln.Close()
	defer h.Close()
	hc := negotiateHTTPClient()
	u := "http://" + ln.Addr().String() + "/r"

	var wg sync.WaitGroup
	var bad int32
	scenarios := []func() (int, string){
		func() (int, string) {
			resp, _ := doRequest(t, hc, "GET", u, map[string]string{
				"Accept": "application/json", "Accept-Encoding": "gzip",
			})
			return resp.StatusCode, resp.Header.Get("Etag")
		},
		func() (int, string) {
			resp, _ := doRequest(t, hc, "GET", u, map[string]string{
				"Accept": "application/json", "Accept-Encoding": "identity",
			})
			return resp.StatusCode, resp.Header.Get("Etag")
		},
		func() (int, string) {
			resp, _ := doRequest(t, hc, "GET", u, map[string]string{
				"Accept": "application/json", "Accept-Encoding": "gzip",
				"If-None-Match": etagJSONGzip,
			})
			return resp.StatusCode, resp.Header.Get("Etag")
		},
		func() (int, string) {
			resp, _ := doRequest(t, hc, "HEAD", u, map[string]string{
				"Accept": "application/json", "Accept-Encoding": "gzip",
			})
			return resp.StatusCode, resp.Header.Get("Etag")
		},
		func() (int, string) {
			resp, _ := doRequest(t, hc, "GET", u, map[string]string{
				"Accept": "image/webp",
			})
			return resp.StatusCode, ""
		},
	}
	want := []struct {
		status int
		etag   string
	}{
		{consts.StatusOK, etagJSONGzip},
		{consts.StatusOK, etagJSON},
		{consts.StatusNotModified, etagJSONGzip},
		{consts.StatusOK, etagJSONGzip},
		{consts.StatusNotAcceptable, ""},
	}
	for g := 0; g < 20; g++ {
		for s, sc := range scenarios {
			wg.Add(1)
			go func(s int, sc func() (int, string)) {
				defer wg.Done()
				status, etag := sc()
				if status != want[s].status || etag != want[s].etag {
					t.Errorf("scenario %d got (%d,%q) want (%d,%q)", s, status, etag, want[s].status, want[s].etag)
					atomic.AddInt32(&bad, 1)
				}
			}(s, sc)
		}
	}
	wg.Wait()
	if atomic.LoadInt32(&bad) != 0 {
		t.Fatalf("%d concurrent negotiations leaked across requests", bad)
	}

	// Every successful coded GET renders once; HEAD and 304 render nothing.
	snap := f.snapshot()
	if snap.gzip != 20 {
		t.Fatalf("gzip renderer calls=%d, want 20 (one per coded 200 GET only)", snap.gzip)
	}
	if snap.plain != 20 {
		t.Fatalf("plain renderer calls=%d, want 20", snap.plain)
	}
}

func mergeHeaders(a, b map[string]string) map[string]string {
	out := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}
