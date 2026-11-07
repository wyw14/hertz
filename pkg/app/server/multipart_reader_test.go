/*
 * Copyright 2022 CloudWeGo Authors
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
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/hertz/internal/testutils"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/test/assert"
	"github.com/cloudwego/hertz/pkg/protocol"
)

// buildBody builds a canonical multipart/form-data body and returns it
// together with the boundary.
func buildBody(t *testing.T, boundary string, build func(w *multipart.Writer)) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	if err := w.SetBoundary(boundary); err != nil {
		t.Fatalf("set boundary: %v", err)
	}
	build(w)
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	return buf.Bytes()
}

func multipartTempFileSet() map[string]struct{} {
	set := map[string]struct{}{}
	entries, _ := os.ReadDir(os.TempDir())
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "multipart-") {
			set[e.Name()] = struct{}{}
		}
	}
	return set
}

// connIO bundles one raw TCP connection with a buffered reader reused
// across pipelined/sequential requests.
type connIO struct {
	t   *testing.T
	c   net.Conn
	br  *bufio.Reader
	buf []byte
}

func dial(t *testing.T, addr string) *connIO {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return &connIO{t: t, c: c, br: bufio.NewReader(c)}
}

func (io2 *connIO) send(text string, body []byte) {
	io2.t.Helper()
	if err := io2.c.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		io2.t.Fatalf("write deadline: %v", err)
	}
	if _, err := io2.c.Write([]byte(text)); err != nil {
		io2.t.Fatalf("write header: %v", err)
	}
	if len(body) > 0 {
		if _, err := io2.c.Write(body); err != nil {
			io2.t.Fatalf("write body: %v", err)
		}
	}
}

func (io2 *connIO) sendChunked(head string, body []byte, chunkSize int) {
	io2.t.Helper()
	if err := io2.c.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		io2.t.Fatalf("write deadline: %v", err)
	}
	if _, err := io2.c.Write([]byte(head)); err != nil {
		io2.t.Fatalf("write head: %v", err)
	}
	for off := 0; off < len(body); off += chunkSize {
		end := off + chunkSize
		if end > len(body) {
			end = len(body)
		}
		fmt.Fprintf(io2.c, "%x\r\n", end-off)
		io2.c.Write(body[off:end])
		io2.c.Write([]byte("\r\n"))
	}
	io2.c.Write([]byte("0\r\n\r\n"))
}

func (io2 *connIO) readResponse() *http.Response {
	io2.t.Helper()
	_ = io2.c.SetReadDeadline(time.Now().Add(10 * time.Second))
	resp, err := http.ReadResponse(io2.br, nil)
	if err != nil {
		io2.t.Fatalf("read response: %v", err)
	}
	return resp
}

// tryReadResponse is like readResponse but returns the error instead of
// failing, to assert a closed connection.
func (io2 *connIO) tryReadResponse() (*http.Response, error) {
	_ = io2.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	return http.ReadResponse(io2.br, nil)
}

func (io2 *connIO) close() {
	_ = io2.c.Close()
}

// closeWrite half-closes the send side (TCP FIN) while keeping the
// receive side open. It lets the server observe a truncated request body
// without the client discarding a still-open write side.
func (io2 *connIO) closeWrite() {
	io2.t.Helper()
	tcp, ok := io2.c.(*net.TCPConn)
	if !ok {
		io2.t.Fatalf("not a tcp connection: %T", io2.c)
	}
	if err := tcp.CloseWrite(); err != nil {
		io2.t.Fatalf("close write: %v", err)
	}
}

func startMultipartReaderServer(t *testing.T, extra ...config.Option) (*Hertz, net.Listener) {
	t.Helper()
	ln := testutils.NewTestListener(t)
	opts := []config.Option{
		WithListener(ln),
		WithStreamBody(true),
		WithDisablePreParseMultipartForm(true),
	}
	opts = append(opts, extra...)
	h := New(opts...)

	h.POST("/upload", func(c context.Context, ctx *app.RequestContext) {
		mr, err := ctx.MultipartReader()
		if err != nil {
			ctx.String(http.StatusInternalServerError, "reader: %v", err)
			return
		}
		files, values := 0, 0
		for {
			p, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				ctx.String(http.StatusInternalServerError, "next: %v", err)
				return
			}
			n, err := io.Copy(io.Discard, p)
			if err != nil {
				ctx.String(http.StatusInternalServerError, "read part %d: %v", p.PartNumber, err)
				return
			}
			if p.IsFile {
				files++
			} else {
				values++
			}
			_ = n
		}
		if err := mr.Close(); err != nil {
			ctx.String(http.StatusInternalServerError, "close: %v", err)
			return
		}
		ctx.String(http.StatusOK, "ok bytes=%d files=%d values=%d close=%v",
			mr.BytesRead(), files, values, mr.MustCloseConnection())
	})

	h.GET("/ping", func(c context.Context, ctx *app.RequestContext) {
		ctx.String(http.StatusOK, "pong")
	})

	h.POST("/abort", func(c context.Context, ctx *app.RequestContext) {
		mr, err := ctx.MultipartReader(&protocol.MultipartReaderOptions{
			MaxParts:          -1,
			MaxValueBytes:     -1,
			MaxFileBytes:      -1,
			MaxTotalFileBytes: -1,
			MaxDrainBytes:     64,
		})
		if err != nil {
			ctx.String(http.StatusInternalServerError, "reader: %v", err)
			return
		}
		p, err := mr.NextPart()
		if err != nil {
			ctx.String(http.StatusInternalServerError, "next: %v", err)
			return
		}
		if _, err := io.Copy(io.Discard, p); err != nil {
			ctx.String(http.StatusInternalServerError, "read: %v", err)
			return
		}
		// The second (large) part is intentionally abandoned. Both a
		// drain-budget overflow and a truncated body must be reported
		// as typed errors and force connection closure.
		closeErr := mr.Close()
		if !errors.Is(closeErr, protocol.ErrMultipartDrainExceeded) &&
			!errors.Is(closeErr, protocol.ErrMultipartTruncated) {
			ctx.String(http.StatusInternalServerError, "want close error, got %v", closeErr)
			return
		}
		if !mr.MustCloseConnection() {
			ctx.String(http.StatusInternalServerError, "want must-close")
			return
		}
		ctx.String(http.StatusOK, "aborted")
	})

	go h.Spin()
	waitEngineRunning(h)
	return h, ln
}

// TestMultipartReaderE2E_KeepAliveAfterFullConsume verifies that after a
// full, streaming multipart consumption the same TCP connection serves a
// second request, the reported consumed bytes match the real body, and
// no temporary files are created.
func TestMultipartReaderE2E_KeepAliveAfterFullConsume(t *testing.T) {
	h, ln := startMultipartReaderServer(t)
	defer h.Close()
	defer ln.Close()

	boundary := "E2EBoundary"
	body := buildBody(t, boundary, func(w *multipart.Writer) {
		if err := w.WriteField("field", "value"); err != nil {
			t.Fatal(err)
		}
		must(t, w.WriteField("field2", strings.Repeat("v", 3000)))
		hdr := protocol.CreateMultipartHeader("file", "a.txt", "text/plain")
		pw, err := w.CreatePart(hdr)
		must(t, err)
		_, err = pw.Write(bytes.Repeat([]byte{'a'}, 4200))
		must(t, err)
	})

	before := multipartTempFileSet()

	c := dial(t, ln.Addr().String())
	defer c.close()

	req1 := fmt.Sprintf("POST /upload HTTP/1.1\r\nHost: x\r\nContent-Type: multipart/form-data; boundary=%s\r\nContent-Length: %d\r\nConnection: keep-alive\r\n\r\n",
		boundary, len(body))
	c.send(req1, body)
	resp := c.readResponse()
	assertStatus(t, resp, http.StatusOK)
	// Go's http.ReadResponse folds "Connection: close" into Close.
	assert.Assert(t, !resp.Close)
	b1, _ := io.ReadAll(resp.Body)
	want := fmt.Sprintf("ok bytes=%d files=1 values=2 close=false", len(body))
	assert.DeepEqual(t, want, string(b1))

	// Second request on the very same connection: keep-alive works.
	c.send("GET /ping HTTP/1.1\r\nHost: x\r\nConnection: keep-alive\r\n\r\n", nil)
	resp2 := c.readResponse()
	assertStatus(t, resp2, http.StatusOK)
	b2, _ := io.ReadAll(resp2.Body)
	assert.DeepEqual(t, "pong", string(b2))

	// No temp files may appear: the streaming reader never spools files.
	after := multipartTempFileSet()
	for name := range after {
		if _, had := before[name]; !had {
			t.Fatalf("unexpected multipart temp file created: %s", name)
		}
	}
}

// TestMultipartReaderE2E_ChunkedUnknownLength reuses a connection after
// fully consuming a chunked (unknown Content-Length) multipart body sent
// in very small network chunks.
func TestMultipartReaderE2E_ChunkedUnknownLength(t *testing.T) {
	h, ln := startMultipartReaderServer(t)
	defer h.Close()
	defer ln.Close()

	boundary := "ChunkedB"
	body := buildBody(t, boundary, func(w *multipart.Writer) {
		must(t, w.WriteField("k", "v"))
		hdr := protocol.CreateMultipartHeader("f", "f.bin", "")
		pw, err := w.CreatePart(hdr)
		must(t, err)
		_, err = pw.Write(bytes.Repeat([]byte{0xAB}, 5000))
		must(t, err)
	})

	c := dial(t, ln.Addr().String())
	defer c.close()

	head := fmt.Sprintf("POST /upload HTTP/1.1\r\nHost: x\r\nContent-Type: multipart/form-data; boundary=%s\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n",
		boundary)
	c.sendChunked(head, body, 97) // tiny chunks split boundaries/headers
	resp := c.readResponse()
	assertStatus(t, resp, http.StatusOK)
	assert.Assert(t, !resp.Close)
	b1, _ := io.ReadAll(resp.Body)
	assert.DeepEqual(t, fmt.Sprintf("ok bytes=%d files=1 values=1 close=false", len(body)), string(b1))

	c.send("GET /ping HTTP/1.1\r\nHost: x\r\nConnection: keep-alive\r\n\r\n", nil)
	resp2 := c.readResponse()
	assertStatus(t, resp2, http.StatusOK)
	b2, _ := io.ReadAll(resp2.Body)
	assert.DeepEqual(t, "pong", string(b2))
}

// TestMultipartReaderE2E_EarlyCloseDrainsOrCloses verifies that when the
// handler abandons a large body beyond the drain budget, the server does
// not read an unbounded body for keep-alive but responds with
// "Connection: close" and closes the connection, so the next request on
// that connection fails.
//
// The client sends exactly as many bytes as the server prefetches for a
// streaming request body (see maxContentLengthInStream in ext) and then
// half-closes the send side, so no unread request byte remains in the
// socket: the server's FIN is graceful and the response is delivered
// deterministically regardless of OS reset semantics.
func TestMultipartReaderE2E_EarlyCloseDrainsOrCloses(t *testing.T) {
	h, ln := startMultipartReaderServer(t)
	defer h.Close()
	defer ln.Close()

	boundary := "AbortB"
	body := buildBody(t, boundary, func(w *multipart.Writer) {
		must(t, w.WriteField("small", "s"))
		hdr := protocol.CreateMultipartHeader("big", "big.bin", "")
		pw, err := w.CreatePart(hdr)
		must(t, err)
		_, err = pw.Write(bytes.Repeat([]byte{'z'}, 1<<20)) // 1 MiB
		must(t, err)
	})

	c := dial(t, ln.Addr().String())
	defer c.close()

	// Stream-mode prefetch window (ext.maxContentLengthInStream).
	const prefetch = 8 * 1024
	if len(body) <= prefetch {
		t.Fatalf("body too short for test: %d", len(body))
	}

	req1 := fmt.Sprintf("POST /abort HTTP/1.1\r\nHost: x\r\nContent-Type: multipart/form-data; boundary=%s\r\nContent-Length: %d\r\nConnection: keep-alive\r\n\r\n",
		boundary, len(body))
	// Headers plus exactly the prefetched prefix, which already contains
	// the first part, the second part header and plenty of big-file bytes.
	c.send(req1, body[:prefetch])
	c.closeWrite()

	resp := c.readResponse()
	assertStatus(t, resp, http.StatusOK)
	b, _ := io.ReadAll(resp.Body)
	assert.DeepEqual(t, "aborted", string(b))
	// The server explicitly tells the client it is closing. Go's
	// http.ReadResponse folds "Connection: close" into resp.Close.
	assert.Assert(t, resp.Close)

	// Nothing further can be read: the connection has been closed.
	if r, err := c.tryReadResponse(); err == nil {
		rb, _ := io.ReadAll(r.Body)
		t.Fatalf("expected connection to be closed, got response: %s", rb)
	}
}

// TestMultipartReaderE2E_PreParseMutexAndClassicCompat verifies that the
// reader is mutually exclusive with server-side pre-parsing (explicit
// typed error) while the classic MultipartForm/FormFile path keeps
// working when the new mode is not used.
func TestMultipartReaderE2E_PreParseMutexAndClassicCompat(t *testing.T) {
	ln := testutils.NewTestListener(t)
	defer ln.Close()
	// StreamBody enabled but pre-parse left ON (default).
	h := New(WithListener(ln), WithStreamBody(true))
	defer h.Close()

	h.POST("/reader", func(c context.Context, ctx *app.RequestContext) {
		_, err := ctx.MultipartReader(nil)
		if !errors.Is(err, protocol.ErrMultipartPreParsed) {
			ctx.String(http.StatusInternalServerError, "want preparsed error, got %v", err)
			return
		}
		ctx.String(http.StatusPreconditionFailed, "preparsed")
	})
	h.POST("/classic", func(c context.Context, ctx *app.RequestContext) {
		mf, err := ctx.MultipartForm()
		if err != nil {
			ctx.String(http.StatusInternalServerError, "mf: %v", err)
			return
		}
		ctx.String(http.StatusOK, "classic=%s", mf.Value["k"][0])
	})

	go h.Spin()
	waitEngineRunning(h)

	body := buildBody(t, "PB", func(w *multipart.Writer) {
		must(t, w.WriteField("k", "v"))
	})

	c := dial(t, ln.Addr().String())
	defer c.close()

	req1 := fmt.Sprintf("POST /reader HTTP/1.1\r\nHost: x\r\nContent-Type: multipart/form-data; boundary=PB\r\nContent-Length: %d\r\nConnection: keep-alive\r\n\r\n", len(body))
	c.send(req1, body)
	resp := c.readResponse()
	assertStatus(t, resp, http.StatusPreconditionFailed)
	b1, _ := io.ReadAll(resp.Body)
	assert.DeepEqual(t, "preparsed", string(b1))

	// Same connection: classic pre-parsed API still works.
	req2 := fmt.Sprintf("POST /classic HTTP/1.1\r\nHost: x\r\nContent-Type: multipart/form-data; boundary=PB\r\nContent-Length: %d\r\nConnection: keep-alive\r\n\r\n", len(body))
	c.send(req2, body)
	resp2 := c.readResponse()
	assertStatus(t, resp2, http.StatusOK)
	b2, _ := io.ReadAll(resp2.Body)
	assert.DeepEqual(t, "classic=v", string(b2))
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func assertStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	if resp.StatusCode != want {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected status %d, want %d, body=%q", resp.StatusCode, want, b)
	}
}
