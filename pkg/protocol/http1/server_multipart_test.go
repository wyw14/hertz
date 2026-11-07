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

package http1

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime/multipart"
	"strings"
	"sync"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	errs "github.com/cloudwego/hertz/pkg/common/errors"
	"github.com/cloudwego/hertz/pkg/common/test/assert"
	"github.com/cloudwego/hertz/pkg/common/test/mock"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/protocol/http1/resp"
)

// countingReader counts the bytes the server really pulls from the
// request side of a mock connection.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func multipartMockServer(handler func(c context.Context, ctx *app.RequestContext)) *Server {
	server := NewServer()
	reqCtx := &app.RequestContext{}
	server.Core = &mockCore{
		ctxPool: &sync.Pool{New: func() interface{} {
			return reqCtx
		}},
		isRunning:   true,
		mockHandler: handler,
	}
	server.StreamRequestBody = true
	server.DisablePreParseMultipartForm = true
	return server
}

func buildRawMultipartRequest(t *testing.T, boundary string, build func(w *multipart.Writer)) (head string, body []byte, raw []byte) {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	assert.Nil(t, w.SetBoundary(boundary))
	build(w)
	assert.Nil(t, w.Close())
	body = buf.Bytes()
	head = "POST /upload HTTP/1.1\r\nHost: aaa\r\n" +
		"Content-Type: multipart/form-data; boundary=" + boundary + "\r\n" +
		"Content-Length: " + itoa(len(body)) + "\r\n\r\n"
	return head, body, append([]byte(head), body...)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func parseRecordedResponse(t *testing.T, conn *mock.Conn) *protocol.Response {
	t.Helper()
	response := protocol.AcquireResponse()
	assert.Nil(t, resp.Read(response, conn.WriterRecorder()))
	return response
}

// TestMultipartMock_BoundedDrainClosesConnection verifies the security
// guarantee of the bounded drain policy: when the handler abandons a
// large body beyond the drain budget, the server responds, asks for the
// connection to be closed and does NOT slurp the remaining body.
func TestMultipartMock_BoundedDrainClosesConnection(t *testing.T) {
	boundary := "MockB"
	_, _, raw := buildRawMultipartRequest(t, boundary, func(w *multipart.Writer) {
		assert.Nil(t, w.WriteField("small", "s"))
		h := protocol.CreateMultipartHeader("big", "big.bin", "")
		pw, err := w.CreatePart(h)
		assert.Nil(t, err)
		_, err = pw.Write(bytes.Repeat([]byte{'z'}, 1<<20))
		assert.Nil(t, err)
	})

	server := multipartMockServer(func(c context.Context, ctx *app.RequestContext) {
		mr, err := ctx.MultipartReader(&protocol.MultipartReaderOptions{
			MaxParts:          -1,
			MaxValueBytes:     -1,
			MaxFileBytes:      -1,
			MaxTotalFileBytes: -1,
			MaxDrainBytes:     64,
		})
		if err != nil {
			ctx.String(500, "reader: %v", err)
			return
		}
		p, err := mr.NextPart()
		if err != nil {
			ctx.String(500, "next: %v", err)
			return
		}
		if _, err := io.Copy(io.Discard, p); err != nil {
			ctx.String(500, "read: %v", err)
			return
		}
		if err := mr.Close(); !errors.Is(err, protocol.ErrMultipartDrainExceeded) {
			ctx.String(500, "want drain error, got %v", err)
			return
		}
		if !mr.MustCloseConnection() {
			ctx.String(500, "want must-close")
			return
		}
		ctx.String(200, "aborted")
	})

	cr := &countingReader{r: bytes.NewReader(raw)}
	conn := mock.NewConnWithReader(cr)

	err := server.Serve(context.TODO(), conn)
	assert.True(t, errors.Is(err, errs.ErrShortConnection))

	response := parseRecordedResponse(t, conn)
	assert.DeepEqual(t, 200, response.StatusCode())
	assert.DeepEqual(t, "close", string(response.Header.Peek("Connection")))
	assert.DeepEqual(t, "aborted", string(response.Body()))

	// The server must not have consumed the attack-sized body. It may
	// pull only the initial prefetch (a few KiB), the parsed headers,
	// the small first part and at most the 64-byte drain budget.
	t.Logf("server consumed %d of %d request bytes before closing", cr.n, len(raw))
	assert.Assert(t, cr.n < 64*1024, "server consumed %d bytes, expected a bounded amount", cr.n)
}

// TestMultipartMock_FullConsumeReusesConnection verifies the normal path:
// after full consumption the whole request is read, the response does
// not ask for closing and no body slurp remains.
func TestMultipartMock_FullConsumeReusesConnection(t *testing.T) {
	boundary := "MockB2"
	_, _, raw := buildRawMultipartRequest(t, boundary, func(w *multipart.Writer) {
		assert.Nil(t, w.WriteField("k", "v"))
		h := protocol.CreateMultipartHeader("f", "f.bin", "")
		pw, err := w.CreatePart(h)
		assert.Nil(t, err)
		_, err = pw.Write(bytes.Repeat([]byte{'a'}, 10*1024))
		assert.Nil(t, err)
	})

	server := multipartMockServer(func(c context.Context, ctx *app.RequestContext) {
		mr, err := ctx.MultipartReader(nil)
		if err != nil {
			ctx.String(500, "reader: %v", err)
			return
		}
		files := 0
		for {
			p, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				ctx.String(500, "next: %v", err)
				return
			}
			if _, err := io.Copy(io.Discard, p); err != nil {
				ctx.String(500, "read: %v", err)
				return
			}
			if p.IsFile {
				files++
			}
		}
		if err := mr.Close(); err != nil {
			ctx.String(500, "close: %v", err)
			return
		}
		ctx.String(200, "ok files=%d bytes=%d", files, mr.BytesRead())
	})

	cr := &countingReader{r: bytes.NewReader(raw)}
	conn := mock.NewConnWithReader(cr)

	// A single keep-alive request followed by EOF: Serve returns nil
	// after the connection goes idle.
	assert.Nil(t, server.Serve(context.TODO(), conn))

	response := parseRecordedResponse(t, conn)
	assert.DeepEqual(t, 200, response.StatusCode())
	connHeader := strings.ToLower(string(response.Header.Peek("Connection")))
	assert.Assert(t, connHeader != "close", "unexpected Connection: %q", connHeader)
	body := string(response.Body())
	assert.Assert(t, strings.HasPrefix(body, "ok files=1 bytes="), body)

	// Every request byte has been consumed, so nothing is left for a
	// keep-alive drain.
	assert.DeepEqual(t, int64(len(raw)), cr.n)
}
