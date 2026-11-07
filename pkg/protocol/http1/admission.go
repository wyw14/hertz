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

	"github.com/cloudwego/hertz/pkg/network"
	"github.com/cloudwego/hertz/pkg/protocol"
)

// Permit represents a single route-level concurrency slot acquired through an
// AdmissionController.
//
// A permit covers the COMPLETE HTTP exchange of one request: the handler
// chain, pumping a streaming request/response body and the final flush.
// Implementations MUST make Release idempotent so that write failures,
// panics, timeouts, engine shutdown and context-pool resets cannot release
// the permit twice or transfer it to another request.
type Permit interface {
	// Release returns the slot to the generation that issued the permit.
	// It must be callable exactly once; further calls are no-ops.
	Release()
}

// Admission is the result of AdmissionController.Acquire.
//
// Exactly one of the following holds:
//   - Rejected is true: the request must not be handled and a rejection
//     response (or a silent close when Canceled) is produced;
//   - Permit is non-nil: the request was admitted and Permit.Release must be
//     called after the full exchange;
//   - Rejected is false and Permit is nil: no admission policy matched the
//     route, the request proceeds as usual.
type Admission struct {
	// Permit is the acquired slot, non-nil iff the request is admitted.
	Permit Permit

	// Rejected indicates that the request is rejected (queue full, wait
	// deadline exceeded, queue body budget exhausted or engine shutting down).
	Rejected bool

	// StatusCode is the HTTP status used for the rejection response.
	StatusCode int

	// Body optionally carries the rejection response body.
	Body []byte

	// MaxDrainBytes bounds how many bytes of the not-yet-read request body are
	// consumed so that the keep-alive connection can serve the next request.
	// When the body cannot be fully drained within this budget the server
	// closes the connection instead.
	MaxDrainBytes int64

	// Canceled indicates that the request left the queue because its context
	// was canceled or the connection was observed closed. No response is
	// guaranteed to reach the peer, so the connection is closed directly.
	Canceled bool
}

// AdmissionController is implemented by the engine. The HTTP/1.1 server calls
// Acquire after the request headers are parsed and before the request body is
// read and before the handler chain runs.
//
// Acquire MUST NOT perform any reads from the connection or its reader:
// waiting requests must not race the protocol layer for the same Reader.
// Peer disconnection is observed solely through ctx cancellation or later
// I/O errors; instant detection without I/O is not promised.
type AdmissionController interface {
	Acquire(ctx context.Context, conn network.Conn, req *protocol.Request) Admission
}

// drainUnreadRequestBody consumes a request body that was never read because
// the request was rejected while queued, so the following keep-alive request
// cannot see stale body bytes.
//
// It returns true iff the whole body was consumed within maxDrain bytes and
// no read error occurred. It is the sole reader of the connection while
// running, and releases the reader buffer after each skip so a large body
// cannot accumulate in memory.
//
// Bodies with unknown framing (chunked transfer-encoding) cannot be skipped
// with a strict byte budget, so the caller must close the connection for
// those.
func drainUnreadRequestBody(zr network.Reader, req *protocol.Request, maxDrain int64) bool {
	contentLength := req.Header.ContentLength()

	// -2: no request body; 0: empty body.
	if contentLength <= 0 {
		return contentLength != -1
	}
	if maxDrain <= 0 || int64(contentLength) > maxDrain {
		return false
	}

	remaining := int64(contentLength)
	for remaining > 0 {
		ready := int64(zr.Len())
		if ready == 0 {
			// Honors the request read timeout; a peer that never sends the
			// advertised body makes Peek fail and the connection gets closed.
			if _, err := zr.Peek(1); err != nil {
				return false
			}
			ready = int64(zr.Len())
		}
		if ready <= 0 {
			return false
		}
		n := ready
		if n > remaining {
			n = remaining
		}
		if err := zr.Skip(int(n)); err != nil {
			return false
		}
		// Free read buffer so a big body is not buffered unbounded.
		if err := zr.Release(); err != nil {
			return false
		}
		remaining -= n
	}
	return true
}
