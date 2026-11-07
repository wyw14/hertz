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
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"net/url"
	"sort"

	"github.com/cloudwego/hertz/pkg/app"
)

// errBodyTooLarge signals that the complete request body could not be
// obtained within the configured fingerprint byte budget. Such requests are
// rejected before the handler ever runs.
var errBodyTooLarge = errors.New("idempotency: request body exceeds fingerprint budget")

// errBodyRead signals a transport-level failure while fully reading a
// streaming request body for fingerprinting.
var errBodyRead = errors.New("idempotency: cannot read request body for fingerprint")

// fingerprint computes a stable digest over:
//   - the request method;
//   - the canonical target: normalized path plus query parameters ordered by
//     (key, value), so reordering identical query parameters never changes
//     the fingerprint;
//   - every caller-designated business header (name plus all its values, in
//     wire order). Unlisted headers - including hop-by-hop and credential
//     headers - are deliberately not part of the fingerprint;
//   - a digest of the complete request body.
//
// All sections are length-prefixed so that concatenation can never be
// ambiguous.
func (m *Idempotency) fingerprint(ctx *app.RequestContext) ([]byte, error) {
	h := sha256.New()

	writeField(h, ctx.Method())

	// Canonical target: normalized request path + canonicalized query.
	writeField(h, ctx.URI().Path())
	type kv struct{ k, v string }
	q := make([]kv, 0, ctx.URI().QueryArgs().Len())
	ctx.URI().QueryArgs().VisitAll(func(key, value []byte) {
		q = append(q, kv{k: url.QueryEscape(string(key)), v: url.QueryEscape(string(value))})
	})
	sort.Slice(q, func(i, j int) bool {
		if q[i].k != q[j].k {
			return q[i].k < q[j].k
		}
		return q[i].v < q[j].v
	})
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(q)))
	h.Write(count[:])
	for _, p := range q {
		writeField(h, []byte(p.k))
		writeField(h, []byte(p.v))
	}

	// Designated business request headers only.
	binary.BigEndian.PutUint32(count[:], uint32(len(m.cfg.businessHeaderNames)))
	h.Write(count[:])
	for _, name := range m.cfg.businessHeaderNames {
		writeField(h, []byte(name))
		// PeekAll reuses an internal buffer, so values must be consumed
		// synchronously - hashing them here is safe.
		values := ctx.Request.Header.PeekAll(name)
		binary.BigEndian.PutUint32(count[:], uint32(len(values)))
		h.Write(count[:])
		for _, v := range values {
			writeField(h, v)
		}
	}

	// Complete body digest. Streaming bodies are admitted only when the
	// whole body can be materialized within the explicit byte budget.
	if ctx.Request.IsBodyStream() {
		body, err := m.readStreamBodyWithinBudget(ctx)
		if err != nil {
			return nil, err
		}
		writeField(h, body)
	} else {
		body, err := ctx.Request.BodyE()
		if err != nil {
			return nil, errBodyRead
		}
		if len(body) > m.cfg.maxRequestBodyBytes {
			return nil, errBodyTooLarge
		}
		writeField(h, body)
	}

	return h.Sum(nil), nil
}

// readStreamBodyWithinBudget fully reads a streaming request body, rejecting
// as soon as it exceeds maxRequestBodyBytes+1 bytes. On success the consumed
// body is installed back as a buffered body (Request.SetBodyRaw closes the
// consumed stream), so the downstream handler reads an ordinary buffered body.
func (m *Idempotency) readStreamBodyWithinBudget(ctx *app.RequestContext) ([]byte, error) {
	if cl := ctx.Request.Header.ContentLength(); cl > m.cfg.maxRequestBodyBytes {
		return nil, errBodyTooLarge
	}

	budget := m.cfg.maxRequestBodyBytes
	stream := ctx.Request.BodyStream()

	// Grow in chunks so a small request never allocates the whole budget,
	// while reading at most budget+1 bytes to prove the bound.
	chunk := make([]byte, 32*1024)
	if chunkSize := budget + 1; len(chunk) > chunkSize {
		chunk = make([]byte, chunkSize)
	}
	body := make([]byte, 0, 256)
	limited := io.LimitReader(stream, int64(budget)+1)
	for {
		n, err := limited.Read(chunk)
		if n > 0 {
			if len(body)+n > budget {
				return nil, errBodyTooLarge
			}
			body = append(body, chunk[:n]...)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, errBodyRead
		}
	}

	// Install the buffered copy; SetBodyRaw resets/closes the consumed
	// stream. The slice is owned by the request from here on.
	ctx.Request.SetBodyRaw(body)
	return body, nil
}

func writeField(h hash.Hash, b []byte) {
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(b)))
	h.Write(l[:])
	h.Write(b)
}
