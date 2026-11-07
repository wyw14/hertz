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

package protocol

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/textproto"
)

// Default limits of the restricted multipart reader. Zero-valued
// MultipartReaderOptions fields fall back to these defaults.
const (
	// defaultMultipartMaxParts is the default total number of parts
	// (including both regular fields and files).
	defaultMultipartMaxParts = 1000
	// defaultMultipartMaxPartHeaderBytes bounds the raw header block
	// (including the terminating blank line) of a single part.
	defaultMultipartMaxPartHeaderBytes = 8 * 1024
	// defaultMultipartMaxPartHeaderFields bounds the number of header
	// fields of a single part (continuation lines do not count).
	defaultMultipartMaxPartHeaderFields = 64
	// defaultMultipartMaxValueBytes bounds the aggregate decoded bytes
	// delivered from all regular (non-file) parts.
	defaultMultipartMaxValueBytes = 1 << 20
	// defaultMultipartMaxFileBytes bounds the bytes delivered from a
	// single file part.
	defaultMultipartMaxFileBytes = 32 << 20
	// defaultMultipartMaxTotalFileBytes bounds the aggregate bytes
	// delivered from all file parts.
	defaultMultipartMaxTotalFileBytes = 64 << 20
	// defaultMultipartMaxDrainBytes bounds how many request body bytes
	// the reader is allowed to discard on behalf of the caller when a
	// part is skipped or the reader is closed early.
	defaultMultipartMaxDrainBytes = 1 << 20

	// multipartUnlimited marks a disabled limit.
	multipartUnlimited = int64(math.MaxInt64)
	// multipartReaderBufferSize must be larger than the longest legal
	// boundary delimiter: CRLF (2) + "--" (2) + boundary (70) = 74.
	multipartReaderBufferSize = 4096
	// multipartDrainChunkSize is the scratch buffer used while draining.
	multipartDrainChunkSize = 16 * 1024
	// multipartMaxBoundaryLength follows RFC 2046 (bchars, 1..70).
	multipartMaxBoundaryLength = 70
)

// MultipartReaderOptions configures the restricted, streaming multipart
// reader.
//
// All limits are enforced against bytes actually delivered or discarded
// while the request body is read. The aggregate quotas (MaxValueBytes and
// MaxTotalFileBytes) are shared across all parts and across nested reads;
// they are never reset when moving to the next part. Limits also apply
// when the request Content-Length is unknown (chunked transfer encoding).
//
// A zero value falls back to the package defaults; set a field to -1 to
// disable the corresponding limit.
type MultipartReaderOptions struct {
	// MaxParts bounds the total number of parts in a request.
	MaxParts int

	// MaxPartHeaderBytes bounds the raw header bytes of a single part.
	MaxPartHeaderBytes int

	// MaxPartHeaderFields bounds the number of header fields of a part.
	MaxPartHeaderFields int

	// MaxValueBytes bounds the aggregate decoded bytes delivered from
	// regular (non-file) parts.
	MaxValueBytes int64

	// MaxFileBytes bounds the bytes delivered from a single file part.
	MaxFileBytes int64

	// MaxTotalFileBytes bounds the aggregate bytes delivered from all
	// file parts.
	MaxTotalFileBytes int64

	// MaxDrainBytes bounds the bytes discarded for the caller when a
	// part is skipped (NextPart before full consumption) or when the
	// reader is closed before the final boundary. If a remaining body
	// cannot be consumed within this budget, the reader stops reading,
	// reports ErrMultipartDrainExceeded and asks the server to close
	// the connection instead of draining an attacker-controlled body
	// without bounds for keep-alive.
	MaxDrainBytes int64
}

func (o *MultipartReaderOptions) normalize() {
	if o.MaxParts == 0 {
		o.MaxParts = defaultMultipartMaxParts
	} else if o.MaxParts < 0 {
		o.MaxParts = math.MaxInt32
	}
	if o.MaxPartHeaderBytes == 0 {
		o.MaxPartHeaderBytes = defaultMultipartMaxPartHeaderBytes
	} else if o.MaxPartHeaderBytes < 0 {
		o.MaxPartHeaderBytes = math.MaxInt32
	}
	if o.MaxPartHeaderFields == 0 {
		o.MaxPartHeaderFields = defaultMultipartMaxPartHeaderFields
	} else if o.MaxPartHeaderFields < 0 {
		o.MaxPartHeaderFields = math.MaxInt32
	}
	if o.MaxValueBytes == 0 {
		o.MaxValueBytes = defaultMultipartMaxValueBytes
	} else if o.MaxValueBytes < 0 {
		o.MaxValueBytes = multipartUnlimited
	}
	if o.MaxFileBytes == 0 {
		o.MaxFileBytes = defaultMultipartMaxFileBytes
	} else if o.MaxFileBytes < 0 {
		o.MaxFileBytes = multipartUnlimited
	}
	if o.MaxTotalFileBytes == 0 {
		o.MaxTotalFileBytes = defaultMultipartMaxTotalFileBytes
	} else if o.MaxTotalFileBytes < 0 {
		o.MaxTotalFileBytes = multipartUnlimited
	}
	if o.MaxDrainBytes == 0 {
		o.MaxDrainBytes = defaultMultipartMaxDrainBytes
	} else if o.MaxDrainBytes < 0 {
		o.MaxDrainBytes = multipartUnlimited
	}
}

// MultipartErrorKind identifies the kind of a MultipartError.
type MultipartErrorKind uint8

const (
	// ErrMultipartInvalidBoundaryKind means the boundary is missing,
	// illegal or cannot be located in the body.
	ErrMultipartInvalidBoundaryKind MultipartErrorKind = iota
	// ErrMultipartMalformedHeaderKind means a part header is malformed.
	ErrMultipartMalformedHeaderKind
	// ErrMultipartTooManyPartsKind means the total part count exceeds
	// MaxParts.
	ErrMultipartTooManyPartsKind
	// ErrMultipartPartHeaderTooLargeKind means a part header block
	// exceeds MaxPartHeaderBytes.
	ErrMultipartPartHeaderTooLargeKind
	// ErrMultipartTooManyHeaderFieldsKind means a part contains more
	// header fields than MaxPartHeaderFields.
	ErrMultipartTooManyHeaderFieldsKind
	// ErrMultipartValueTooLargeKind means the regular-field byte quota
	// (MaxValueBytes) is exceeded.
	ErrMultipartValueTooLargeKind
	// ErrMultipartFileTooLargeKind means a single file exceeds
	// MaxFileBytes.
	ErrMultipartFileTooLargeKind
	// ErrMultipartTotalFileTooLargeKind means the aggregate file byte
	// quota (MaxTotalFileBytes) is exceeded.
	ErrMultipartTotalFileTooLargeKind
	// ErrMultipartTruncatedKind means the body ended before the
	// expected boundary (unexpected EOF).
	ErrMultipartTruncatedKind
	// ErrMultipartDrainExceededKind means a skipped or abandoned body
	// could not be consumed within MaxDrainBytes; the connection must
	// be closed.
	ErrMultipartDrainExceededKind
	// ErrMultipartReaderConflictKind means the streaming reader cannot
	// be combined with the pre-parsed multipart form or with another
	// active reader of the same body.
	ErrMultipartReaderConflictKind
	// ErrMultipartPreParsedKind means the multipart form has already
	// been pre-parsed before the streaming reader was created.
	ErrMultipartPreParsedKind
	// ErrMultipartUnsupportedEncodingKind means the request uses a
	// Content-Encoding the streaming reader does not support.
	ErrMultipartUnsupportedEncodingKind
	// ErrMultipartClosedKind means the reader or part was used after
	// being closed.
	ErrMultipartClosedKind
)

// Sentinel errors. They are wrapped by *MultipartError (which also
// carries the part number), so errors.Is works against them.
var (
	ErrMultipartInvalidBoundary     = errors.New("multipart: invalid or missing boundary")
	ErrMultipartMalformedHeader     = errors.New("multipart: malformed part header")
	ErrMultipartTooManyParts        = errors.New("multipart: too many parts")
	ErrMultipartPartHeaderTooLarge  = errors.New("multipart: part header too large")
	ErrMultipartTooManyHeaderFields = errors.New("multipart: too many part header fields")
	ErrMultipartValueTooLarge       = errors.New("multipart: regular form field value too large")
	ErrMultipartFileTooLarge        = errors.New("multipart: file too large")
	ErrMultipartTotalFileTooLarge   = errors.New("multipart: total uploaded file size too large")
	ErrMultipartTruncated           = errors.New("multipart: request body truncated before boundary")
	ErrMultipartDrainExceeded       = errors.New("multipart: unread body exceeds the bounded drain budget; connection must be closed")
	ErrMultipartReaderConflict      = errors.New("multipart: a streaming reader is already consuming the request body")
	ErrMultipartPreParsed           = errors.New("multipart: form has been pre-parsed; disable pre-parsing with WithDisablePreParseMultipartForm(true) and stream the body with WithStreamBody(true)")
	ErrMultipartUnsupportedEncoding = errors.New("multipart: streaming reader does not support Content-Encoded request bodies")
	ErrMultipartClosed              = errors.New("multipart: reader or part is closed")
)

func multipartKindSentinel(kind MultipartErrorKind) error {
	switch kind {
	case ErrMultipartInvalidBoundaryKind:
		return ErrMultipartInvalidBoundary
	case ErrMultipartMalformedHeaderKind:
		return ErrMultipartMalformedHeader
	case ErrMultipartTooManyPartsKind:
		return ErrMultipartTooManyParts
	case ErrMultipartPartHeaderTooLargeKind:
		return ErrMultipartPartHeaderTooLarge
	case ErrMultipartTooManyHeaderFieldsKind:
		return ErrMultipartTooManyHeaderFields
	case ErrMultipartValueTooLargeKind:
		return ErrMultipartValueTooLarge
	case ErrMultipartFileTooLargeKind:
		return ErrMultipartFileTooLarge
	case ErrMultipartTotalFileTooLargeKind:
		return ErrMultipartTotalFileTooLarge
	case ErrMultipartTruncatedKind:
		return ErrMultipartTruncated
	case ErrMultipartDrainExceededKind:
		return ErrMultipartDrainExceeded
	case ErrMultipartReaderConflictKind:
		return ErrMultipartReaderConflict
	case ErrMultipartPreParsedKind:
		return ErrMultipartPreParsed
	case ErrMultipartUnsupportedEncodingKind:
		return ErrMultipartUnsupportedEncoding
	case ErrMultipartClosedKind:
		return ErrMultipartClosed
	default:
		return nil
	}
}

// MultipartError is the typed error returned by the restricted multipart
// reader. Part is the 1-based part number the error refers to; it is 0
// for errors located before the first part (e.g. the boundary itself).
type MultipartError struct {
	// Kind classifies the error.
	Kind MultipartErrorKind
	// Part is the 1-based part number, or 0 when the error is not
	// associated with a concrete part.
	Part int
	// Detail carries optional human-readable context.
	Detail string
}

// Error implements error.
func (e *MultipartError) Error() string {
	sentinel := multipartKindSentinel(e.Kind)
	msg := "multipart: unknown error"
	if sentinel != nil {
		msg = sentinel.Error()
	}
	if e.Part > 0 {
		msg = fmt.Sprintf("%s (part %d)", msg, e.Part)
	}
	if e.Detail != "" {
		msg = msg + ": " + e.Detail
	}
	return msg
}

// Unwrap exposes the kind sentinel for errors.Is/errors.As.
func (e *MultipartError) Unwrap() error {
	return multipartKindSentinel(e.Kind)
}

// PartNumber returns the 1-based part number the error refers to, or 0
// when the error is not associated with a concrete part.
func (e *MultipartError) PartNumber() int {
	return e.Part
}

func newMultipartError(kind MultipartErrorKind, part int, detail string) *MultipartError {
	return &MultipartError{Kind: kind, Part: part, Detail: detail}
}

// streamBuffer is a small byte buffer over an io.Reader.
//
// Unlike bufio.Reader, fill performs AT MOST one underlying Read and
// returns as soon as any byte (or an error) is available. This matters
// for the bounded drain policy: the reader must be able to give up after
// consuming exactly the budget even if the client is still holding the
// connection open and waiting for a response. A bufio.Reader would issue
// an additional blocking Read to fill its 4 KiB window and could never
// reach the give-up decision.
type streamBuffer struct {
	r    io.Reader
	buf  []byte
	rOff int
	wOff int
	err  error
	// wire counts every byte pulled from the underlying reader. These
	// are the bytes really consumed from the request body stream, which
	// makes the counter meaningful even when Content-Length is unknown.
	wire *int64
}

func newStreamBuffer(r io.Reader, size int, wire *int64) *streamBuffer {
	return &streamBuffer{r: r, buf: make([]byte, size), wire: wire}
}

// fill moves the unread tail to the front and performs a single Read.
// It blocks only when the buffer is empty and the underlying Read
// blocks. Callers must not call fill when the unread window already
// fills the buffer.
func (b *streamBuffer) fill() {
	if b.err != nil {
		return
	}
	if b.rOff > 0 {
		copy(b.buf, b.buf[b.rOff:b.wOff])
		b.wOff -= b.rOff
		b.rOff = 0
	}
	if b.wOff >= len(b.buf) {
		return
	}
	n, err := b.r.Read(b.buf[b.wOff:])
	*b.wire += int64(n)
	b.wOff += n
	// A fixed-length body that ends before its declared length reports
	// ErrUnexpectedEOF; for multipart framing this is simply the end of
	// available data and must surface as the typed truncation error, so
	// treat it like EOF in all line/body parsing paths.
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}
	if err != nil {
		b.err = err
	}
}

// pending returns the currently buffered, unread bytes without reading.
func (b *streamBuffer) pending() []byte {
	return b.buf[b.rOff:b.wOff]
}

// wait returns the currently buffered bytes, performing exactly one
// blocking fill first if the buffer is empty. It never busy-loops: the
// single fill blocks until data arrives or the stream reports an error.
func (b *streamBuffer) wait() []byte {
	if b.rOff == b.wOff && b.err == nil {
		b.fill()
	}
	return b.pending()
}

// discard advances the read position by n buffered bytes.
func (b *streamBuffer) discard(n int) {
	b.rOff += n
}

// readLine consumes one physical line including its trailing '\n'.
// Bytes are copied out so callers may keep the line while the buffer is
// refilled. If the stream ends before '\n', the remaining bytes are
// returned together with the underlying error (usually io.EOF).
func (b *streamBuffer) readLine() ([]byte, error) {
	var out []byte
	for {
		pending := b.buf[b.rOff:b.wOff]
		if i := bytes.IndexByte(pending, '\n'); i >= 0 {
			out = append(out, pending[:i+1]...)
			b.rOff += i + 1
			return out, nil
		}
		out = append(out, pending...)
		b.rOff = b.wOff
		if b.err != nil {
			return out, b.err
		}
		b.fill()
	}
}

// MultipartReader is a streaming, resource-bounded reader for
// multipart/form-data request bodies.
//
// It reads one part at a time with NextPart and never parses the whole
// form nor spools files to disk. Quotas advance with the bytes actually
// read and are shared across parts. The zero value is not usable; obtain
// an instance with Request.MultipartReader (or NewMultipartReader).
type MultipartReader struct {
	buf  *streamBuffer
	opts MultipartReaderOptions

	// delimiter that terminates a part body: CRLF + "--" + boundary.
	delim []byte
	// dashBoundary is "--" + boundary.
	dashBoundary []byte
	// dashBoundaryDash is "--" + boundary + "--".
	dashBoundaryDash []byte

	// partsRead is the number of parts whose boundary has been seen.
	partsRead int
	// cur is the part currently being read, or nil.
	cur *Part
	// sawFinal is true once the closing delimiter was consumed.
	sawFinal bool
	// closed is true after Close.
	closed bool
	// mustClose becomes true once the reader decides the connection
	// cannot be reused (bounded drain exhausted or fatal framing error).
	mustClose bool
	// closeErr is sticky after Close.
	closeErr error

	// Shared quotas, advanced by real reads.
	totalValue int64
	totalFile  int64
	// bytes discarded on behalf of the caller while draining.
	drained int64
	// bytes pulled from the underlying request body reader.
	wireBytes int64

	drainBuf []byte
}

// NewMultipartReader creates a restricted multipart reader over r.
// boundary must match the Content-Type boundary parameter. The reader
// takes ownership of r's read position for the lifetime of the request.
func NewMultipartReader(r io.Reader, boundary string, opts *MultipartReaderOptions) (*MultipartReader, error) {
	if len(boundary) == 0 || len(boundary) > multipartMaxBoundaryLength {
		return nil, newMultipartError(ErrMultipartInvalidBoundaryKind, 0,
			fmt.Sprintf("boundary length must be in [1,%d]", multipartMaxBoundaryLength))
	}
	for i := 0; i < len(boundary); i++ {
		b := boundary[i]
		if b == '\r' || b == '\n' || b < 0x20 || b == 0x7f {
			return nil, newMultipartError(ErrMultipartInvalidBoundaryKind, 0, "boundary contains illegal characters")
		}
	}

	normalized := MultipartReaderOptions{}
	if opts != nil {
		normalized = *opts
	}
	normalized.normalize()

	mr := &MultipartReader{
		opts:             normalized,
		dashBoundary:     append([]byte("--"), boundary...),
		dashBoundaryDash: append([]byte("--"), boundary+"--"...),
		drainBuf:         make([]byte, multipartDrainChunkSize),
	}
	mr.delim = append([]byte("\r\n"), mr.dashBoundary...)
	mr.buf = newStreamBuffer(r, multipartReaderBufferSize, &mr.wireBytes)
	return mr, nil
}

// NextPart advances to the next part and returns it.
//
// When the current part has not been fully consumed, its remaining body
// is discarded within the configured drain budget. If the budget is
// exhausted, ErrMultipartDrainExceeded is returned, the connection is
// marked for closing and no further reading is possible.
//
// It returns io.EOF when there are no more parts. The returned Part is
// valid until the next NextPart or Close call.
func (mr *MultipartReader) NextPart() (*Part, error) {
	if mr.closed {
		return nil, newMultipartError(ErrMultipartClosedKind, mr.partsRead, "")
	}
	if mr.sawFinal {
		return nil, io.EOF
	}
	if mr.mustClose {
		// Fatal state: connection is doomed, do not read more data.
		return nil, newMultipartError(ErrMultipartDrainExceededKind, mr.partsRead, "")
	}

	if mr.cur != nil {
		// Skip the remainder of the current part up to its boundary.
		if err := mr.drainCurrentPart(); err != nil {
			return nil, err
		}
		mr.cur = nil
	}

	if mr.partsRead == 0 {
		if err := mr.skipPreamble(); err != nil {
			return nil, err
		}
		if mr.sawFinal {
			// Immediate closing delimiter: the multipart body has no parts.
			return nil, io.EOF
		}
	} else {
		final, err := mr.consumeBoundaryLine()
		if err != nil {
			return nil, err
		}
		if final {
			mr.sawFinal = true
			mr.cur = nil
			return nil, io.EOF
		}
	}

	mr.partsRead++
	if mr.partsRead > mr.opts.MaxParts {
		mr.mustClose = true
		return nil, newMultipartError(ErrMultipartTooManyPartsKind, mr.partsRead, "")
	}

	p, err := mr.buildPart()
	if err != nil {
		return nil, err
	}
	mr.cur = p
	return p, nil
}

// Close skips the remaining parts within the drain budget and releases
// the reader state. It is idempotent: subsequent calls return the error
// recorded by the first call (nil on success).
//
// If the remaining body cannot be consumed within MaxDrainBytes, or if a
// fatal framing error was hit earlier, Close returns
// ErrMultipartDrainExceeded (or the fatal error) and MustCloseConnection
// reports true; callers should not attempt keep-alive.
func (mr *MultipartReader) Close() error {
	if mr.closed {
		return mr.closeErr
	}
	mr.closed = true

	if mr.sawFinal {
		mr.closeErr = nil
		return nil
	}
	if mr.mustClose {
		mr.closeErr = newMultipartError(ErrMultipartDrainExceededKind, mr.partsRead, "reader already gave up within the bounded drain budget")
		return mr.closeErr
	}

	var err error
	if mr.cur != nil {
		if e := mr.drainCurrentPart(); e != nil {
			err = e
		}
		mr.cur = nil
	}
	for err == nil && !mr.sawFinal {
		if mr.partsRead == 0 {
			// No part has been entered yet: honor an optional MIME
			// preamble before the first boundary.
			if e := mr.skipPreamble(); e != nil {
				err = e
				break
			}
			if mr.sawFinal {
				break
			}
		} else {
			final, e := mr.consumeBoundaryLine()
			if e != nil {
				err = e
				break
			}
			if final {
				mr.sawFinal = true
				break
			}
		}
		mr.partsRead++
		if mr.partsRead > mr.opts.MaxParts {
			mr.mustClose = true
			err = newMultipartError(ErrMultipartTooManyPartsKind, mr.partsRead, "")
			break
		}
		p, e := mr.buildPart()
		if e != nil {
			err = e
			break
		}
		mr.cur = p
		if e := mr.drainCurrentPart(); e != nil {
			err = e
			mr.cur = nil
			break
		}
		mr.cur = nil
	}
	if mr.mustClose && err == nil {
		err = newMultipartError(ErrMultipartDrainExceededKind, mr.partsRead, "")
	}
	mr.closeErr = err
	return err
}

// MustCloseConnection reports whether the underlying connection must be
// closed because the reader could not bring the request body to a known
// end within the configured limits.
func (mr *MultipartReader) MustCloseConnection() bool {
	return mr.mustClose
}

// BytesRead returns the number of bytes pulled from the underlying
// request body reader so far.
func (mr *MultipartReader) BytesRead() int64 {
	return mr.wireBytes
}

// PartsRead returns the number of parts seen so far.
func (mr *MultipartReader) PartsRead() int {
	return mr.partsRead
}

// TotalValueBytes returns the aggregate bytes delivered from regular
// (non-file) parts.
func (mr *MultipartReader) TotalValueBytes() int64 {
	return mr.totalValue
}

// TotalFileBytes returns the aggregate bytes delivered from file parts.
func (mr *MultipartReader) TotalFileBytes() int64 {
	return mr.totalFile
}

// Part represents a single multipart part.
type Part struct {
	// PartNumber is the 1-based position of the part in the request.
	PartNumber int
	// Header holds the parsed MIME headers of the part.
	Header textproto.MIMEHeader
	// FormName is the name field of the Content-Disposition header.
	FormName string
	// FileName is the filename parameter of Content-Disposition. Its
	// presence (including an empty string) marks the part as a file.
	FileName string
	// IsFile reports whether the part carries a filename.
	IsFile bool

	pr *partReader
}

// Name returns the form field name of the part.
func (p *Part) Name() string {
	return p.FormName
}

// Filename returns the submitted filename of the part, or "".
func (p *Part) Filename() string {
	return p.FileName
}

// Size returns the number of body bytes delivered from this part so far.
func (p *Part) Size() int64 {
	return p.pr.n
}

// Read implements io.Reader. It returns io.EOF when the whole part body
// has been delivered (the terminating boundary is located but not yet
// consumed). Quota violations are returned as typed *MultipartError; the
// unread body must then be skipped via Close (or the next NextPart),
// which applies the bounded drain policy.
func (p *Part) Read(dst []byte) (int, error) {
	if p.pr == nil {
		return 0, newMultipartError(ErrMultipartClosedKind, p.PartNumber, "")
	}
	return p.pr.read(dst, true)
}

// Close discards the remaining body of the part within the drain budget.
// It is implicitly called by the next MultipartReader.NextPart.
func (p *Part) Close() error {
	if p.pr == nil {
		return nil
	}
	return p.pr.mr.drainCurrentPart()
}

// partReader serves a single part body and detects the terminating
// boundary without mistaking an unread body for the next part header.
type partReader struct {
	p    *Part
	mr   *MultipartReader
	done bool
	// n is the number of body bytes delivered for this part.
	n int64
	// sticky is the first quota error hit while reading.
	sticky *MultipartError
}

// read delivers body bytes. When enforce is false, quotas are bypassed
// (used for bounded draining); boundary detection always stays active.
//
// Bytes beyond an enforced quota are never delivered: the deliverable
// window is clamped to the remaining quota before copying, and the
// typed quota error is returned alongside the final allowed bytes.
//
// The function blocks on at most one underlying Read when progress
// requires more data, so it returns as soon as any body byte is
// available. This is what allows the bounded drain to stop exactly at
// the budget even if the client is waiting for a response instead of
// sending more data.
func (pr *partReader) read(dst []byte, enforce bool) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if pr.done {
		return 0, io.EOF
	}
	if enforce && pr.sticky != nil {
		return 0, pr.sticky
	}

	mr := pr.mr
	m := len(mr.delim)

	for {
		b := mr.buf.pending()

		// The body ends exactly here: consume the CRLF prefix and leave
		// the "--boundary[--]" line for the next boundary check. This
		// must be recognized before quota exhaustion so an exact
		// quota fit at the boundary returns io.EOF, not an error.
		if bytes.HasPrefix(b, mr.delim) {
			mr.buf.discard(2)
			pr.done = true
			return 0, io.EOF
		}

		if enforce && pr.quotaRemaining() <= 0 {
			qerr := pr.makeQuotaError()
			pr.sticky = qerr
			return 0, qerr
		}

		marker := bytes.Index(b, mr.delim)
		maxN := 0
		if marker > 0 {
			maxN = marker
		} else if marker < 0 && len(b) >= m {
			// Hold back the last m-1 bytes: a delimiter may start
			// there once more data arrives from the network.
			maxN = len(b) - m + 1
		}
		if maxN > len(dst) {
			maxN = len(dst)
		}
		if enforce {
			if rem := pr.quotaRemaining(); int64(maxN) > rem {
				maxN = int(rem)
			}
		}

		if maxN > 0 {
			n := copy(dst, b[:maxN])
			mr.buf.discard(n)
			pr.account(n, enforce)
			if marker > 0 && n == marker {
				mr.buf.discard(2)
				pr.done = true
				return n, io.EOF
			}
			return n, nil
		}

		// No deliverable byte yet. Decide using the stream state so we
		// never busy-loop: wait for more data or report termination.
		if mr.buf.err != nil {
			if mr.buf.err == io.EOF {
				// Stream ended without a complete delimiter. Flush the
				// buffered tail (shorter than the delimiter, so it
				// cannot be a boundary) once, honoring the quota; the
				// next call reports truncation.
				n := len(b)
				if n > len(dst) {
					n = len(dst)
				}
				if enforce {
					if rem := pr.quotaRemaining(); int64(n) > rem {
						n = int(rem)
					}
				}
				if n > 0 {
					copy(dst, b[:n])
					mr.buf.discard(n)
					pr.account(n, enforce)
					return n, nil
				}
				mr.mustClose = true
				return 0, newMultipartError(ErrMultipartTruncatedKind, pr.p.PartNumber, "")
			}
			return 0, mr.buf.err
		}

		// Block for additional bytes (exactly one underlying Read) and
		// re-scan for the delimiter.
		mr.buf.fill()
	}
}

// quotaRemaining returns how many body bytes may still be delivered
// before the tightest applicable quota is reached.
func (pr *partReader) quotaRemaining() int64 {
	if pr.p.IsFile {
		rem := pr.mr.opts.MaxFileBytes - pr.n
		if total := pr.mr.opts.MaxTotalFileBytes - pr.mr.totalFile; total < rem {
			rem = total
		}
		return rem
	}
	return pr.mr.opts.MaxValueBytes - pr.mr.totalValue
}

// quotaError is decided inline in read(): an exact quota fit at the
// boundary returns io.EOF; further body bytes beyond the quota return
// the typed error.

// makeQuotaError builds the typed error for the tightest exhausted quota.
func (pr *partReader) makeQuotaError() *MultipartError {
	mr := pr.mr
	if pr.p.IsFile {
		if mr.opts.MaxFileBytes != multipartUnlimited && pr.n >= mr.opts.MaxFileBytes {
			return newMultipartError(ErrMultipartFileTooLargeKind, pr.p.PartNumber,
				fmt.Sprintf("file part %q exceeds %d bytes", pr.p.FormName, mr.opts.MaxFileBytes))
		}
		return newMultipartError(ErrMultipartTotalFileTooLargeKind, pr.p.PartNumber,
			fmt.Sprintf("uploaded files exceed %d bytes in total", mr.opts.MaxTotalFileBytes))
	}
	return newMultipartError(ErrMultipartValueTooLargeKind, pr.p.PartNumber,
		fmt.Sprintf("form fields exceed %d bytes in total", mr.opts.MaxValueBytes))
}

// account advances the per-part and shared counters after n body bytes
// were delivered. Shared quotas are only advanced for enforced reads;
// bytes discarded while draining use the separate drain budget.
func (pr *partReader) account(n int, enforce bool) {
	if n <= 0 {
		return
	}
	pr.n += int64(n)
	if enforce {
		if pr.p.IsFile {
			pr.mr.totalFile += int64(n)
		} else {
			pr.mr.totalValue += int64(n)
		}
	}
}

// drainCurrentPart discards the unread body of mr.cur until its
// terminating delimiter, counting discarded bytes against MaxDrainBytes.
// It never crosses a part boundary blindly, so an unread file body can
// never be interpreted as the next part header.
func (mr *MultipartReader) drainCurrentPart() error {
	if mr.cur == nil || mr.cur.pr.done {
		return nil
	}
	pr := mr.cur.pr
	for !pr.done {
		remaining := mr.opts.MaxDrainBytes - mr.drained
		if remaining <= 0 {
			mr.mustClose = true
			return newMultipartError(ErrMultipartDrainExceededKind, mr.cur.PartNumber,
				fmt.Sprintf("more than %d unread bytes", mr.opts.MaxDrainBytes))
		}
		buf := mr.drainBuf
		if int64(len(buf)) > remaining {
			buf = buf[:remaining]
		}
		n, err := pr.read(buf, false)
		mr.drained += int64(n)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// readPhysicalLine reads one physical line including its trailing '\n'.
// The returned slice is an independent copy, so callers may keep it
// while further data is read. It blocks on at most one underlying Read
// while waiting for more bytes.
func (mr *MultipartReader) readPhysicalLine() ([]byte, error) {
	return mr.buf.readLine()
}

// skipPreamble reads until the first boundary delimiter. Preamble bytes
// are counted against the drain budget to avoid unbounded skipping.
func (mr *MultipartReader) skipPreamble() error {
	for {
		line, err := mr.readPhysicalLine()
		mr.drained += int64(len(line))
		if final, ok := mr.matchBoundaryLine(line); ok {
			if final {
				// Zero parts at all: legal but empty multipart body.
				mr.sawFinal = true
			}
			return nil
		}
		if err == io.EOF {
			return newMultipartError(ErrMultipartInvalidBoundaryKind, 0, "no boundary found before end of body")
		}
		if err != nil {
			return err
		}
		if bytes.HasPrefix(bytes.TrimRight(line, "\r\n"), mr.dashBoundary) {
			// Looks like a boundary but is not exactly one.
			mr.mustClose = true
			return newMultipartError(ErrMultipartInvalidBoundaryKind, 0, "malformed boundary line")
		}
		if mr.drained > mr.opts.MaxDrainBytes {
			mr.mustClose = true
			return newMultipartError(ErrMultipartDrainExceededKind, 0, "preamble exceeds the bounded drain budget")
		}
	}
}

// consumeBoundaryLine reads the boundary line left after a part body was
// fully consumed and reports whether it is the closing delimiter.
func (mr *MultipartReader) consumeBoundaryLine() (bool, error) {
	line, err := mr.readPhysicalLine()
	if final, ok := mr.matchBoundaryLine(line); ok {
		return final, nil
	}
	if err == io.EOF {
		mr.mustClose = true
		return false, newMultipartError(ErrMultipartTruncatedKind, mr.partsRead+1, "missing boundary line")
	}
	if err != nil {
		return false, err
	}
	mr.mustClose = true
	return false, newMultipartError(ErrMultipartInvalidBoundaryKind, mr.partsRead+1, "expected boundary delimiter")
}

// matchBoundaryLine validates a physical line against the boundary.
// It tolerates the optional transport padding (spaces/tabs) allowed by
// RFC 2046 and quoted/unquoted boundaries already extracted from the
// Content-Type header.
func (mr *MultipartReader) matchBoundaryLine(line []byte) (final bool, ok bool) {
	line = bytes.TrimRight(line, "\r\n")
	line = bytes.TrimRight(line, " \t")
	if bytes.Equal(line, mr.dashBoundary) {
		return false, true
	}
	if bytes.Equal(line, mr.dashBoundaryDash) {
		return true, true
	}
	return false, false
}

// buildPart reads and validates the header block of the new part and
// prepares its body reader.
func (mr *MultipartReader) buildPart() (*Part, error) {
	hdr, err := mr.readPartHeader()
	if err != nil {
		// Framing cannot be trusted after an invalid header block.
		mr.mustClose = true
		return nil, err
	}

	formName, fileName, isFile := disposition(hdr)

	p := &Part{
		PartNumber: mr.partsRead,
		Header:     hdr,
		FormName:   formName,
		FileName:   fileName,
		IsFile:     isFile,
	}
	p.pr = &partReader{p: p, mr: mr}
	return p, nil
}

// readPartHeader parses an RFC-compliant MIME header block terminated by
// a blank line, enforcing the per-part header byte and field-count
// limits.
func (mr *MultipartReader) readPartHeader() (textproto.MIMEHeader, error) {
	hdr := make(textproto.MIMEHeader)
	total := 0
	fields := 0
	var lastKey string
	haveField := false

	for {
		line, err := mr.readPhysicalLine()
		total += len(line)
		if total > mr.opts.MaxPartHeaderBytes {
			return nil, newMultipartError(ErrMultipartPartHeaderTooLargeKind, mr.partsRead,
				fmt.Sprintf("part header exceeds %d bytes", mr.opts.MaxPartHeaderBytes))
		}
		// A blank line terminates the header block. It must be present;
		// EOF before it means the body is truncated.
		if isBlankLine(line) {
			return hdr, nil
		}
		if err != nil {
			if err == io.EOF {
				mr.mustClose = true
				return nil, newMultipartError(ErrMultipartTruncatedKind, mr.partsRead, "part header is incomplete")
			}
			return nil, err
		}

		// Obs-fold continuation line.
		if line[0] == ' ' || line[0] == '\t' {
			if !haveField {
				return nil, newMultipartError(ErrMultipartMalformedHeaderKind, mr.partsRead, "continuation line without a field")
			}
			folded := trimHeaderValue(bytes.TrimRight(line, "\r\n"))
			if len(hdr[lastKey]) > 0 {
				hdr[lastKey][len(hdr[lastKey])-1] += " " + folded
			}
			continue
		}

		fields++
		if fields > mr.opts.MaxPartHeaderFields {
			return nil, newMultipartError(ErrMultipartTooManyHeaderFieldsKind, mr.partsRead,
				fmt.Sprintf("more than %d header fields", mr.opts.MaxPartHeaderFields))
		}

		colon := bytes.IndexByte(line, ':')
		if colon <= 0 {
			return nil, newMultipartError(ErrMultipartMalformedHeaderKind, mr.partsRead, "missing colon in header field")
		}
		name := string(line[:colon])
		if !isValidHeaderFieldName(name) {
			return nil, newMultipartError(ErrMultipartMalformedHeaderKind, mr.partsRead, "illegal header field name")
		}
		value := trimHeaderValue(bytes.TrimRight(line[colon+1:], "\r\n"))

		canonical := textproto.CanonicalMIMEHeaderKey(name)
		hdr[canonical] = append(hdr[canonical], value)
		lastKey = canonical
		haveField = true
	}
}

func isBlankLine(line []byte) bool {
	for _, b := range line {
		if b != '\r' && b != '\n' {
			return false
		}
	}
	return len(line) > 0
}

func trimHeaderValue(v []byte) string {
	// OWS around a field value is insignificant.
	return string(bytes.Trim(v, " \t"))
}

func isValidHeaderFieldName(name string) bool {
	if len(name) == 0 {
		return false
	}
	for i := 0; i < len(name); i++ {
		if !isTokenChar(name[i]) {
			return false
		}
	}
	return true
}

func isTokenChar(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z':
		return true
	case b >= 'A' && b <= 'Z':
		return true
	case b >= '0' && b <= '9':
		return true
	}
	switch b {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	}
	return false
}

// disposition extracts the form field name and filename. Quoted
// parameters (including escaped quotes) are handled by the standard MIME
// media-type parser.
func disposition(hdr textproto.MIMEHeader) (name, filename string, isFile bool) {
	mediaType, params, err := mime.ParseMediaType(hdr.Get("Content-Disposition"))
	if err != nil || mediaType != "form-data" {
		return "", "", false
	}
	name = params["name"]
	if filename, isFile = params["filename"]; isFile {
		return name, filename, true
	}
	return name, "", false
}
