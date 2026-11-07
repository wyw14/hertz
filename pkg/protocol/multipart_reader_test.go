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
	"io"
	"mime/multipart"
	"strings"
	"testing"

	"github.com/cloudwego/hertz/pkg/common/test/assert"
)

// chunkReader returns at most max bytes per Read, to emulate tiny
// network chunks (boundaries and headers end up split arbitrarily).
type chunkReader struct {
	data []byte
	off  int
	max  int
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if c.off >= len(c.data) {
		return 0, io.EOF
	}
	n := len(p)
	if n == 0 {
		return 0, nil
	}
	if n > c.max {
		n = c.max
	}
	if n > len(c.data)-c.off {
		n = len(c.data) - c.off
	}
	copy(p, c.data[c.off:c.off+n])
	c.off += n
	return n, nil
}

// collectedPart is a fully consumed part for assertions.
type collectedPart struct {
	number   int
	name     string
	filename string
	isFile   bool
	content  []byte
}

func collectAllParts(t *testing.T, mr *MultipartReader) []collectedPart {
	t.Helper()
	var out []collectedPart
	for {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("NextPart error: %v", err)
		}
		content, err := io.ReadAll(p)
		if err != nil {
			t.Fatalf("part %d ReadAll error: %v", p.PartNumber, err)
		}
		out = append(out, collectedPart{
			number:   p.PartNumber,
			name:     p.FormName,
			filename: p.FileName,
			isFile:   p.IsFile,
			content:  content,
		})
	}
	return out
}

func buildMultipartBody(t *testing.T, boundary string, build func(w *multipart.Writer)) []byte {
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

func TestMultipartReader_BasicMixedPartsTinyChunks(t *testing.T) {
	t.Parallel()
	boundary := "MyBoundary123"
	body := buildMultipartBody(t, boundary, func(w *multipart.Writer) {
		assert.Nil(t, w.WriteField("field1", "value1"))
		assert.Nil(t, w.WriteField("field1", "value2")) // repeated field
		// files
		h := CreateMultipartHeader("file1", "a.json", "application/json")
		pw, err := w.CreatePart(h)
		assert.Nil(t, err)
		_, err = pw.Write([]byte(`{"k":"v"}`))
		assert.Nil(t, err)
		h2 := CreateMultipartHeader("empty", "empty.txt", "text/plain")
		pw2, err := w.CreatePart(h2)
		assert.Nil(t, err)
		_ = pw2
		// large value spanning many chunks
		assert.Nil(t, w.WriteField("big", strings.Repeat("Z", 5000)))
		// binary-ish file containing CRLF bytes
		h3 := CreateMultipartHeader("bin", "b.bin", "application/octet-stream")
		pw3, err := w.CreatePart(h3)
		assert.Nil(t, err)
		_, err = pw3.Write(bytes.Repeat([]byte{'x'}, 4200))
		assert.Nil(t, err)
	})

	for _, chunk := range []int{1, 2, 3, 7, 64} {
		r := &chunkReader{data: body, max: chunk}
		mr, err := NewMultipartReader(r, boundary, nil)
		assert.Nil(t, err)

		parts := collectAllParts(t, mr)
		assert.DeepEqual(t, 6, len(parts))

		assert.DeepEqual(t, "field1", parts[0].name)
		assert.DeepEqual(t, "value1", string(parts[0].content))
		assert.Assert(t, !parts[0].isFile)

		assert.DeepEqual(t, "field1", parts[1].name)
		assert.DeepEqual(t, "value2", string(parts[1].content))

		assert.DeepEqual(t, "file1", parts[2].name)
		assert.DeepEqual(t, "a.json", parts[2].filename)
		assert.Assert(t, parts[2].isFile)
		assert.DeepEqual(t, `{"k":"v"}`, string(parts[2].content))

		assert.DeepEqual(t, "empty", parts[3].name)
		assert.DeepEqual(t, "empty.txt", parts[3].filename)
		assert.DeepEqual(t, 0, len(parts[3].content))

		assert.DeepEqual(t, "big", parts[4].name)
		assert.DeepEqual(t, 5000, len(parts[4].content))

		assert.DeepEqual(t, "bin", parts[5].name)
		assert.DeepEqual(t, "b.bin", parts[5].filename)
		assert.DeepEqual(t, 4200, len(parts[5].content))

		assert.Nil(t, mr.Close())
		assert.Assert(t, !mr.MustCloseConnection())
		// Every body byte has really been consumed, even with
		// Content-Length unknown (chunkReader does not carry one).
		assert.DeepEqual(t, int64(len(body)), mr.BytesRead())
		assert.DeepEqual(t, int64(5000+len("value1")+len("value2")), mr.TotalValueBytes())
		assert.DeepEqual(t, int64(len(`{"k":"v"}`)+0+4200), mr.TotalFileBytes())
	}
}

func TestMultipartReader_NestedReaderSharesQuota(t *testing.T) {
	t.Parallel()
	boundary := "bnd"
	fileContent := bytes.Repeat([]byte{'a'}, 3000)
	body := buildMultipartBody(t, boundary, func(w *multipart.Writer) {
		h := CreateMultipartHeader("f", "f.bin", "")
		pw, err := w.CreatePart(h)
		assert.Nil(t, err)
		_, _ = pw.Write(fileContent)
		assert.Nil(t, w.WriteField("v", "hello"))
	})

	mr, err := NewMultipartReader(&chunkReader{data: body, max: 5}, boundary, &MultipartReaderOptions{
		MaxFileBytes:      10000,
		MaxTotalFileBytes: 10000,
		MaxValueBytes:     10000,
		MaxDrainBytes:     10000,
	})
	assert.Nil(t, err)

	p, err := mr.NextPart()
	assert.Nil(t, err)
	// Read through a nested LimitReader-like wrapper: counters must move
	// with real reads and be shared.
	limited := io.LimitReader(p, 1000)
	n, err := io.Copy(io.Discard, limited)
	assert.Nil(t, err)
	assert.DeepEqual(t, int64(1000), n)
	assert.DeepEqual(t, int64(1000), mr.TotalFileBytes())
	// Continue with another nested reader over the same part.
	n2, err := io.Copy(io.Discard, io.LimitReader(p, 2000))
	assert.Nil(t, err)
	assert.DeepEqual(t, int64(2000), n2)
	assert.DeepEqual(t, int64(3000), mr.TotalFileBytes())
	rest, err := io.ReadAll(p)
	assert.Nil(t, err)
	assert.DeepEqual(t, 0, len(rest))
	parts := collectAllParts(t, mr)
	assert.DeepEqual(t, 1, len(parts))
	assert.DeepEqual(t, "hello", string(parts[0].content))
	assert.DeepEqual(t, int64(5), mr.TotalValueBytes())
	assert.Nil(t, mr.Close())
}

func TestMultipartReader_TotalFileLimitMultipleSmallFiles(t *testing.T) {
	t.Parallel()
	boundary := "bnd"
	body := buildMultipartBody(t, boundary, func(w *multipart.Writer) {
		for i := 0; i < 4; i++ {
			h := CreateMultipartHeader("f", "f.bin", "")
			pw, err := w.CreatePart(h)
			assert.Nil(t, err)
			_, _ = pw.Write(bytes.Repeat([]byte{byte('a' + i)}, 100))
		}
	})
	mr, err := NewMultipartReader(&chunkReader{data: body, max: 11}, boundary, &MultipartReaderOptions{
		MaxFileBytes:      10000,
		MaxTotalFileBytes: 250,
		MaxDrainBytes:     -1,
	})
	assert.Nil(t, err)

	// First two files fit (200 bytes), third crosses the aggregate cap.
	p1, err := mr.NextPart()
	assert.Nil(t, err)
	_, err = io.Copy(io.Discard, p1)
	assert.Nil(t, err)

	p2, err := mr.NextPart()
	assert.Nil(t, err)
	_, err = io.Copy(io.Discard, p2)
	assert.Nil(t, err)

	p3, err := mr.NextPart()
	assert.Nil(t, err)
	var got *MultipartError
	buf := make([]byte, 100)
	for {
		_, rerr := p3.Read(buf)
		if rerr != nil {
			me := &MultipartError{}
			assert.Assert(t, errors.As(rerr, &me))
			got = me
			break
		}
	}
	assert.Assert(t, got != nil)
	assert.DeepEqual(t, ErrMultipartTotalFileTooLargeKind, got.Kind)
	assert.DeepEqual(t, 3, got.PartNumber())
	assert.Assert(t, errors.Is(got, ErrMultipartTotalFileTooLarge))
}

func TestMultipartReader_SingleFileLimit(t *testing.T) {
	t.Parallel()
	boundary := "bnd"
	body := buildMultipartBody(t, boundary, func(w *multipart.Writer) {
		h := CreateMultipartHeader("big", "big.bin", "")
		pw, err := w.CreatePart(h)
		assert.Nil(t, err)
		_, _ = pw.Write(bytes.Repeat([]byte{'z'}, 5000))
	})
	mr, err := NewMultipartReader(bytes.NewReader(body), boundary, &MultipartReaderOptions{
		MaxFileBytes:      100,
		MaxTotalFileBytes: 100000,
	})
	assert.Nil(t, err)

	p, err := mr.NextPart()
	assert.Nil(t, err)
	var total int
	buf := make([]byte, 40)
	var limitErr *MultipartError
	for {
		n, rerr := p.Read(buf)
		total += n
		if rerr != nil {
			me := &MultipartError{}
			if errors.As(rerr, &me) {
				limitErr = me
			}
			break
		}
	}
	assert.Assert(t, limitErr != nil)
	assert.DeepEqual(t, ErrMultipartFileTooLargeKind, limitErr.Kind)
	assert.DeepEqual(t, 1, limitErr.PartNumber())
	// Never deliver more than the cap.
	assert.DeepEqual(t, 100, total)
}

func TestMultipartReader_ValueLimitSharedAcrossParts(t *testing.T) {
	t.Parallel()
	boundary := "bnd"
	body := buildMultipartBody(t, boundary, func(w *multipart.Writer) {
		assert.Nil(t, w.WriteField("a", strings.Repeat("a", 300)))
		assert.Nil(t, w.WriteField("b", strings.Repeat("b", 300)))
	})
	mr, err := NewMultipartReader(&chunkReader{data: body, max: 13}, boundary, &MultipartReaderOptions{
		MaxValueBytes: 500,
		MaxDrainBytes: -1,
	})
	assert.Nil(t, err)

	p1, err := mr.NextPart()
	assert.Nil(t, err)
	d1, err := io.ReadAll(p1)
	assert.Nil(t, err)
	assert.DeepEqual(t, 300, len(d1))

	p2, err := mr.NextPart()
	assert.Nil(t, err)
	var total int
	buf := make([]byte, 64)
	var valueErr *MultipartError
	for {
		n, rerr := p2.Read(buf)
		total += n
		if rerr != nil {
			me := &MultipartError{}
			if errors.As(rerr, &me) {
				valueErr = me
			}
			break
		}
	}
	assert.Assert(t, valueErr != nil)
	assert.DeepEqual(t, ErrMultipartValueTooLargeKind, valueErr.Kind)
	assert.DeepEqual(t, 2, valueErr.PartNumber())
	// Shared cap is 500: 300 already spent on part 1, only 200 left.
	assert.DeepEqual(t, 200, total)
}

func TestMultipartReader_ValueExactlyAtCapIsOK(t *testing.T) {
	t.Parallel()
	boundary := "bnd"
	body := buildMultipartBody(t, boundary, func(w *multipart.Writer) {
		assert.Nil(t, w.WriteField("a", strings.Repeat("a", 256)))
	})
	mr, err := NewMultipartReader(&chunkReader{data: body, max: 3}, boundary, &MultipartReaderOptions{
		MaxValueBytes: 256,
		MaxDrainBytes: -1,
	})
	assert.Nil(t, err)
	parts := collectAllParts(t, mr)
	assert.DeepEqual(t, 256, len(parts[0].content))
	assert.Nil(t, mr.Close())
}

func TestMultipartReader_MaxParts(t *testing.T) {
	t.Parallel()
	boundary := "bnd"
	body := buildMultipartBody(t, boundary, func(w *multipart.Writer) {
		for i := 0; i < 5; i++ {
			assert.Nil(t, w.WriteField("k", "v"))
		}
	})
	mr, err := NewMultipartReader(bytes.NewReader(body), boundary, &MultipartReaderOptions{
		MaxParts:      3,
		MaxDrainBytes: -1,
	})
	assert.Nil(t, err)
	for i := 1; i <= 3; i++ {
		_, err := mr.NextPart()
		assert.Nil(t, err)
	}
	_, err = mr.NextPart()
	me := &MultipartError{}
	assert.Assert(t, errors.As(err, &me))
	assert.DeepEqual(t, ErrMultipartTooManyPartsKind, me.Kind)
	assert.DeepEqual(t, 4, me.PartNumber())
	assert.Assert(t, mr.MustCloseConnection())
}

func TestMultipartReader_PartHeaderLimits(t *testing.T) {
	t.Parallel()
	// Too many header fields.
	var b strings.Builder
	b.WriteString("--bnd\r\n")
	for i := 0; i < 5; i++ {
		b.WriteString("X-Custom-")
		b.WriteString(string(rune('a' + i)))
		b.WriteString(": v\r\n")
	}
	b.WriteString("\r\nv\r\n--bnd--\r\n")
	mr, err := NewMultipartReader(strings.NewReader(b.String()), "bnd", &MultipartReaderOptions{
		MaxPartHeaderFields: 3,
	})
	assert.Nil(t, err)
	_, err = mr.NextPart()
	me := &MultipartError{}
	assert.Assert(t, errors.As(err, &me))
	assert.DeepEqual(t, ErrMultipartTooManyHeaderFieldsKind, me.Kind)
	assert.DeepEqual(t, 1, me.PartNumber())

	// Header block too large (continuation line keeps it small in field
	// count but large in bytes).
	var b2 strings.Builder
	b2.WriteString("--bnd\r\n")
	b2.WriteString("Content-Disposition: form-data; name=\"k\"\r\n")
	b2.WriteString("X-Long: " + strings.Repeat("a", 300) + "\r\n")
	b2.WriteString("\r\nv\r\n--bnd--\r\n")
	mr2, err := NewMultipartReader(strings.NewReader(b2.String()), "bnd", &MultipartReaderOptions{
		MaxPartHeaderBytes: 128,
	})
	assert.Nil(t, err)
	_, err = mr2.NextPart()
	me2 := &MultipartError{}
	assert.Assert(t, errors.As(err, &me2))
	assert.DeepEqual(t, ErrMultipartPartHeaderTooLargeKind, me2.Kind)
}

func TestMultipartReader_MalformedHeader(t *testing.T) {
	t.Parallel()
	body := strings.ReplaceAll("--bnd\nNotAHeaderLine\n\nv\n--bnd--\n", "\n", "\r\n")
	mr, err := NewMultipartReader(strings.NewReader(body), "bnd", nil)
	assert.Nil(t, err)
	_, err = mr.NextPart()
	me := &MultipartError{}
	assert.Assert(t, errors.As(err, &me))
	assert.DeepEqual(t, ErrMultipartMalformedHeaderKind, me.Kind)
	assert.DeepEqual(t, 1, me.PartNumber())
	assert.Assert(t, mr.MustCloseConnection())

	// Continuation without a preceding field.
	body2 := strings.ReplaceAll("--bnd\n orphan\r\n\r\nv\r\n--bnd--\r\n", "\n", "\r\n")
	mr2, err := NewMultipartReader(strings.NewReader(body2), "bnd", nil)
	assert.Nil(t, err)
	_, err = mr2.NextPart()
	me2 := &MultipartError{}
	assert.Assert(t, errors.As(err, &me2))
	assert.DeepEqual(t, ErrMultipartMalformedHeaderKind, me2.Kind)
}

func TestMultipartReader_InvalidBoundary(t *testing.T) {
	t.Parallel()
	_, err := NewMultipartReader(strings.NewReader(""), "", nil)
	me := &MultipartError{}
	assert.Assert(t, errors.As(err, &me))
	assert.DeepEqual(t, ErrMultipartInvalidBoundaryKind, me.Kind)
	assert.Assert(t, errors.Is(err, ErrMultipartInvalidBoundary))

	_, err = NewMultipartReader(strings.NewReader(""), strings.Repeat("a", 71), nil)
	assert.Assert(t, errors.Is(err, ErrMultipartInvalidBoundary))

	// Boundary never appears in the body.
	mr, err := NewMultipartReader(strings.NewReader("garbage preamble\r\nwithout boundary\r\n"), "bnd", nil)
	assert.Nil(t, err)
	_, err = mr.NextPart()
	assert.Assert(t, errors.Is(err, ErrMultipartInvalidBoundary))

	// Close-looking but wrong boundary.
	mr2, err := NewMultipartReader(strings.NewReader("--bndXYZ\r\n\r\nv\r\n--bndXYZ--\r\n"), "bnd", nil)
	assert.Nil(t, err)
	_, err = mr2.NextPart()
	assert.Assert(t, errors.Is(err, ErrMultipartInvalidBoundary))
}

func TestMultipartReader_TruncatedBody(t *testing.T) {
	t.Parallel()
	// Cut mid-way through the second part's file body.
	full := buildMultipartBody(t, "bnd", func(w *multipart.Writer) {
		assert.Nil(t, w.WriteField("k", "v"))
		h := CreateMultipartHeader("f", "f", "")
		pw, err := w.CreatePart(h)
		assert.Nil(t, err)
		_, _ = pw.Write(bytes.Repeat([]byte{'q'}, 1000))
	})
	body := full[:len(full)-800]
	mr, err := NewMultipartReader(&chunkReader{data: body, max: 4}, "bnd", &MultipartReaderOptions{
		MaxDrainBytes: -1,
	})
	assert.Nil(t, err)

	p1, err := mr.NextPart()
	assert.Nil(t, err)
	d1, err := io.ReadAll(p1)
	assert.Nil(t, err)
	assert.DeepEqual(t, "v", string(d1))

	p2, err := mr.NextPart()
	assert.Nil(t, err)
	_, err = io.Copy(io.Discard, p2)
	me := &MultipartError{}
	assert.Assert(t, errors.As(err, &me))
	assert.DeepEqual(t, ErrMultipartTruncatedKind, me.Kind)
	assert.DeepEqual(t, 2, me.PartNumber())
	assert.Assert(t, mr.MustCloseConnection())
}

func TestMultipartReader_EarlyNextPartDrainWithinBudget(t *testing.T) {
	t.Parallel()
	boundary := "bnd"
	body := buildMultipartBody(t, boundary, func(w *multipart.Writer) {
		h := CreateMultipartHeader("skip", "s.bin", "")
		pw, err := w.CreatePart(h)
		assert.Nil(t, err)
		_, _ = pw.Write(bytes.Repeat([]byte{'s'}, 500))
		assert.Nil(t, w.WriteField("keep", "wanted"))
	})
	mr, err := NewMultipartReader(&chunkReader{data: body, max: 9}, boundary, &MultipartReaderOptions{
		MaxDrainBytes: 4096,
	})
	assert.Nil(t, err)

	p1, err := mr.NextPart()
	assert.Nil(t, err)
	// Consume only 10 bytes then advance: the remaining 490 must be
	// drained correctly without being mistaken for a part header. Like
	// a real TCP connection, individual reads may be short, so ask for
	// exactly 10 bytes.
	small := make([]byte, 10)
	n, err := io.ReadFull(p1, small)
	assert.Nil(t, err)
	assert.DeepEqual(t, 10, n)

	p2, err := mr.NextPart()
	assert.Nil(t, err)
	d, err := io.ReadAll(p2)
	assert.Nil(t, err)
	assert.DeepEqual(t, "keep", p2.FormName)
	assert.DeepEqual(t, "wanted", string(d))
	_, err = mr.NextPart()
	assert.DeepEqual(t, io.EOF, err)
	assert.Nil(t, mr.Close())
	assert.Assert(t, !mr.MustCloseConnection())
}

func TestMultipartReader_CloseExceedsDrainBudget(t *testing.T) {
	t.Parallel()
	boundary := "bnd"
	body := buildMultipartBody(t, boundary, func(w *multipart.Writer) {
		h := CreateMultipartHeader("huge", "h.bin", "")
		pw, err := w.CreatePart(h)
		assert.Nil(t, err)
		_, _ = pw.Write(bytes.Repeat([]byte{'h'}, 20000))
		assert.Nil(t, w.WriteField("tail", "x"))
	})
	mr, err := NewMultipartReader(&chunkReader{data: body, max: 32}, boundary, &MultipartReaderOptions{
		MaxDrainBytes: 128,
	})
	assert.Nil(t, err)

	p, err := mr.NextPart()
	assert.Nil(t, err)
	small := make([]byte, 16)
	_, _ = p.Read(small)

	err = mr.Close()
	me := &MultipartError{}
	assert.Assert(t, errors.As(err, &me))
	assert.DeepEqual(t, ErrMultipartDrainExceededKind, me.Kind)
	assert.Assert(t, mr.MustCloseConnection())
	// Idempotent.
	err2 := mr.Close()
	assert.Assert(t, errors.Is(err2, ErrMultipartDrainExceeded))

	// Further NextPart is rejected because the reader is closed.
	_, err = mr.NextPart()
	assert.Assert(t, errors.Is(err, ErrMultipartClosed))
}

func TestMultipartReader_FullConsumeCloseIsClean(t *testing.T) {
	t.Parallel()
	boundary := "bnd"
	body := buildMultipartBody(t, boundary, func(w *multipart.Writer) {
		assert.Nil(t, w.WriteField("k", "v"))
	})
	mr, err := NewMultipartReader(&chunkReader{data: body, max: 2}, boundary, nil)
	assert.Nil(t, err)
	parts := collectAllParts(t, mr)
	assert.DeepEqual(t, 1, len(parts))
	assert.Nil(t, mr.Close())
	assert.Nil(t, mr.Close())
	assert.Assert(t, !mr.MustCloseConnection())
}

func TestMultipartReader_EmptyMultipartBody(t *testing.T) {
	t.Parallel()
	body := "--bnd--\r\n"
	mr, err := NewMultipartReader(strings.NewReader(body), "bnd", nil)
	assert.Nil(t, err)
	_, err = mr.NextPart()
	assert.DeepEqual(t, io.EOF, err)
	assert.Nil(t, mr.Close())
}

func TestMultipartReader_Preamble(t *testing.T) {
	t.Parallel()
	// RFC 2046 preamble before the first boundary.
	body := "this is a preamble line\r\n--bnd\r\n" +
		"Content-Disposition: form-data; name=\"k\"\r\n\r\nv\r\n--bnd--\r\n"
	mr, err := NewMultipartReader(&chunkReader{data: []byte(body), max: 4}, "bnd", nil)
	assert.Nil(t, err)
	parts := collectAllParts(t, mr)
	assert.DeepEqual(t, 1, len(parts))
	assert.DeepEqual(t, "v", string(parts[0].content))
	assert.Nil(t, mr.Close())

	// Close before the first NextPart must drain the preamble and all
	// parts within the budget.
	mr2, err := NewMultipartReader(strings.NewReader(body), "bnd", &MultipartReaderOptions{
		MaxDrainBytes: -1,
	})
	assert.Nil(t, err)
	assert.Nil(t, mr2.Close())
	assert.Assert(t, !mr2.MustCloseConnection())
}

func TestMultipartReader_PartHeaderFoldingAndCustomHeaders(t *testing.T) {
	t.Parallel()
	body := "--bnd\r\n" +
		"Content-Disposition: form-data;\r\n" +
		" name=\"folded\"\r\n" +
		"X-Trace: abc\r\n" +
		"\r\nbody\r\n--bnd--\r\n"
	mr, err := NewMultipartReader(strings.NewReader(body), "bnd", nil)
	assert.Nil(t, err)
	p, err := mr.NextPart()
	assert.Nil(t, err)
	assert.DeepEqual(t, "folded", p.FormName)
	assert.DeepEqual(t, "abc", p.Header.Get("X-Trace"))
	d, err := io.ReadAll(p)
	assert.Nil(t, err)
	assert.DeepEqual(t, "body", string(d))
	assert.Nil(t, mr.Close())
}

func TestMultipartReader_QuotedFilenameWithEscapes(t *testing.T) {
	t.Parallel()
	body := "--bnd\r\n" +
		"Content-Disposition: form-data; name=\"f\"; filename=\"a\\\"q.txt\"\r\n" +
		"Content-Type: text/plain\r\n" +
		"\r\nhello\r\n--bnd--\r\n"
	mr, err := NewMultipartReader(&chunkReader{data: []byte(body), max: 5}, "bnd", nil)
	assert.Nil(t, err)
	p, err := mr.NextPart()
	assert.Nil(t, err)
	assert.DeepEqual(t, `a"q.txt`, p.FileName)
	assert.DeepEqual(t, "f", p.FormName)
	assert.Assert(t, p.IsFile)
	d, err := io.ReadAll(p)
	assert.Nil(t, err)
	assert.DeepEqual(t, "hello", string(d))
	assert.Nil(t, mr.Close())
}

func TestMultipartReader_UnknownContentLengthStream(t *testing.T) {
	t.Parallel()
	// A plain reader that carries no length: limits and counters must
	// still work (chunked transfer encoding case).
	boundary := "bnd"
	body := buildMultipartBody(t, boundary, func(w *multipart.Writer) {
		h := CreateMultipartHeader("f", "f", "")
		pw, err := w.CreatePart(h)
		assert.Nil(t, err)
		_, _ = pw.Write(bytes.Repeat([]byte{1}, 10000))
	})
	mr, err := NewMultipartReader(&chunkReader{data: body, max: 64}, boundary, &MultipartReaderOptions{
		MaxFileBytes:      1 << 20,
		MaxTotalFileBytes: 1 << 20,
	})
	assert.Nil(t, err)
	p, err := mr.NextPart()
	assert.Nil(t, err)
	n, err := io.Copy(io.Discard, p)
	assert.Nil(t, err)
	assert.DeepEqual(t, int64(10000), n)
	_, err = mr.NextPart()
	assert.DeepEqual(t, io.EOF, err)
	assert.DeepEqual(t, int64(len(body)), mr.BytesRead())
}

// ---- Request-level integration ----

func newReaderRequest(t *testing.T, body []byte, boundary string, stream bool) *Request {
	t.Helper()
	req := &Request{}
	req.Header.SetMultipartFormBoundary(boundary)
	if stream {
		req.SetBodyStream(bytes.NewReader(body), len(body))
	} else {
		req.SetBodyRaw(body)
	}
	return req
}

func TestRequest_MultipartReaderBufferedAndStream(t *testing.T) {
	t.Parallel()
	boundary := "ReqBnd"
	body := buildMultipartBody(t, boundary, func(w *multipart.Writer) {
		assert.Nil(t, w.WriteField("k", "v"))
		h := CreateMultipartHeader("f", "f.txt", "text/plain")
		pw, err := w.CreatePart(h)
		assert.Nil(t, err)
		_, _ = pw.Write([]byte("filebytes"))
	})
	for _, stream := range []bool{false, true} {
		req := newReaderRequest(t, body, boundary, stream)
		mr, err := req.MultipartReader(nil)
		assert.Nil(t, err)
		assert.Assert(t, req.IsMultipartReaderActive())
		parts := collectAllParts(t, mr)
		assert.DeepEqual(t, 2, len(parts))
		assert.DeepEqual(t, "filebytes", string(parts[1].content))
		assert.Nil(t, req.FinishMultipartReader())
		assert.Assert(t, !req.MultipartReaderNeedsClose())
	}
}

func TestRequest_MultipartReaderQuotedBoundaryParam(t *testing.T) {
	t.Parallel()
	boundary := "QuotedB"
	body := buildMultipartBody(t, boundary, func(w *multipart.Writer) {
		assert.Nil(t, w.WriteField("k", "v"))
	})
	req := &Request{}
	req.Header.SetContentTypeBytes([]byte(`multipart/form-data; boundary="QuotedB"`))
	req.SetBodyRaw(body)
	mr, err := req.MultipartReader(nil)
	assert.Nil(t, err)
	parts := collectAllParts(t, mr)
	assert.DeepEqual(t, 1, len(parts))
	assert.DeepEqual(t, "v", string(parts[0].content))
	assert.Nil(t, mr.Close())
}

func TestRequest_MultipartReaderMutexWithForm(t *testing.T) {
	t.Parallel()
	boundary := "bnd"
	body := buildMultipartBody(t, boundary, func(w *multipart.Writer) {
		assert.Nil(t, w.WriteField("k", "v"))
	})

	// Pre-parsed form -> reader rejected explicitly.
	req := &Request{}
	req.Header.SetMultipartFormBoundary(boundary)
	req.SetBodyRaw(body)
	_, err := req.MultipartForm()
	assert.Nil(t, err)
	_, err = req.MultipartReader(nil)
	assert.Assert(t, errors.Is(err, ErrMultipartPreParsed))

	// Active reader -> MultipartForm/FormFile/second reader rejected.
	req2 := &Request{}
	req2.Header.SetMultipartFormBoundary(boundary)
	req2.SetBodyRaw(body)
	mr, err := req2.MultipartReader(nil)
	assert.Nil(t, err)
	_, err = req2.MultipartReader(nil)
	assert.Assert(t, errors.Is(err, ErrMultipartReaderConflict))
	_, err = req2.MultipartForm()
	assert.Assert(t, errors.Is(err, ErrMultipartReaderConflict))
	_, err = req2.FormFile("k")
	assert.Assert(t, errors.Is(err, ErrMultipartReaderConflict))
	assert.Nil(t, mr.Close())

	// Content-Encoding is rejected for the streaming reader.
	req3 := &Request{}
	req3.Header.SetMultipartFormBoundary(boundary)
	req3.SetHeader("Content-Encoding", "gzip")
	req3.SetBodyRaw(body)
	_, err = req3.MultipartReader(nil)
	assert.Assert(t, errors.Is(err, ErrMultipartUnsupportedEncoding))
}

func TestRequest_MultipartReaderResetClearsState(t *testing.T) {
	t.Parallel()
	boundary := "bnd"
	body := buildMultipartBody(t, boundary, func(w *multipart.Writer) {
		assert.Nil(t, w.WriteField("k", "v"))
	})
	req := &Request{}
	req.Header.SetMultipartFormBoundary(boundary)
	req.SetBodyRaw(body)
	mr, err := req.MultipartReader(nil)
	assert.Nil(t, err)
	p, err := mr.NextPart()
	assert.Nil(t, err)
	assert.DeepEqual(t, "k", p.FormName)

	req.Reset()
	assert.Assert(t, !req.IsMultipartReaderActive())
	assert.Assert(t, !req.HasMultipartForm())
}
