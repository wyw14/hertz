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

package render

import (
	"testing"

	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// ---------------------------------------------------------------------------
// Accept parsing
// ---------------------------------------------------------------------------

func TestParseAccept_QAndOrder(t *testing.T) {
	ranges, present := parseAccept([]string{"text/plain;q=0.5, application/json;q=0.8, */*;q=0.1"})
	if !present || len(ranges) != 3 {
		t.Fatalf("present=%v len=%d, want 3 ranges", present, len(ranges))
	}
	want := []struct {
		typ string
		sub string
		q   float64
	}{
		{"text", "plain", 0.5},
		{"application", "json", 0.8},
		{"*", "*", 0.1},
	}
	for i, w := range want {
		if ranges[i].typ != w.typ || ranges[i].subtype != w.sub || ranges[i].q != w.q {
			t.Fatalf("range[%d]=%s/%s q=%v, want %s/%s q=%v",
				i, ranges[i].typ, ranges[i].subtype, ranges[i].q, w.typ, w.sub, w.q)
		}
	}
}

func TestParseAccept_QuotedParameter(t *testing.T) {
	// A semicolon/comma inside a quoted value must not split the range, and
	// quoted-pair escapes must be unescaped.
	ranges, _ := parseAccept([]string{`application/json; charset="utf8; weird, x"; level="a\"b";q=0.9`})
	if len(ranges) != 1 {
		t.Fatalf("len=%d, want 1: %+v", len(ranges), ranges)
	}
	m := map[string]string{}
	for _, p := range ranges[0].params {
		m[p.key] = p.value
	}
	if m["charset"] != "utf8; weird, x" || m["level"] != `a"b` {
		t.Fatalf("unexpected params: %+v", m)
	}
	if ranges[0].q != 0.9 {
		t.Fatalf("q=%v, want 0.9", ranges[0].q)
	}
}

func TestParseAccept_DuplicateRangeFirstWins(t *testing.T) {
	// Decisions must not be made by string-contains or by the last comma
	// item: the first occurrence of a duplicate media range fixes its q.
	rs, _ := parseAccept([]string{"text/plain;q=0, text/plain"})
	if len(rs) != 1 || rs[0].q != 0 {
		t.Fatalf("duplicate range must keep first q, got %+v", rs)
	}
	// The same rule applies when the field arrives as two header fields.
	rs, _ = parseAccept([]string{"text/plain;q=0.3", "text/plain"})
	if len(rs) != 1 || rs[0].q != 0.3 {
		t.Fatalf("duplicate across fields must keep first q, got %+v", rs)
	}
}

func TestParseAccept_MalformedIgnored(t *testing.T) {
	if rs, present := parseAccept([]string{"text/plain;q=2, application/json"}); !present || len(rs) != 1 ||
		rs[0].typ != "application" {
		t.Fatalf("bad q range should be dropped, got %+v present=%v", rs, present)
	}
	if rs, present := parseAccept(nil); present || len(rs) != 0 {
		t.Fatalf("missing Accept must report absent, got %+v", rs)
	}
	if rs, present := parseAccept([]string{"   "}); !present || len(rs) != 0 {
		t.Fatalf("blank Accept yields no ranges, got %+v", rs)
	}
}

func TestMediaType_SpecificityAndParams(t *testing.T) {
	jsonMT := mustParseMediaType(t, "application/json; charset=utf-8; version=1")
	htmlMT := mustParseMediaType(t, "text/html")
	ranges, _ := parseAccept([]string{"*/*;q=0.1, application/*;q=0.5, application/json;version=1;charset=utf-8;q=0.9, text/html"})

	if q := mediaQuality(jsonMT, ranges, true); q != 0.9 {
		t.Fatalf("json q=%v, want exact range 0.9 despite wildcard entries", q)
	}
	if q := mediaQuality(htmlMT, ranges, true); q != 1 {
		t.Fatalf("html q=%v, want default 1 from exact text/html range", q)
	}
	// Constrained parameter must match; charset is compared case-insensitively.
	noVer := mustParseMediaType(t, "application/json; charset=UTF-8")
	if q := mediaQuality(noVer, ranges, true); q != 0.5 {
		t.Fatalf("json without version should fall through to application/*, got %v", q)
	}
	badCharset := mustParseMediaType(t, "application/json; version=1; charset=us-ascii")
	if q := mediaQuality(badCharset, ranges, true); q != 0.5 {
		t.Fatalf("charset mismatch must fall through, got %v", q)
	}
	absent := mustParseMediaType(t, "image/png")
	if q := mediaQuality(absent, ranges, true); q != 0.1 {
		t.Fatalf("png q=%v, want wildcard 0.1", q)
	}
	if q := mediaQuality(absent, nil, false); q != 1 {
		t.Fatalf("missing Accept means anything acceptable, got %v", q)
	}
}

func mustParseMediaType(t *testing.T, s string) mediaType {
	t.Helper()
	mt, err := parseMediaType(s)
	if err != nil {
		t.Fatalf("parseMediaType(%q): %s", s, err)
	}
	return mt
}

// ---------------------------------------------------------------------------
// Accept-Encoding parsing
// ---------------------------------------------------------------------------

func TestParseAcceptEncoding(t *testing.T) {
	prefs, present := parseAcceptEncoding(nil)
	if present {
		t.Fatalf("missing field must be absent")
	}
	prefs, present = parseAcceptEncoding([]string{"gzip;q=0, br, identity;q=0.5, *;q=0.1"})
	if !present || len(prefs) != 4 {
		t.Fatalf("present=%v len=%d", present, len(prefs))
	}
	cases := map[string]float64{
		"gzip":     0, // explicit refusal, not string-contains matching
		"br":       1,
		"identity": 0.5, // explicit identity wins over star
		"deflate":  0.1, // star fallback
		"zstd":     0.1,
	}
	for coding, want := range cases {
		if got := prefs.quality(coding); got != want {
			t.Fatalf("quality(%q)=%v, want %v", coding, got, want)
		}
	}
}

func TestParseAcceptEncoding_IdentityDefaults(t *testing.T) {
	// Field present, identity not mentioned, no star: identity still OK,
	// unlisted codings are refused.
	prefs, _ := parseAcceptEncoding([]string{"gzip"})
	if q := prefs.quality("identity"); q != 1 {
		t.Fatalf("identity default q=%v, want 1", q)
	}
	if q := prefs.quality("br"); q != 0 {
		t.Fatalf("unlisted br q=%v, want 0", q)
	}
	// identity explicitly refused.
	prefs, _ = parseAcceptEncoding([]string{"gzip, identity;q=0"})
	if q := prefs.quality("identity"); q != 0 {
		t.Fatalf("identity q=%v, want 0", q)
	}
	// star refusal without a specific identity entry also disables identity.
	prefs, _ = parseAcceptEncoding([]string{"*;q=0, gzip;q=1"})
	if q := prefs.quality("identity"); q != 0 {
		t.Fatalf("identity under *;q=0: q=%v, want 0", q)
	}
	if q := prefs.quality("gzip"); q != 1 {
		t.Fatalf("exact gzip must override star: q=%v", q)
	}
	// Duplicate coding keeps the first q.
	prefs, _ = parseAcceptEncoding([]string{"gzip;q=0, gzip;q=1"})
	if q := prefs.quality("gzip"); q != 0 {
		t.Fatalf("duplicate gzip first-wins q=%v", q)
	}
}

// ---------------------------------------------------------------------------
// Entity-tags
// ---------------------------------------------------------------------------

func TestValidateEntityTag(t *testing.T) {
	valid := []string{`""`, `"abc"`, `"a-b?x=1"`, `W/"weak"`, `"!"`, `"` + string(rune(0x80)) + `"`}
	for _, v := range valid {
		if err := validateEntityTag(v); err != nil {
			t.Fatalf("expected %q valid: %s", v, err)
		}
	}
	invalid := []string{"", "abc", `"abc`, `abc"`, "W/abc", `W/"abc`, `"a"b`, `"a b"`}
	for _, v := range invalid {
		if err := validateEntityTag(v); err == nil {
			t.Fatalf("expected %q invalid", v)
		}
	}
}

func TestIfNoneMatch(t *testing.T) {
	if !ifNoneMatch([]string{`"v1", "v2"`}, `"v2"`) {
		t.Fatal("exact strong tag in list must match")
	}
	if !ifNoneMatch([]string{`W/"v1"`}, `"v1"`) {
		t.Fatal("weak comparison must match W/\"v1\" against \"v1\"")
	}
	if !ifNoneMatch([]string{` w/"v1" `}, `W/"v1"`) {
		t.Fatal("whitespace and marker case must not break weak comparison")
	}
	if !ifNoneMatch([]string{"*"}, `"anything"`) {
		t.Fatal("star must match any representation")
	}
	if ifNoneMatch([]string{`"v1"`}, `"v2"`) {
		t.Fatal("different opaque tags must not match")
	}
	if ifNoneMatch([]string{"garbage"}, `"v1"`) {
		t.Fatal("malformed tags must not match")
	}
	if ifNoneMatch(nil, `"v1"`) {
		t.Fatal("missing header must not match")
	}
}

// ---------------------------------------------------------------------------
// Configuration validation
// ---------------------------------------------------------------------------

func TestNewRepresentationSet_InvalidConfig(t *testing.T) {
	renderer := RepresentationFunc(func(resp *protocol.Response) error { return nil })
	cases := map[string][]Representation{
		"empty": nil,
		"nil renderer": {{
			MediaType: "application/json",
			ETag:      `"v1"`,
		}},
		"wildcard media type": {{
			MediaType: "application/*",
			ETag:      `"v1"`,
			Renderer:  renderer,
		}},
		"malformed media type": {{
			MediaType: "application",
			ETag:      `"v1"`,
			Renderer:  renderer,
		}},
		"invalid etag": {{
			MediaType: "application/json",
			ETag:      "v1",
			Renderer:  renderer,
		}},
		"layered coding": {{
			MediaType: "application/json",
			Encoding:  "gzip, br",
			ETag:      `"v1"`,
			Renderer:  renderer,
		}},
		"coding with params": {{
			MediaType: "application/json",
			Encoding:  "gzip;level=9",
			ETag:      `"v1"`,
			Renderer:  renderer,
		}},
		"negative length": {{
			MediaType:     "application/json",
			ETag:          `"v1"`,
			ContentLength: -1,
			Renderer:      renderer,
		}},
		"bad qs": {{
			MediaType: "application/json",
			ETag:      `"v1"`,
			QS:        2,
			Renderer:  renderer,
		}},
	}
	for name, reps := range cases {
		if _, err := NewRepresentationSet(reps...); err == nil {
			t.Fatalf("case %q: expected construction error", name)
		}
	}

	// Same identity (media type incl. parameters + coding) duplicated.
	dup := []Representation{
		{MediaType: `application/json; charset="utf-8"`, ETag: `"a"`, Renderer: renderer},
		{MediaType: "application/json;charset=utf-8", ETag: `"b"`, Renderer: renderer},
	}
	if _, err := NewRepresentationSet(dup...); err == nil {
		t.Fatal("duplicate identity must be rejected")
	}

	// Same content, different coding but identical strong ETag: rejected,
	// because coded variants need their own validators.
	dupTag := []Representation{
		{MediaType: "application/json", ETag: `"v1"`, Renderer: renderer},
		{MediaType: "application/json", Encoding: "gzip", ETag: `W/"v1"`, Renderer: renderer},
	}
	if _, err := NewRepresentationSet(dupTag...); err == nil {
		t.Fatal("weak-equal ETags across variants must be rejected")
	}
}

func TestNewRepresentationSet_Canonicalization(t *testing.T) {
	var called int
	set, err := NewRepresentationSet(
		Representation{
			MediaType: `Application/JSON; Charset="UTF-8"; Version=1`,
			Encoding:  "x-gzip",
			ETag:      `"g"`,
			Renderer: RepresentationFunc(func(resp *protocol.Response) error {
				called++
				return nil
			}),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	got := set.reps[0]
	if got.encoding != "gzip" {
		t.Fatalf("x-gzip alias not canonicalized: %q", got.encoding)
	}
	if got.rep.MediaType != "application/json; charset=utf-8; version=1" {
		t.Fatalf("media type not canonical: %q", got.rep.MediaType)
	}
	if got.rep.QS != 1 {
		t.Fatalf("zero QS should default to 1, got %v", got.rep.QS)
	}
}

// ---------------------------------------------------------------------------
// End-to-end Negotiate over protocol.Request / protocol.Response
// ---------------------------------------------------------------------------

type countingRenderer struct {
	calls *int
	body  []byte
}

func (r countingRenderer) Render(resp *protocol.Response) error {
	*r.calls++
	resp.AppendBody(r.body)
	return nil
}

func newNegotiateSet(t *testing.T) (set *RepresentationSet, plainCalls, gzipCalls, xmlCalls *int) {
	t.Helper()
	plainCalls, gzipCalls, xmlCalls = new(int), new(int), new(int)
	plainBody := []byte(`{"hello":"world"}`)
	gzipBody := []byte("FAKE-GZIP-BYTES")
	xmlBody := []byte("<hello>world</hello>")
	set, err := NewRepresentationSet(
		Representation{
			MediaType:     "application/json; charset=utf-8",
			ETag:          `"v1-json"`,
			ContentLength: len(plainBody),
			QS:            0.8,
			Renderer:      countingRenderer{calls: plainCalls, body: plainBody},
		},
		Representation{
			MediaType:     "application/json; charset=utf-8",
			Encoding:      "gzip",
			ETag:          `"v1-json-gzip"`,
			ContentLength: len(gzipBody),
			// Server prefers the coded variant when the client rates both
			// codings equally.
			QS:       0.9,
			Renderer: countingRenderer{calls: gzipCalls, body: gzipBody},
		},
		Representation{
			MediaType:     "application/xml",
			ETag:          `"v1-xml"`,
			ContentLength: len(xmlBody),
			Renderer:      countingRenderer{calls: xmlCalls, body: xmlBody},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return set, plainCalls, gzipCalls, xmlCalls
}

func negotiateRequest(t *testing.T, set *RepresentationSet, method string, headers map[string]string) (*protocol.Response, error) {
	t.Helper()
	var req protocol.Request
	req.Header.SetMethod(method)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp := &protocol.Response{}
	err := set.Negotiate(&req, resp)
	return resp, err
}

func TestNegotiate_Get200Identity(t *testing.T) {
	set, plain, gzip, xml := newNegotiateSet(t)
	resp, err := negotiateRequest(t, set, "GET", map[string]string{
		"Accept":          "application/json",
		"Accept-Encoding": "identity",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode() != consts.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode())
	}
	if got := string(resp.Body()); got != `{"hello":"world"}` {
		t.Fatalf("body=%q", got)
	}
	if ct := string(resp.Header.ContentType()); ct != "application/json; charset=utf-8" {
		t.Fatalf("content-type=%q", ct)
	}
	if ce := resp.Header.ContentEncoding(); len(ce) != 0 {
		t.Fatalf("identity must not carry Content-Encoding, got %q", ce)
	}
	if etag := resp.Header.Get("Etag"); etag != `"v1-json"` {
		t.Fatalf("etag=%q", etag)
	}
	assertVary(t, resp, "Accept", "Accept-Encoding")
	if *plain != 1 || *gzip != 0 || *xml != 0 {
		t.Fatalf("renderer calls plain=%d gzip=%d xml=%d, want exactly one selected render", *plain, *gzip, *xml)
	}
}

func TestNegotiate_Get200GzipOwnValidator(t *testing.T) {
	set, plain, gzip, _ := newNegotiateSet(t)
	resp, err := negotiateRequest(t, set, "GET", map[string]string{
		"Accept":          "application/json",
		"Accept-Encoding": "gzip",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode() != consts.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode())
	}
	if ce := string(resp.Header.ContentEncoding()); ce != "gzip" {
		t.Fatalf("content-encoding=%q", ce)
	}
	if etag := resp.Header.Get("Etag"); etag != `"v1-json-gzip"` {
		t.Fatalf("gzip bytes must carry their own ETag, got %q", etag)
	}
	if body := string(resp.Body()); body != "FAKE-GZIP-BYTES" {
		t.Fatalf("body=%q", body)
	}
	if *gzip != 1 || *plain != 0 {
		t.Fatalf("calls gzip=%d plain=%d; the gzip representation must be the only rendered candidate", *gzip, *plain)
	}
}

func TestNegotiate_HeadDoesNotRender(t *testing.T) {
	set, plain, gzip, _ := newNegotiateSet(t)
	resp, err := negotiateRequest(t, set, "HEAD", map[string]string{
		"Accept":          "application/json",
		"Accept-Encoding": "gzip",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode() != consts.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode())
	}
	if len(resp.Body()) != 0 {
		t.Fatalf("HEAD must have no body, got %q", resp.Body())
	}
	if ce := string(resp.Header.ContentEncoding()); ce != "gzip" {
		t.Fatalf("content-encoding=%q", ce)
	}
	if etag := resp.Header.Get("Etag"); etag != `"v1-json-gzip"` {
		t.Fatalf("etag=%q", etag)
	}
	if cl := resp.Header.ContentLength(); cl != len("FAKE-GZIP-BYTES") {
		t.Fatalf("HEAD content-length=%d, want coded length %d", cl, len("FAKE-GZIP-BYTES"))
	}
	if *gzip != 0 || *plain != 0 {
		t.Fatalf("HEAD must not render any body: gzip=%d plain=%d", *gzip, *plain)
	}
}

func TestNegotiate_ConditionalGetAndHead304(t *testing.T) {
	for _, method := range []string{"GET", "HEAD"} {
		set, plain, gzip, _ := newNegotiateSet(t)
		resp, err := negotiateRequest(t, set, method, map[string]string{
			"Accept":          "application/json",
			"Accept-Encoding": "gzip",
			"If-None-Match":   `"v1-json-gzip"`,
		})
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode() != consts.StatusNotModified {
			t.Fatalf("%s status=%d, want 304", method, resp.StatusCode())
		}
		if len(resp.Body()) != 0 {
			t.Fatalf("%s 304 must have no body", method)
		}
		if etag := resp.Header.Get("Etag"); etag != `"v1-json-gzip"` {
			t.Fatalf("%s etag=%q", method, etag)
		}
		assertVary(t, resp, "Accept", "Accept-Encoding")
		if *gzip != 0 {
			t.Fatalf("%s 304 must not render: gzip calls=%d", method, *gzip)
		}
		if *plain != 0 {
			t.Fatalf("%s 304 must not render: plain calls=%d", method, *plain)
		}
	}
}

func TestNegotiate_ConditionalWeakAndStar(t *testing.T) {
	set, _, gzip, _ := newNegotiateSet(t)
	resp, err := negotiateRequest(t, set, "GET", map[string]string{
		"Accept":          "application/json",
		"Accept-Encoding": "gzip",
		"If-None-Match":   `W/"v1-json-gzip"`,
	})
	if err != nil || resp.StatusCode() != consts.StatusNotModified {
		t.Fatalf("weak INM against strong validator: status=%d err=%v", resp.StatusCode(), err)
	}
	if *gzip != 0 {
		t.Fatal("304 must not render")
	}

	set2, plain2, _, _ := newNegotiateSet(t)
	resp2, err := negotiateRequest(t, set2, "GET", map[string]string{
		"Accept":        "application/json",
		"If-None-Match": "*",
	})
	if err != nil || resp2.StatusCode() != consts.StatusNotModified {
		t.Fatalf("INM *: status=%d err=%v", resp2.StatusCode(), err)
	}
	if *plain2 != 0 {
		t.Fatal("304 must not render")
	}
}

func TestNegotiate_OtherMethodsNot304(t *testing.T) {
	set, plain, _, _ := newNegotiateSet(t)
	resp, err := negotiateRequest(t, set, "POST", map[string]string{
		"Accept":          "application/json",
		"Accept-Encoding": "identity",
		"If-None-Match":   `"v1-json"`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode() != consts.StatusOK {
		t.Fatalf("POST status=%d, must not be 304", resp.StatusCode())
	}
	if *plain != 1 {
		t.Fatalf("POST 200 must render once, got %d", *plain)
	}
}

func TestNegotiate_406(t *testing.T) {
	set, plain, gzip, xml := newNegotiateSet(t)
	resp, err := negotiateRequest(t, set, "GET", map[string]string{
		"Accept":          "image/webp",
		"Accept-Encoding": "identity;q=0, *;q=0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode() != consts.StatusNotAcceptable {
		t.Fatalf("status=%d, want 406", resp.StatusCode())
	}
	if len(resp.Body()) != 0 {
		t.Fatal("406 must not render a candidate body")
	}
	assertVary(t, resp, "Accept", "Accept-Encoding")
	if *plain+*gzip+*xml != 0 {
		t.Fatalf("406 must not invoke any renderer: %d/%d/%d", *plain, *gzip, *xml)
	}
}

func TestNegotiate_IdentityDisabledPicksGzip(t *testing.T) {
	set, plain, gzip, _ := newNegotiateSet(t)
	resp, err := negotiateRequest(t, set, "GET", map[string]string{
		"Accept":          "application/json",
		"Accept-Encoding": "identity;q=0, gzip",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode() != consts.StatusOK || string(resp.Header.ContentEncoding()) != "gzip" {
		t.Fatalf("status=%d encoding=%q", resp.StatusCode(), resp.Header.ContentEncoding())
	}
	if *gzip != 1 || *plain != 0 {
		t.Fatalf("calls gzip=%d plain=%d", *gzip, *plain)
	}
}

func TestNegotiate_ExistingRenderCandidate(t *testing.T) {
	// An existing Render (Data) already satisfies RepresentationRenderer.
	// Even though Data carries its own ContentType, the negotiated headers
	// must come from the set metadata (here: text/plain wins over the
	// renderer's stale text/html claim).
	body := []byte("hello render")
	set, err := NewRepresentationSet(Representation{
		MediaType:     "text/plain; charset=utf-8",
		ETag:          `"data-1"`,
		ContentLength: len(body),
		Renderer:      Data{ContentType: "text/html", Data: body},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := negotiateRequest(t, set, "GET", map[string]string{"Accept": "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode() != consts.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode())
	}
	if string(resp.Body()) != string(body) {
		t.Fatalf("body=%q", resp.Body())
	}
	if ct := string(resp.Header.ContentType()); ct != "text/plain; charset=utf-8" {
		t.Fatalf("metadata Content-Type must win after render, got %q", ct)
	}
}

func TestNegotiate_MissingHeadersAndServerWeight(t *testing.T) {
	// No Accept / Accept-Encoding: both json variants qualify at q=1;
	// server weight is equal (0.8), xml defaults to 1, so xml wins... no:
	// json reps register first with equal client weights; qs 0.8 < xml 1,
	// so xml is selected by server preference.
	set, _, _, xml := newNegotiateSet(t)
	resp, err := negotiateRequest(t, set, "GET", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Header.Get("Etag"); got != `"v1-xml"` {
		t.Fatalf("server weight tiebreak failed: etag=%q", got)
	}
	if *xml != 1 {
		t.Fatalf("xml renderer calls=%d", *xml)
	}
}

func TestNegotiate_PerRequestStateIsolation(t *testing.T) {
	set, plain, gzip, _ := newNegotiateSet(t)
	// Two sequential requests on the same set must not leak selections.
	resp1, err := negotiateRequest(t, set, "GET", map[string]string{
		"Accept":          "application/json",
		"Accept-Encoding": "gzip",
	})
	if err != nil {
		t.Fatal(err)
	}
	resp2, err := negotiateRequest(t, set, "GET", map[string]string{
		"Accept":          "application/json",
		"Accept-Encoding": "identity;q=0, gzip;q=0, *;q=0",
	})
	if err != nil {
		t.Fatal(err)
	}
	// No Accept-Encoding: any coding is acceptable (RFC 7231, 5.3.4), so the
	// server-side weight decides and the gzip variant is preferred.
	resp3, err := negotiateRequest(t, set, "GET", map[string]string{"Accept": "application/json"})
	if err != nil {
		t.Fatal(err)
	}
	if string(resp1.Header.ContentEncoding()) != "gzip" {
		t.Fatalf("first response encoding=%q", resp1.Header.ContentEncoding())
	}
	if resp2.StatusCode() != consts.StatusNotAcceptable {
		t.Fatalf("second response status=%d, want fresh 406", resp2.StatusCode())
	}
	if ce := resp3.Header.ContentEncoding(); string(ce) != "gzip" {
		t.Fatalf("third response must independently select by server weight, got %q", ce)
	}
	if *gzip != 2 || *plain != 0 {
		t.Fatalf("calls plain=%d gzip=%d", *plain, *gzip)
	}
}

// ---------------------------------------------------------------------------
// Vary merging
// ---------------------------------------------------------------------------

func TestMergeVary(t *testing.T) {
	var resp protocol.Response
	resp.Header.Add("Vary", "X-Test")
	resp.Header.Add("Vary", "accept, accept-encoding , X-Other")
	mergeVary(&resp, "Accept", "Accept-Encoding", "Accept-Language")
	got := resp.Header.GetAll("Vary")
	if len(got) != 1 {
		t.Fatalf("expected a single merged Vary header, got %v", got)
	}
	want := map[string]bool{
		"X-Test": true, "accept": true, "accept-encoding": true,
		"X-Other": true, "Accept-Language": true,
	}
	for _, item := range splitList(got[0], ",") {
		token := trimSpace(item)
		if !want[token] {
			t.Fatalf("unexpected or duplicated Vary token %q in %q", token, got[0])
		}
		delete(want, token)
	}
	if len(want) != 0 {
		t.Fatalf("missing Vary tokens: %v", want)
	}
}

func TestMergeVary_WildcardPreserved(t *testing.T) {
	var resp protocol.Response
	resp.Header.Set("Vary", "*")
	mergeVary(&resp, "Accept", "Accept-Encoding")
	if got := resp.Header.Get("Vary"); got != "*" {
		t.Fatalf("existing Vary:* must be preserved, got %q", got)
	}
}

func trimSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}

func assertVary(t *testing.T, resp *protocol.Response, want ...string) {
	t.Helper()
	got := resp.Header.Get("Vary")
	for _, w := range want {
		found := false
		for _, item := range splitList(got, ",") {
			if equalFoldASCII(trimSpace(item), w) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Vary=%q missing %q", got, w)
		}
	}
}

func equalFoldASCII(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		x, y := a[i], b[i]
		if x >= 'A' && x <= 'Z' {
			x += 'a' - 'A'
		}
		if y >= 'A' && y <= 'Z' {
			y += 'a' - 'A'
		}
		if x != y {
			return false
		}
	}
	return true
}
