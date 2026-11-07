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
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// Content negotiation header names, in addition to the ones in consts.
const (
	headerETag        = "ETag"
	headerVary        = "Vary"
	headerIfNoneMatch = "If-None-Match"

	identityCoding = "identity"
)

// RepresentationRenderer writes the body of a selected representation.
//
// Every existing Render (JSONRender, Data, String, ...) already satisfies
// this interface, so an already-built renderer can be registered directly.
// RepresentationFunc adapts ordinary functions.
//
// The renderer is invoked at most once per request, and only when a 200
// response with an entity body is actually sent. It must produce the final,
// already content-coded bytes; the negotiator itself does not compress or
// otherwise transform the body.
type RepresentationRenderer interface {
	Render(resp *protocol.Response) error
}

// RepresentationFunc is an adapter allowing an ordinary function to be used
// as a RepresentationRenderer.
type RepresentationFunc func(resp *protocol.Response) error

// Render implements RepresentationRenderer.
func (f RepresentationFunc) Render(resp *protocol.Response) error {
	return f(resp)
}

// Representation is one explicitly available variant of a resource:
// a concrete media type (with parameters), a single content coding, the
// validator and length of the FINAL (already coded) representation, and the
// unique renderer able to produce its body.
//
// A gzip variant and an identity variant of the same content are different
// representations and must carry different strong entity-tags, because the
// strong validator identifies the exact byte sequence of the selected
// representation.
//
// All fields are static metadata. They never carry per-request selection
// state, so a Representation may be shared across concurrent requests.
type Representation struct {
	// MediaType is the full media type of this representation, including
	// parameters, e.g. "application/json; charset=utf-8". Wildcards are not
	// allowed.
	MediaType string

	// Encoding is the single content coding (RFC 7231, section 3.1.2.1)
	// applied to the body, e.g. "gzip". An empty string or "identity" means
	// no content coding. Layered codings such as "gzip, br" are rejected at
	// set construction time; the negotiator does not implement compression.
	Encoding string

	// ETag is the entity-tag (RFC 7232) of THIS representation, e.g.
	// `"v1-json-gzip"` for a strong validator or `W/"v1"` for a weak one.
	// A strong validator of the identity representation must not be reused
	// for the bytes of a content-coded representation.
	ETag string

	// ContentLength is the length of the final coded entity body. It is
	// static metadata and is used for HEAD responses without invoking the
	// renderer.
	ContentLength int

	// QS is the server-side preference weight in [0, 1]. It only breaks
	// ties between representations that the client rates equally. A zero
	// value is treated as 1 (no preference); declaration order is the
	// final tiebreaker.
	QS float64

	// Renderer produces the final coded body when this representation is
	// selected and a 200 entity body is required. It must not be nil.
	Renderer RepresentationRenderer
}

// preparedRepresentation is the validated, canonicalized form of a
// Representation. A RepresentationSet only contains immutable values after
// successful construction, so concurrent reads need no synchronization.
type preparedRepresentation struct {
	rep      Representation
	media    mediaType
	encoding string
}

// RepresentationSet is an explicit, immutable collection of representations
// of one resource. Build it once with NewRepresentationSet and reuse it for
// every request; Negotiate holds no per-request state on the set.
type RepresentationSet struct {
	reps []preparedRepresentation
}

// NewRepresentationSet validates the given representations and returns the
// immutable set used by Negotiate.
//
// Construction fails before any request is processed when:
//   - no representation is given or a renderer is missing;
//   - a media type is missing, malformed or contains a wildcard;
//   - an encoding is not a single valid content coding (layered codings,
//     separators or invalid tokens are rejected);
//   - an entity-tag does not match the RFC 7232 grammar;
//   - ContentLength is negative or QS is out of [0, 1];
//   - two representations share the same identity (media type including
//     parameters plus content coding) or weak-equal entity-tags.
func NewRepresentationSet(reps ...Representation) (*RepresentationSet, error) {
	if len(reps) == 0 {
		return nil, errors.New("render: representation set must not be empty")
	}
	prepared := make([]preparedRepresentation, 0, len(reps))
	identities := make(map[string]int, len(reps))
	tags := make(map[string]int, len(reps))
	for i, r := range reps {
		if r.Renderer == nil {
			return nil, fmt.Errorf("render: representation #%d (%q) has no renderer", i, r.MediaType)
		}
		if r.ContentLength < 0 {
			return nil, fmt.Errorf("render: representation #%d (%q) has negative ContentLength", i, r.MediaType)
		}
		if r.QS < 0 || r.QS > 1 {
			return nil, fmt.Errorf("render: representation #%d (%q) has invalid QS %g", i, r.MediaType, r.QS)
		}
		if r.QS == 0 {
			r.QS = 1
		}
		mt, err := parseMediaType(r.MediaType)
		if err != nil {
			return nil, fmt.Errorf("render: representation #%d has invalid media type: %w", i, err)
		}
		enc, err := canonicalEncoding(r.Encoding)
		if err != nil {
			return nil, fmt.Errorf("render: representation #%d (%q) has invalid content coding: %w", i, r.MediaType, err)
		}
		if err := validateEntityTag(r.ETag); err != nil {
			return nil, fmt.Errorf("render: representation #%d (%q): %w", i, r.MediaType, err)
		}
		identity := mt.identityKey() + "|" + enc
		if prev, ok := identities[identity]; ok {
			return nil, fmt.Errorf("render: duplicate representation %q with encoding %q (declared at #%d and #%d)",
				mt.canonical(), enc, prev, i)
		}
		identities[identity] = i
		tagKey := opaqueTag(r.ETag)
		if prev, ok := tags[tagKey]; ok {
			return nil, fmt.Errorf("render: representations #%d and #%d use the same entity-tag %q; "+
				"content-coded variants need their own validators", prev, i, r.ETag)
		}
		tags[tagKey] = i

		r.MediaType = mt.canonical()
		r.Encoding = enc
		prepared = append(prepared, preparedRepresentation{
			rep:      r,
			media:    mt,
			encoding: enc,
		})
	}
	return &RepresentationSet{reps: prepared}, nil
}

// MustRepresentationSet is like NewRepresentationSet but panics on
// construction error. It is intended for package-level sets built from
// static, known-good configuration.
func MustRepresentationSet(reps ...Representation) *RepresentationSet {
	s, err := NewRepresentationSet(reps...)
	if err != nil {
		panic(err)
	}
	return s
}

// Negotiate performs server-driven content negotiation for one request:
// it selects at most one representation based on the request Accept and
// Accept-Encoding header fields together with the server-side weights, and
// either responds 406 (nothing acceptable) or invokes the selected
// representation's renderer exactly once.
//
// Negotiate only reads static metadata while selecting; it never renders
// candidate bodies up front. Per-request state is kept on the stack and is
// never stored on the set, the representations, or pooled header slices.
//
// Conditional requests: for GET and HEAD only, If-None-Match is evaluated
// against the selected representation using HTTP weak comparison (RFC 7232,
// section 2.3.2); a match produces 304 without invoking the renderer. Other
// request methods never receive 304 here.
//
// The renderer is not invoked for HEAD (the static ContentLength is used)
// or for 304 responses. Content-Type, Content-Encoding, ETag and the merged
// Vary header always describe the selected representation.
//
// The negotiator deliberately does not implement Range handling, cache
// storage, response compression or other conditional headers; the body of
// every representation is supplied by its own renderer (an existing Render
// or a caller-provided function).
func (s *RepresentationSet) Negotiate(req *protocol.Request, resp *protocol.Response) error {
	// GetAll copies values into fresh strings, so the negotiation below
	// never retains references into pooled request header storage.
	ranges, acceptPresent := parseAccept(req.Header.GetAll(consts.HeaderAccept))
	codings, encodingPresent := parseAcceptEncoding(req.Header.GetAll(consts.HeaderAcceptEncoding))

	best := -1
	bestMQ, bestEQ, bestQS := 0.0, 0.0, 0.0
	for i := range s.reps {
		p := &s.reps[i]
		mq := mediaQuality(p.media, ranges, acceptPresent)
		if mq <= 0 {
			continue
		}
		eq := 1.0
		if encodingPresent {
			eq = codings.quality(p.encoding)
		}
		if eq <= 0 {
			continue
		}
		// Preference order: client media-type quality, then client
		// content-coding quality, then the server-side weight, and finally
		// the stable declaration order.
		if best == -1 || mq > bestMQ ||
			(mq == bestMQ && eq > bestEQ) ||
			(mq == bestMQ && eq == bestEQ && p.rep.QS > bestQS) {
			best, bestMQ, bestEQ, bestQS = i, mq, eq, p.rep.QS
		}
	}

	// Vary applies regardless of the outcome: caches must distinguish
	// responses that vary on Accept and/or Accept-Encoding, including 406.
	mergeVary(resp, consts.HeaderAccept, consts.HeaderAcceptEncoding)

	if best == -1 {
		resp.ResetBody()
		resp.SetStatusCode(consts.StatusNotAcceptable)
		return nil
	}

	p := &s.reps[best]
	method := req.Header.Method()
	isGet := bytes.Equal(method, []byte(consts.MethodGet))
	isHead := bytes.Equal(method, []byte(consts.MethodHead))

	// Conditional GET/HEAD only; other methods must not be answered 304.
	if (isGet || isHead) && ifNoneMatch(req.Header.GetAll(headerIfNoneMatch), p.rep.ETag) {
		resp.ResetBody()
		resp.SetStatusCode(consts.StatusNotModified)
		resp.Header.Set(headerETag, p.rep.ETag)
		return nil
	}

	resp.SetStatusCode(consts.StatusOK)

	if isHead {
		// Metadata-only response: never render the body. The explicitly
		// declared content length survives because the HTTP/1 server skips
		// the body for HEAD and therefore does not recompute it.
		resp.ResetBody()
		writeRepresentationHeaders(resp, p)
		resp.Header.SetContentLength(p.rep.ContentLength)
		return nil
	}

	// A 200 entity is required: invoke exactly the selected renderer once.
	if err := p.rep.Renderer.Render(resp); err != nil {
		return err
	}
	// Re-assert negotiation headers after rendering so a renderer cannot
	// leave Content-Type or Content-Encoding inconsistent with the
	// selected representation.
	writeRepresentationHeaders(resp, p)
	return nil
}

// writeRepresentationHeaders sets Content-Type, Content-Encoding and ETag
// from the static metadata of the selected representation.
func writeRepresentationHeaders(resp *protocol.Response, p *preparedRepresentation) {
	resp.Header.SetContentType(p.rep.MediaType)
	if p.encoding == identityCoding {
		resp.Header.Del(consts.HeaderContentEncoding)
	} else {
		resp.Header.SetContentEncoding(p.encoding)
	}
	resp.Header.Set(headerETag, p.rep.ETag)
}

// mergeVary adds the given header field names to the Vary response header,
// combining them with values already present (for example, set by an outer
// middleware). Existing tokens are matched case-insensitively and
// de-duplicated. If any existing token is "*", the wildcard is retained and
// nothing is appended; already-present headers are never overwritten.
func mergeVary(resp *protocol.Response, names ...string) {
	var tokens []string
	index := make(map[string]int)
	wildcard := false
	add := func(token string) {
		token = strings.TrimSpace(token)
		if token == "" {
			return
		}
		if strings.EqualFold(token, "*") {
			wildcard = true
			return
		}
		key := strings.ToLower(token)
		if _, ok := index[key]; ok {
			return
		}
		index[key] = len(tokens)
		tokens = append(tokens, token)
	}
	for _, v := range resp.Header.GetAll(headerVary) {
		for _, item := range splitList(v, ",") {
			add(item)
		}
	}
	// Set only overwrites the first stored header line, so the existing
	// (possibly multiple) Vary fields must be removed explicitly first.
	resp.Header.Del(headerVary)
	if wildcard {
		resp.Header.Set(headerVary, "*")
		return
	}
	for _, name := range names {
		add(name)
	}
	resp.Header.Set(headerVary, strings.Join(tokens, ", "))
}

// ---------------------------------------------------------------------------
// Media types and media ranges
// ---------------------------------------------------------------------------

type param struct {
	key   string
	value string
}

type mediaType struct {
	typ     string
	subtype string
	params  []param // canonical: keys lower, sorted, first-wins duplicates
}

type mediaRange struct {
	typ     string  // lower-cased, "*" allowed
	subtype string  // lower-cased, "*" allowed
	params  []param // accept-parameters except q (keys lower-cased)
	q       float64
	// rank compares two matching ranges: the media-range level dominates
	// (type/subtype > type/* > */*) and the number of parameters breaks
	// ties at the same level.
	level int
}

// parseMediaType parses a concrete media type with parameters. Wildcards are
// only valid in Accept media ranges, not in registered representations.
func parseMediaType(s string) (mediaType, error) {
	before, after, found := cutUnquoted(s, ';')
	mt := mediaType{}
	typePart := strings.ToLower(strings.TrimSpace(before))
	slash := strings.IndexByte(typePart, '/')
	if slash <= 0 || slash == len(typePart)-1 {
		return mt, fmt.Errorf("malformed media type %q", s)
	}
	mt.typ = typePart[:slash]
	mt.subtype = typePart[slash+1:]
	if mt.typ == "*" || mt.subtype == "*" {
		return mt, fmt.Errorf("wildcard media type %q is not a representation", s)
	}
	if !isToken(mt.typ) || !isToken(mt.subtype) {
		return mt, fmt.Errorf("malformed media type %q", s)
	}
	if found {
		ps, err := parseParameters(after)
		if err != nil {
			return mt, err
		}
		mt.params = canonicalParams(ps)
	}
	return mt, nil
}

// canonicalParams lower-cases parameter keys, normalizes charset values,
// removes duplicate keys (first occurrence wins) and sorts by key.
func canonicalParams(in []param) []param {
	seen := make(map[string]bool, len(in))
	out := make([]param, 0, len(in))
	for _, p := range in {
		if seen[p.key] {
			continue
		}
		seen[p.key] = true
		if p.key == "charset" {
			p.value = strings.ToLower(p.value)
		}
		out = append(out, p)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out
}

// canonical renders the canonical on-wire media type value.
func (mt mediaType) canonical() string {
	var b strings.Builder
	b.WriteString(mt.typ)
	b.WriteByte('/')
	b.WriteString(mt.subtype)
	for _, p := range mt.params {
		b.WriteString("; ")
		b.WriteString(p.key)
		b.WriteByte('=')
		if isToken(p.value) {
			b.WriteString(p.value)
		} else {
			b.WriteString(quoteValue(p.value))
		}
	}
	return b.String()
}

// identityKey builds the case-insensitive identity used to reject duplicate
// registrations.
func (mt mediaType) identityKey() string {
	var b strings.Builder
	b.WriteString(mt.typ)
	b.WriteByte('/')
	b.WriteString(mt.subtype)
	for _, p := range mt.params {
		b.WriteByte(';')
		b.WriteString(p.key)
		b.WriteByte('=')
		if p.key == "charset" {
			b.WriteString(strings.ToLower(p.value))
		} else {
			b.WriteString(p.value)
		}
	}
	return b.String()
}

// matches reports whether the concrete media type is covered by the given
// Accept media range. Every parameter constrained by the range must be
// present and equal on the representation; extra representation parameters
// do not prevent the match.
func (mt mediaType) matches(r mediaRange) bool {
	if r.typ != "*" && r.typ != mt.typ {
		return false
	}
	if r.subtype != "*" && r.subtype != mt.subtype {
		return false
	}
	for _, want := range r.params {
		if !mt.hasParam(want) {
			return false
		}
	}
	return true
}

func (mt mediaType) hasParam(want param) bool {
	for _, p := range mt.params {
		if p.key != want.key {
			continue
		}
		if want.key == "charset" {
			return strings.EqualFold(p.value, want.value)
		}
		return p.value == want.value
	}
	return false
}

// parseAccept parses all Accept header field values. Multiple header fields
// are combined as a comma-separated list, as required for #rule fields.
//
// Duplicate media ranges (same type/subtype and parameters) keep the quality
// of their first occurrence. Elements with an invalid quality value or
// malformed syntax are ignored instead of failing the whole negotiation.
// The returned present flag reports whether at least one Accept field was
// present; with no Accept field every media type is acceptable.
func parseAccept(values []string) (ranges []mediaRange, present bool) {
	present = len(values) > 0
	seen := make(map[string]bool)
	for _, v := range values {
		for _, item := range splitList(v, ",") {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			mr, ok := parseMediaRange(item)
			if !ok {
				continue
			}
			key := mr.typ + "/" + mr.subtype + "|" + paramsKey(mr.params)
			if seen[key] {
				continue
			}
			seen[key] = true
			ranges = append(ranges, mr)
		}
	}
	return ranges, present
}

func parseMediaRange(item string) (mediaRange, bool) {
	before, after, found := cutUnquoted(item, ';')
	typePart := strings.ToLower(strings.TrimSpace(before))
	slash := strings.IndexByte(typePart, '/')
	if slash <= 0 || slash == len(typePart)-1 {
		return mediaRange{}, false
	}
	mr := mediaRange{q: 1, level: -1}
	mr.typ = typePart[:slash]
	mr.subtype = typePart[slash+1:]
	if mr.typ != "*" && !isToken(mr.typ) {
		return mediaRange{}, false
	}
	if mr.subtype != "*" && !isToken(mr.subtype) {
		return mediaRange{}, false
	}
	if mr.typ == "*" && mr.subtype != "*" {
		return mediaRange{}, false
	}
	if found {
		ps, err := parseParameters(after)
		if err != nil {
			return mediaRange{}, false
		}
		qSeen := false
		for _, p := range ps {
			if p.key == "q" {
				if qSeen {
					continue
				}
				f, err := strconv.ParseFloat(p.value, 64)
				if err != nil || f < 0 || f > 1 {
					// Malformed weight: ignore this whole media range.
					return mediaRange{}, false
				}
				mr.q = f
				qSeen = true
				continue
			}
			mr.params = append(mr.params, p)
		}
	}
	switch {
	case mr.typ != "*" && mr.subtype != "*":
		mr.level = 2
	case mr.typ != "*":
		mr.level = 1
	default:
		mr.level = 0
	}
	// Parameter counts only break ties within one media-range level.
	mr.level = mr.level*1000 + len(mr.params)
	return mr, true
}

// mediaQuality returns the client quality for a concrete media type. With no
// Accept header field everything is acceptable (q=1). The most specific
// matching media range determines the quality, so a concrete range
// overrides a wildcard even when listed first.
func mediaQuality(mt mediaType, ranges []mediaRange, present bool) float64 {
	if !present {
		return 1
	}
	bestLevel := -1
	q := 0.0
	for i := range ranges {
		r := &ranges[i]
		if mt.matches(*r) && r.level > bestLevel {
			bestLevel = r.level
			q = r.q
		}
	}
	return q
}

func paramsKey(ps []param) string {
	parts := make([]string, 0, len(ps))
	for _, p := range ps {
		parts = append(parts, p.key+"="+p.value)
	}
	sort.Strings(parts)
	return strings.Join(parts, ";")
}

// ---------------------------------------------------------------------------
// Content codings
// ---------------------------------------------------------------------------

type codingPreference struct {
	coding string
	q      float64
}

type codingPreferences []codingPreference

// parseAcceptEncoding parses all Accept-Encoding header field values.
// Multiple fields combine as one list; duplicate codings keep the quality of
// their first occurrence (so "gzip;q=0, gzip" disables gzip). Malformed
// elements are ignored.
func parseAcceptEncoding(values []string) (codingPreferences, bool) {
	if len(values) == 0 {
		return nil, false
	}
	var prefs codingPreferences
	seen := make(map[string]bool)
	for _, v := range values {
		for _, item := range splitList(v, ",") {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			before, after, found := cutUnquoted(item, ';')
			coding := strings.ToLower(strings.TrimSpace(before))
			if coding != "*" && !isToken(coding) {
				continue
			}
			q := 1.0
			if found {
				ps, err := parseParameters(after)
				if err != nil {
					continue
				}
				qSeen := false
				valid := true
				for _, p := range ps {
					if p.key != "q" {
						// Unknown accept-extensions are tolerated.
						continue
					}
					if qSeen {
						continue
					}
					f, err := strconv.ParseFloat(p.value, 64)
					if err != nil || f < 0 || f > 1 {
						valid = false
						break
					}
					q = f
					qSeen = true
				}
				if !valid {
					continue
				}
			}
			if seen[coding] {
				continue
			}
			seen[coding] = true
			prefs = append(prefs, codingPreference{coding: coding, q: q})
		}
	}
	return prefs, true
}

// quality implements RFC 7231, section 5.3.4: an exact entry wins over "*";
// without either, identity stays acceptable by default while every other
// coding is refused. A zero weight means "not acceptable".
func (prefs codingPreferences) quality(coding string) float64 {
	exactQ, hasExact := 0.0, false
	starQ, hasStar := 0.0, false
	for _, p := range prefs {
		switch p.coding {
		case coding:
			hasExact, exactQ = true, p.q
		case "*":
			hasStar, starQ = true, p.q
		}
	}
	if hasExact {
		return exactQ
	}
	if hasStar {
		return starQ
	}
	if coding == identityCoding {
		return 1
	}
	return 0
}

// canonicalEncoding validates and normalizes a single declared content
// coding. Layered codings (commas), parameter lists, separators and invalid
// tokens are rejected: each representation carries exactly one coding.
func canonicalEncoding(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return identityCoding, nil
	}
	if strings.ContainsAny(s, ",;") || strings.ContainsAny(s, " \t\"") {
		return "", fmt.Errorf("only a single content coding is allowed, got %q", s)
	}
	if !isToken(s) {
		return "", fmt.Errorf("invalid content coding %q", s)
	}
	coding := strings.ToLower(s)
	// Historical aliases registered by RFC 7231, section 3.1.2.1.
	switch coding {
	case "x-gzip":
		coding = "gzip"
	case "x-deflate":
		coding = "deflate"
	case "x-compress":
		coding = "compress"
	}
	return coding, nil
}

// ---------------------------------------------------------------------------
// Entity-tags (RFC 7232)
// ---------------------------------------------------------------------------

// validateEntityTag checks the entity-tag grammar:
//
//	entity-tag = [ weak ] opaque-tag
//	weak       = %x57.2F            ; "W/"
//	opaque-tag = DQUOTE *etagc DQUOTE
//	etagc      = %x21 / %x23-7E / obs-text
func validateEntityTag(tag string) error {
	s := tag
	if strings.HasPrefix(s, "W/") {
		s = s[2:]
	}
	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return fmt.Errorf("invalid entity-tag %q: expected a quoted opaque tag, optionally prefixed with W/", tag)
	}
	for i := 1; i < len(s)-1; i++ {
		c := s[i]
		switch {
		case c == 0x21:
		case c >= 0x23 && c <= 0x7e:
		case c >= 0x80:
		default:
			return fmt.Errorf("invalid entity-tag %q: illegal character in opaque tag", tag)
		}
	}
	return nil
}

// opaqueTag strips any weak marker, returning the strong opaque tag used for
// duplicate detection.
func opaqueTag(tag string) string {
	if strings.HasPrefix(tag, "W/") {
		return tag[2:]
	}
	return tag
}

// ifNoneMatch evaluates an If-None-Match header field value list against the
// selected representation's entity-tag. "*" matches any representation.
func ifNoneMatch(values []string, selectedTag string) bool {
	for _, v := range values {
		for _, item := range splitList(v, ",") {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			if item == "*" {
				return true
			}
			// Be lenient with a lower-case weak marker from a peer and
			// normalize it to the grammar form; ignore malformed elements
			// rather than matching on garbage.
			if len(item) >= 2 && (item[0] == 'w' || item[0] == 'W') && item[1] == '/' {
				item = "W/" + item[2:]
			}
			if err := validateEntityTag(item); err != nil {
				continue
			}
			if opaqueTag(item) == opaqueTag(selectedTag) {
				return true
			}
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Low-level HTTP value parsing
// ---------------------------------------------------------------------------

// parseParameters parses `key=value; key2="value"` parameters. Parameter
// names are token characters; values are a token or a quoted-string with
// backslash escaping. Keys are returned lower-cased.
func parseParameters(s string) ([]param, error) {
	var params []param
	for _, part := range splitList(s, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, errors.New("empty parameter")
		}
		key, raw, found := cutUnquoted(part, '=')
		if !found {
			return nil, fmt.Errorf("parameter %q is missing a value", part)
		}
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "" || !isToken(key) {
			return nil, fmt.Errorf("invalid parameter name %q", key)
		}
		value, err := parseValue(strings.TrimSpace(raw))
		if err != nil {
			return nil, err
		}
		params = append(params, param{key: key, value: value})
	}
	return params, nil
}

// parseValue parses a token or quoted-string value.
func parseValue(v string) (string, error) {
	if v == "" {
		return "", errors.New("empty parameter value")
	}
	if v[0] != '"' {
		if !isToken(v) {
			return "", fmt.Errorf("invalid parameter value %q", v)
		}
		return v, nil
	}
	if len(v) < 2 || v[len(v)-1] != '"' {
		return "", fmt.Errorf("unterminated quoted string %q", v)
	}
	var b strings.Builder
	for i := 1; i < len(v)-1; i++ {
		c := v[i]
		if c == '\\' {
			i++
			if i >= len(v)-1 {
				return "", fmt.Errorf("bad escape in quoted string %q", v)
			}
			b.WriteByte(v[i])
			continue
		}
		if c == '"' {
			return "", fmt.Errorf("unescaped quotation mark in %q", v)
		}
		b.WriteByte(c)
	}
	return b.String(), nil
}

// quoteValue renders v as a quoted-string, escaping '"' and '\'.
func quoteValue(v string) string {
	var b strings.Builder
	b.WriteByte('"')
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '"' || c == '\\' {
			b.WriteByte('\\')
		}
		b.WriteByte(c)
	}
	b.WriteByte('"')
	return b.String()
}

// splitList splits s on sep bytes that occur outside quoted strings,
// honoring backslash quoted-pairs. It never allocates substrings that alias
// pooled storage: callers pass freshly copied strings.
func splitList(s string, seps string) []string {
	var out []string
	start := 0
	inQuote := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inQuote {
			if c == '\\' && i+1 < len(s) {
				i++
				continue
			}
			if c == '"' {
				inQuote = false
			}
			continue
		}
		if c == '"' {
			inQuote = true
			continue
		}
		if strings.IndexByte(seps, c) >= 0 {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return append(out, s[start:])
}

// cutUnquoted splits s at the first sep byte outside quoted strings.
func cutUnquoted(s string, sep byte) (before, after string, found bool) {
	inQuote := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inQuote {
			if c == '\\' && i+1 < len(s) {
				i++
				continue
			}
			if c == '"' {
				inQuote = false
			}
			continue
		}
		if c == '"' {
			inQuote = true
			continue
		}
		if c == sep {
			return s[:i], s[i+1:], true
		}
	}
	return s, "", false
}

// isToken reports whether s is a non-empty RFC 7230 token.
func isToken(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isTokenChar(s[i]) {
			return false
		}
	}
	return true
}

// isTokenChar reports whether c is an RFC 7230 tchar.
func isTokenChar(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z':
		return true
	case c >= 'A' && c <= 'Z':
		return true
	case c >= '0' && c <= '9':
		return true
	}
	switch c {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	}
	return false
}
