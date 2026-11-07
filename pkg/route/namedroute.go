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

package route

import (
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/route/param"
)

// Sentinel errors returned by URLFor. Callers can use errors.Is to detect them.
var (
	// ErrRouteNameNotFound is returned when no route is registered under the given name.
	ErrRouteNameNotFound = errors.New("hertz: route name not found")
	// ErrRouteNameEmpty is returned when an empty name is bound to a route.
	ErrRouteNameEmpty = errors.New("hertz: route name must not be empty")
	// ErrRouteParamMissing is returned when a path parameter declared by the route is not provided.
	ErrRouteParamMissing = errors.New("hertz: missing path parameter value")
	// ErrRouteParamUnexpected is returned when an unknown path parameter is provided.
	ErrRouteParamUnexpected = errors.New("hertz: unexpected path parameter")
	// ErrRouteParamInvalid is returned when a parameter value cannot be encoded safely.
	ErrRouteParamInvalid = errors.New("hertz: invalid path parameter value")
	// ErrRouteNoRoundTrip is returned when the generated URL cannot be guaranteed to
	// re-match the named route under the engine's current path handling configuration
	// (e.g. it would hit a static route, a trailing-slash redirect or a 404).
	ErrRouteNoRoundTrip = errors.New("hertz: generated url does not round-trip to the named route")
	// ErrRouteTableChanged is returned when routes were modified while URLFor was building
	// the URL, so name mapping and match tree cannot be proven to be of the same generation.
	ErrRouteTableChanged = errors.New("hertz: route table changed while building url")
)

const routeKeySep = "\x00"

func routeKey(method, path string) string {
	return method + routeKeySep + path
}

// namedRoute is an immutable descriptor of an explicitly named route.
// pnames is a private slice that is never mutated after publication.
type namedRoute struct {
	method string
	path   string
	pnames []string
	gen    uint64
}

// routeRegistry is an immutable snapshot of every named route. A new snapshot is
// atomically published whenever a named route is registered, keeping the name
// mapping and the structural match tree in one generation.
type routeRegistry struct {
	gen       uint64
	byName    map[string]namedRoute
	routeName map[string]string // method+sep+path -> name (used by Routes)
}

func newRouteRegistry() *routeRegistry {
	return &routeRegistry{
		byName:    make(map[string]namedRoute),
		routeName: make(map[string]string),
	}
}

func (engine *Engine) routeRegistryLoad() *routeRegistry {
	if v := engine.routeReg.Load(); v != nil {
		return v.(*routeRegistry)
	}
	return newRouteRegistry()
}

// parseParamNames returns the ordered parameter names declared by a route
// pattern, e.g. "/u/:id/f/*p" -> ["id", "p"]. There is at most one catch-all
// parameter and it is always last.
func parseParamNames(pattern string) []string {
	var names []string
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case ':':
			i++
			start := i
			for i < len(pattern) && pattern[i] != '/' {
				i++
			}
			names = append(names, pattern[start:i])
			i--
		case '*':
			names = append(names, pattern[i+1:])
			return names
		}
	}
	return names
}

// addRouteNamed inserts the route into the match tree and atomically binds the
// explicit, globally unique name to the final (group-prefix composed) path. The
// handler function name is never used as the route name.
func (engine *Engine) addRouteNamed(method, path string, handlers app.HandlersChain, name string) {
	engine.routeNamesMu.Lock()
	defer engine.routeNamesMu.Unlock()

	if name != "" {
		if _, exists := engine.routeRegistryLoad().byName[name]; exists {
			panic(fmt.Sprintf("route name %q is already registered", name))
		}
	}

	// Mutate the tree and publish the mapping under the same lock so readers of
	// a single generation never observe one without the other.
	engine.insertRoute(method, path, handlers)
	if name == "" {
		return
	}

	next := engine.cloneRouteRegistryLocked()
	next.byName[name] = namedRoute{method: method, path: path, pnames: parseParamNames(path), gen: next.gen}
	next.routeName[routeKey(method, path)] = name
	engine.routeReg.Store(next)
}

// cloneRouteRegistryLocked returns a copy of the current snapshot with a new
// generation. It must be called with routeNamesMu held, between the tree
// mutation and the atomic publication, so every structural change (including an
// unnamed registration) advances the generation atomically with the tree.
func (engine *Engine) cloneRouteRegistryLocked() *routeRegistry {
	cur := engine.routeRegistryLoad()
	next := &routeRegistry{
		gen:       cur.gen + 1,
		byName:    make(map[string]namedRoute, len(cur.byName)),
		routeName: make(map[string]string, len(cur.routeName)),
	}
	for k, v := range cur.byName {
		next.byName[k] = v
	}
	for k, v := range cur.routeName {
		next.routeName[k] = v
	}
	return next
}

// bumpRouteGenerationLocked publishes a new generation snapshot for a structural
// change that does not add a name (an unnamed registration).
func (engine *Engine) bumpRouteGenerationLocked() {
	engine.routeReg.Store(engine.cloneRouteRegistryLocked())
}

// QueryArgument is a single ordered query pair. Duplicate keys and empty values
// are preserved, unlike a map which cannot express either.
type QueryArgument struct {
	Key   string
	Value string
}

// RouteURL is the result of URLFor. The request target returned by String (path
// plus query) is safe to send to an HTTP server; the fragment is provided
// separately and is never part of the request target because HTTP clients do not
// send fragments to servers.
type RouteURL struct {
	path     string
	rawQuery string
	fragment string
}

// Path is the percent-encoded concrete path.
func (u *RouteURL) Path() string { return u.path }

// RawQuery is the encoded query string without the leading '?'.
func (u *RouteURL) RawQuery() string { return u.rawQuery }

// Fragment is the URL fragment. It is delivered out-of-band and is never sent
// to the HTTP server.
func (u *RouteURL) Fragment() string { return u.fragment }

// String returns the request target ("path" or "path?query") that re-matches
// the named route. The fragment is intentionally excluded.
func (u *RouteURL) String() string {
	if u.rawQuery != "" {
		return u.path + "?" + u.rawQuery
	}
	return u.path
}

type urlForConfig struct {
	query     []QueryArgument
	fragment  string
	sortQuery bool
}

// URLOption configures URLFor.
type URLOption func(*urlForConfig)

// WithQuery appends one ordered query pair. Apply it multiple times to keep
// duplicate keys or empty values in explicit order.
func WithQuery(key, value string) URLOption {
	return func(c *urlForConfig) {
		c.query = append(c.query, QueryArgument{Key: key, Value: value})
	}
}

// WithQueryArguments appends a slice of ordered query pairs.
func WithQueryArguments(args []QueryArgument) URLOption {
	return func(c *urlForConfig) {
		c.query = append(c.query, args...)
	}
}

// WithSortedQuery switches query generation to a deterministic fixed order
// (sorted by key, then by value) instead of the explicit input order.
func WithSortedQuery() URLOption {
	return func(c *urlForConfig) {
		c.sortQuery = true
	}
}

// WithFragment sets a fragment that is returned separately from the request
// target and is never placed on the wire.
func WithFragment(fragment string) URLOption {
	return func(c *urlForConfig) {
		c.fragment = fragment
	}
}

// URLFor builds a URL that is guaranteed to re-match the route registered under
// name. Path parameters are substituted positionally and encoded according to
// the engine's UseRawPath / UnescapePathValues / RemoveExtraSlash configuration;
// query pairs keep their order, duplicate keys and empty values; the fragment,
// if any, is only available on the returned RouteURL.
//
// It errors without returning a partial URL when the name is unknown, when
// parameters are missing or unexpected, or when the resulting path would not
// directly re-match the named route (for example colliding with a higher
// priority static route).
func (engine *Engine) URLFor(name string, pathParams map[string]string, opts ...URLOption) (*RouteURL, error) {
	cfg := &urlForConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	reg := engine.routeRegistryLoad()
	desc, ok := reg.byName[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrRouteNameNotFound, name)
	}

	if pathParams == nil {
		pathParams = map[string]string{}
	}

	// Validate exactly-once presence of every declared parameter.
	used := make(map[string]bool, len(desc.pnames))
	for _, pn := range desc.pnames {
		if _, ok := pathParams[pn]; !ok {
			return nil, fmt.Errorf("%w: %q for route %q", ErrRouteParamMissing, pn, name)
		}
		used[pn] = true
	}
	for k := range pathParams {
		if !used[k] {
			return nil, fmt.Errorf("%w: %q for route %q", ErrRouteParamUnexpected, k, name)
		}
	}

	builtPath, err := engine.buildPath(desc, pathParams)
	if err != nil {
		return nil, err
	}

	query := cfg.query
	if cfg.sortQuery {
		query = append([]QueryArgument(nil), query...)
		sort.SliceStable(query, func(i, j int) bool {
			if query[i].Key != query[j].Key {
				return query[i].Key < query[j].Key
			}
			return query[i].Value < query[j].Value
		})
	}
	rawQuery := encodeQuery(query)

	target := builtPath
	if rawQuery != "" {
		target = builtPath + "?" + rawQuery
	}

	if err := engine.verifyRoundTrip(desc, target, rawQuery, query, pathParams, reg.gen); err != nil {
		return nil, err
	}

	return &RouteURL{path: builtPath, rawQuery: rawQuery, fragment: cfg.fragment}, nil
}

// buildPath substitutes the declared parameters into the registered pattern,
// emitting percent-encoded single segments for ":name" and an encoded
// multi-segment remainder for "*name".
func (engine *Engine) buildPath(desc namedRoute, params map[string]string) (string, error) {
	opt := engine.options
	// Values are returned verbatim (still encoded) only in raw-path mode with
	// unescaping disabled. In every other mode the matched value is decoded, so
	// we emit percent-encoded bytes that decode back to the requested value.
	rawValues := opt.UseRawPath && !opt.UnescapePathValues

	var sb strings.Builder
	pattern := desc.path
	for i := 0; i < len(pattern); {
		switch pattern[i] {
		case ':':
			j := i + 1
			for j < len(pattern) && pattern[j] != '/' {
				j++
			}
			pn := pattern[i+1 : j]
			seg, err := encodeSingleSegment(params[pn], opt, rawValues, pn)
			if err != nil {
				return "", err
			}
			sb.WriteString(seg)
			i = j
		case '*':
			pn := pattern[i+1:]
			seg, err := encodeCatchAll(params[pn], opt, rawValues, pn)
			if err != nil {
				return "", err
			}
			sb.WriteString(seg)
			i = len(pattern)
		default:
			j := i + 1
			for j < len(pattern) && pattern[j] != ':' && pattern[j] != '*' {
				j++
			}
			sb.WriteString(pattern[i:j])
			i = j
		}
	}
	return sb.String(), nil
}

func encodeSingleSegment(v string, opt *config.Options, rawValues bool, name string) (string, error) {
	if v == "" {
		return "", fmt.Errorf("%w: parameter %q must not be empty", ErrRouteParamInvalid, name)
	}
	if rawValues {
		if strings.ContainsAny(v, "/?#") {
			return "", fmt.Errorf("%w: parameter %q must not contain '/', '?' or '#' when UseRawPath is enabled with UnescapePathValues disabled", ErrRouteParamInvalid, name)
		}
		if hasControlByte(v) {
			return "", fmt.Errorf("%w: parameter %q contains a control character", ErrRouteParamInvalid, name)
		}
		return v, nil
	}
	// Without UseRawPath the router matches the already-decoded normalized
	// path: an encoded slash (%2F) is decoded into a real slash before matching
	// and would split the segment, so a slash can never round-trip here.
	if !opt.UseRawPath && strings.ContainsRune(v, '/') {
		return "", fmt.Errorf("%w: parameter %q contains a slash that cannot be kept inside one path segment without UseRawPath", ErrRouteParamInvalid, name)
	}
	return percentEscape(v), nil
}

func encodeCatchAll(v string, opt *config.Options, rawValues bool, name string) (string, error) {
	if rawValues {
		if strings.ContainsAny(v, "?#") || hasControlByte(v) {
			return "", fmt.Errorf("%w: parameter %q must not contain '?', '#' or control characters when UseRawPath is enabled with UnescapePathValues disabled", ErrRouteParamInvalid, name)
		}
		return v, nil
	}
	// An empty remainder is reached by the pattern prefix itself (which ends in
	// '/'); e.g. "/src/" matches "/src/*filepath" with filepath == "".
	if v == "" {
		return "", nil
	}
	// The default Path() is normalized in every configuration, and CleanPath is
	// applied additionally when RemoveExtraSlash is enabled. Empty segments
	// ("//") as well as "."/".." elements would be removed before matching and
	// therefore can never round-trip as catch-all value.
	normalizes := !opt.UseRawPath || opt.RemoveExtraSlash
	if normalizes {
		for _, seg := range strings.Split(v, "/") {
			switch seg {
			case "", ".", "..":
				return "", fmt.Errorf("%w: catch-all parameter %q contains an empty, '.' or '..' segment which is normalized away before routing", ErrRouteParamInvalid, name)
			}
		}
	}
	var sb strings.Builder
	for i, seg := range strings.Split(v, "/") {
		if i > 0 {
			sb.WriteByte('/')
		}
		sb.WriteString(percentEscape(seg))
	}
	return sb.String(), nil
}

func hasControlByte(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] == 0x7f {
			return true
		}
	}
	return false
}

// isUnreserved reports whether b is an RFC 3986 unreserved character.
// Everything else is percent-encoded, which keeps '?', '#', '%', '/', '+',
// spaces and Unicode unambiguous in every decoding mode.
func isUnreserved(b byte) bool {
	switch {
	case 'a' <= b && b <= 'z', 'A' <= b && b <= 'Z', '0' <= b && b <= '9':
		return true
	}
	return b == '-' || b == '.' || b == '_' || b == '~'
}

func percentEscape(s string) string {
	var sb strings.Builder
	sb.Grow(len(s))
	for i := 0; i < len(s); i++ {
		b := s[i]
		if isUnreserved(b) {
			sb.WriteByte(b)
			continue
		}
		sb.WriteByte('%')
		const hex = "0123456789ABCDEF"
		sb.WriteByte(hex[b>>4])
		sb.WriteByte(hex[b&0x0f])
	}
	return sb.String()
}

func encodeQuery(args []QueryArgument) string {
	if len(args) == 0 {
		return ""
	}
	var sb strings.Builder
	for i, a := range args {
		if i > 0 {
			sb.WriteByte('&')
		}
		sb.WriteString(percentEscape(a.Key))
		sb.WriteByte('=')
		sb.WriteString(percentEscape(a.Value))
	}
	return sb.String()
}

// verifyRoundTrip replays the exact request-path derivation used by ServeHTTP
// and runs the real tree lookup. It only succeeds when a handler is matched on
// the named route's own pattern and every extracted parameter equals the
// requested value, which rules out static-priority collisions, redirects and
// 404s. It allocates a private parameter slice and never mutates tree nodes.
func (engine *Engine) verifyRoundTrip(
	desc namedRoute,
	target, rawQuery string,
	wantQuery []QueryArgument,
	params map[string]string,
	gen uint64,
) error {
	opt := engine.options

	// Parse the target exactly like incoming traffic would. The target always
	// starts with '/' and never carries a fragment.
	var u protocol.URI
	u.Parse(nil, []byte(target))

	var rPath string
	unescape := false
	if opt.UseRawPath {
		rPath = string(u.PathOriginal())
		unescape = opt.UnescapePathValues
	} else {
		rPath = string(u.Path())
	}
	if opt.RemoveExtraSlash {
		rPath = utils.CleanPath(rPath)
	}

	if rPath == "" || rPath[0] != '/' {
		return fmt.Errorf("%w: %q does not produce an absolute path", ErrRouteNoRoundTrip, target)
	}

	tree := engine.trees.get(desc.method)
	if tree == nil {
		return fmt.Errorf("%w: no tree for method %q", ErrRouteNoRoundTrip, desc.method)
	}

	// Size the scratch parameter slice like ServeHTTP does so backtracking into
	// parameter branches of sibling routes can never exceed its capacity.
	pp := make(param.Params, 0, int(engine.maxParams))
	value := tree.find(rPath, &pp, unescape)

	if value.handlers == nil {
		if value.tsr && opt.RedirectTrailingSlash {
			return fmt.Errorf("%w: %q would only be reached through a trailing-slash redirect", ErrRouteNoRoundTrip, target)
		}
		return fmt.Errorf("%w: %q does not directly match a handler", ErrRouteNoRoundTrip, target)
	}
	if value.fullPath != desc.path {
		return fmt.Errorf("%w: %q matches route %q which takes priority over named route %q", ErrRouteNoRoundTrip, target, value.fullPath, desc.path)
	}
	if len(pp) != len(desc.pnames) {
		return fmt.Errorf("%w: %q yields %d parameters, route declares %d", ErrRouteNoRoundTrip, target, len(pp), len(desc.pnames))
	}
	for i, pn := range desc.pnames {
		if pp[i].Key != pn {
			return fmt.Errorf("%w: %q yields parameter %q where %q is declared", ErrRouteNoRoundTrip, target, pp[i].Key, pn)
		}
		if pp[i].Value != params[pn] {
			return fmt.Errorf("%w: parameter %q would be %q, want %q", ErrRouteNoRoundTrip, pn, pp[i].Value, params[pn])
		}
	}

	if rawQuery != "" {
		if string(u.QueryString()) != rawQuery {
			return fmt.Errorf("%w: query string is altered by URL parsing", ErrRouteNoRoundTrip)
		}
		if err := verifyQueryRoundTrip(rawQuery, wantQuery); err != nil {
			return err
		}
	}

	// Reject if the table moved to another generation while building, so the
	// result cannot belong to a mix of name mapping and match tree.
	if engine.routeRegistryLoad().gen != gen {
		return ErrRouteTableChanged
	}
	return nil
}

func verifyQueryRoundTrip(rawQuery string, want []QueryArgument) error {
	parts := strings.Split(rawQuery, "&")
	if len(parts) != len(want) {
		return fmt.Errorf("%w: query pair count changed", ErrRouteNoRoundTrip)
	}
	for i, p := range parts {
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			return fmt.Errorf("%w: query pair %q is malformed", ErrRouteNoRoundTrip, p)
		}
		k, err := url.QueryUnescape(kv[0])
		if err != nil {
			return fmt.Errorf("%w: invalid encoded query key: %v", ErrRouteNoRoundTrip, err)
		}
		v, err := url.QueryUnescape(kv[1])
		if err != nil {
			return fmt.Errorf("%w: invalid encoded query value: %v", ErrRouteNoRoundTrip, err)
		}
		if k != want[i].Key || v != want[i].Value {
			return fmt.Errorf("%w: query pair %d decoded to %q=%q, want %q=%q", ErrRouteNoRoundTrip, i, k, v, want[i].Key, want[i].Value)
		}
	}
	return nil
}
