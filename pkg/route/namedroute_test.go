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
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/test/assert"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// recorded is filled by the echo handler used in round-trip tests.
type recorded struct {
	status   int
	fullPath string
	id       string
	pid      string
	user     string
	project  string
	filepath string
	q        string
	rawQuery string
}

func echoRecord(rec *recorded) app.HandlerFunc {
	return func(c context.Context, ctx *app.RequestContext) {
		rec.status = consts.StatusOK
		rec.fullPath = ctx.FullPath()
		rec.id = ctx.Param("id")
		rec.pid = ctx.Param("pid")
		rec.user = ctx.Param("user")
		rec.project = ctx.Param("project")
		rec.filepath = ctx.Param("filepath")
		rec.q = ctx.Param("q")
		rec.rawQuery = string(ctx.Request.URI().QueryString())
		ctx.String(consts.StatusOK, "ok")
	}
}

func mustURLFor(t *testing.T, e *Engine, name string, params map[string]string, opts ...URLOption) *RouteURL {
	t.Helper()
	u, err := e.URLFor(name, params, opts...)
	if err != nil {
		t.Fatalf("URLFor(%q) unexpected error: %v", name, err)
	}
	if u == nil {
		t.Fatalf("URLFor(%q) returned nil url without error", name)
	}
	return u
}

func TestURLForBasicParamsQueryFragment(t *testing.T) {
	e := NewEngine(config.NewOptions(nil))
	rec := &recorded{}
	e.HandleNamed("user.post", consts.MethodGet, "/users/:id/posts/:pid", echoRecord(rec))

	u := mustURLFor(t, e, "user.post",
		map[string]string{"id": "42", "pid": "7"},
		WithQuery("tag", "go"),
		WithQuery("tag", "web"), // duplicate key preserved
		WithQuery("empty", ""),  // empty value preserved
		WithQuery("q", "a b+c&=x?#中"),
		WithSortedQuery(),
		WithFragment("section/1"),
	)

	// deterministic fixed order: empty, q, tag, tag
	assert.DeepEqual(t, "empty=&q=a%20b%2Bc%26%3Dx%3F%23%E4%B8%AD&tag=go&tag=web", u.RawQuery())
	assert.DeepEqual(t, "/users/42/posts/7", u.Path())
	assert.DeepEqual(t, "/users/42/posts/7?empty=&q=a%20b%2Bc%26%3Dx%3F%23%E4%B8%AD&tag=go&tag=web", u.String())
	// fragment is provided separately and never sent
	assert.DeepEqual(t, "section/1", u.Fragment())
	assert.Assert(t, !strings.Contains(u.String(), "#"))

	// explicit input order when not sorting
	u2 := mustURLFor(t, e, "user.post",
		map[string]string{"id": "1", "pid": "2"},
		WithQuery("b", "2"), WithQuery("a", "1"),
	)
	assert.DeepEqual(t, "b=2&a=1", u2.RawQuery())

	// real ServeHTTP round-trip: FullPath, params and ordered query
	w := performRequest(e, consts.MethodGet, u.String())
	assert.DeepEqual(t, consts.StatusOK, w.Code)
	assert.DeepEqual(t, "/users/:id/posts/:pid", rec.fullPath)
	assert.DeepEqual(t, "42", rec.id)
	assert.DeepEqual(t, "7", rec.pid)
	assert.DeepEqual(t, u.RawQuery(), rec.rawQuery)
}

func TestURLForCatchAll(t *testing.T) {
	e := NewEngine(config.NewOptions(nil))
	rec := &recorded{}
	e.HandleNamed("src", consts.MethodGet, "/src/*filepath", echoRecord(rec))

	want := "dir/a?b#c%中+x.png"
	u := mustURLFor(t, e, "src", map[string]string{"filepath": want})
	// special characters must be percent-encoded so they cannot become query/fragment
	for _, sub := range []string{"%3F", "%23", "%25", "%2B"} {
		assert.Assert(t, strings.Contains(u.Path(), sub), "missing encoding %s in %s", sub, u.Path())
	}
	assert.Assert(t, !strings.Contains(u.Path(), "?"))
	assert.Assert(t, !strings.Contains(u.Path(), "#"))

	w := performRequest(e, consts.MethodGet, u.String())
	assert.DeepEqual(t, consts.StatusOK, w.Code)
	assert.DeepEqual(t, "/src/*filepath", rec.fullPath)
	assert.DeepEqual(t, want, rec.filepath)

	// multi-segment catch-all
	want2 := "some/nested/file test.png"
	u2 := mustURLFor(t, e, "src", map[string]string{"filepath": want2})
	w2 := performRequest(e, consts.MethodGet, u2.String())
	assert.DeepEqual(t, consts.StatusOK, w2.Code)
	assert.DeepEqual(t, want2, rec.filepath)

	// empty remainder matches the prefix directly
	u3 := mustURLFor(t, e, "src", map[string]string{"filepath": ""})
	assert.DeepEqual(t, "/src/", u3.Path())
	w3 := performRequest(e, consts.MethodGet, u3.String())
	assert.DeepEqual(t, consts.StatusOK, w3.Code)
	assert.DeepEqual(t, "", rec.filepath)
}

func TestURLForEncodedSlash(t *testing.T) {
	// Default mode: Path() is decoded before matching, a slash cannot stay inside
	// one ":param" segment even when percent-encoded, so URLFor must refuse.
	def := NewEngine(config.NewOptions(nil))
	def.HandleNamed("info", consts.MethodGet, "/info/:user", echoRecord(&recorded{}))
	_, err := def.URLFor("info", map[string]string{"user": "slash/gordon"})
	assert.Assert(t, errors.Is(err, ErrRouteParamInvalid), "got %v", err)

	// UseRawPath keeps %2F intact while matching and unescapes the captured value.
	raw := NewEngine(config.NewOptions(nil))
	raw.options.UseRawPath = true
	rec := &recorded{}
	raw.HandleNamed("info", consts.MethodGet, "/info/:user/project/:project", echoRecord(rec))
	u := mustURLFor(t, raw, "info", map[string]string{"user": "slash/gordon", "project": "Project #1"})
	assert.Assert(t, strings.Contains(u.Path(), "%2F"), u.Path())
	w := performRequest(raw, consts.MethodGet, u.String())
	assert.DeepEqual(t, consts.StatusOK, w.Code)
	assert.DeepEqual(t, "/info/:user/project/:project", rec.fullPath)
	assert.DeepEqual(t, "slash/gordon", rec.user)
	assert.DeepEqual(t, "Project #1", rec.project)

	// UseRawPath without unescaping: the raw segment is the value verbatim; a real
	// slash cannot be expressed as a single segment, but an already-encoded value
	// round-trips without decoding.
	rawKeep := NewEngine(config.NewOptions(nil))
	rawKeep.options.UseRawPath = true
	rawKeep.options.UnescapePathValues = false
	rec2 := &recorded{}
	rawKeep.HandleNamed("info", consts.MethodGet, "/info/:user", echoRecord(rec2))
	_, err = rawKeep.URLFor("info", map[string]string{"user": "a/b"})
	assert.Assert(t, errors.Is(err, ErrRouteParamInvalid), "got %v", err)
	uk := mustURLFor(t, rawKeep, "info", map[string]string{"user": "a%2Fb"})
	wk := performRequest(rawKeep, consts.MethodGet, uk.String())
	assert.DeepEqual(t, consts.StatusOK, wk.Code)
	assert.DeepEqual(t, "a%2Fb", rec2.user)
}

func TestURLForStaticConflict(t *testing.T) {
	e := NewEngine(config.NewOptions(nil))
	rec := &recorded{}
	// static route wins over the param route by tree priority
	staticHandler := func(c context.Context, ctx *app.RequestContext) {
		ctx.String(consts.StatusOK, "static")
	}
	e.HandleNamed("user.new", consts.MethodGet, "/users/new", staticHandler)
	e.HandleNamed("user", consts.MethodGet, "/users/:id", echoRecord(rec))

	// must not emit a url that silently lands on the static route
	_, err := e.URLFor("user", map[string]string{"id": "new"})
	assert.Assert(t, errors.Is(err, ErrRouteNoRoundTrip), "got %v", err)

	u := mustURLFor(t, e, "user", map[string]string{"id": "42"})
	w := performRequest(e, consts.MethodGet, u.String())
	assert.DeepEqual(t, consts.StatusOK, w.Code)
	assert.DeepEqual(t, "/users/:id", rec.fullPath)
	assert.DeepEqual(t, "42", rec.id)

	// the static route itself re-matches
	us := mustURLFor(t, e, "user.new", nil)
	assert.DeepEqual(t, "/users/new", us.Path())
	ws := performRequest(e, consts.MethodGet, us.String())
	assert.DeepEqual(t, consts.StatusOK, ws.Code)
	assert.DeepEqual(t, "static", ws.Body.String())
}

func TestURLForGroupPrefix(t *testing.T) {
	e := NewEngine(config.NewOptions(nil))
	rec := &recorded{}
	g := e.Group("/api/v1")
	g.HandleNamed("user", consts.MethodGet, "/users/:id", echoRecord(rec))

	u := mustURLFor(t, e, "user", map[string]string{"id": "9"})
	// name is bound after composing the group prefix
	assert.DeepEqual(t, "/api/v1/users/9", u.String())

	w := performRequest(e, consts.MethodGet, u.String())
	assert.DeepEqual(t, consts.StatusOK, w.Code)
	assert.DeepEqual(t, "/api/v1/users/:id", rec.fullPath)
	assert.DeepEqual(t, "9", rec.id)
}

func TestURLForInvalidInputsDoNotPollute(t *testing.T) {
	e := NewEngine(config.NewOptions(nil))
	rec := &recorded{}
	e.HandleNamed("user", consts.MethodGet, "/users/:id", echoRecord(rec))

	if u, err := e.URLFor("missing", map[string]string{"id": "1"}); err == nil || u != nil {
		t.Fatalf("unknown name expected error, got %v %v", u, err)
	} else {
		assert.Assert(t, errors.Is(err, ErrRouteNameNotFound))
	}
	if _, err := e.URLFor("user", map[string]string{}); !errors.Is(err, ErrRouteParamMissing) {
		t.Fatalf("want ErrRouteParamMissing, got %v", err)
	}
	if _, err := e.URLFor("user", map[string]string{"id": "1", "bogus": "x"}); !errors.Is(err, ErrRouteParamUnexpected) {
		t.Fatalf("want ErrRouteParamUnexpected, got %v", err)
	}
	if _, err := e.URLFor("user", map[string]string{"id": ""}); !errors.Is(err, ErrRouteParamInvalid) {
		t.Fatalf("want ErrRouteParamInvalid, got %v", err)
	}

	// failed calls must leave no half-state: a valid call and request still work,
	// and a following request is not polluted by prior rejected parameters.
	u := mustURLFor(t, e, "user", map[string]string{"id": "123"})
	w := performRequest(e, consts.MethodGet, u.String())
	assert.DeepEqual(t, consts.StatusOK, w.Code)
	assert.DeepEqual(t, "/users/:id", rec.fullPath)
	assert.DeepEqual(t, "123", rec.id)
	w2 := performRequest(e, consts.MethodGet, "/users/123")
	assert.DeepEqual(t, consts.StatusOK, w2.Code)
	assert.DeepEqual(t, "123", rec.id)
}

func TestURLForDuplicateNamePanics(t *testing.T) {
	e := NewEngine(config.NewOptions(nil))
	e.HandleNamed("dup", consts.MethodGet, "/a/:id", echoRecord(&recorded{}))

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("duplicate route name on a different path must panic")
			}
		}()
		e.HandleNamed("dup", consts.MethodGet, "/b/:id", echoRecord(&recorded{}))
	}()

	// name uniqueness is global, even across HTTP methods
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("duplicate route name across methods must panic")
			}
		}()
		e.HandleNamed("dup", consts.MethodPost, "/c/:id", echoRecord(&recorded{}))
	}()

	// same path with different methods and different names is allowed
	e.HandleNamed("a.post", consts.MethodPost, "/a/:id", echoRecord(&recorded{}))
	mustURLFor(t, e, "a.post", map[string]string{"id": "1"})

	// empty explicit name panics
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("empty route name must panic")
			}
		}()
		e.HandleNamed("", consts.MethodGet, "/x", echoRecord(&recorded{}))
	}()
}

func TestURLForRoutesReportName(t *testing.T) {
	e := NewEngine(config.NewOptions(nil))
	e.GET("/unnamed/:id", echoRecord(&recorded{}))
	e.HandleNamed("named", consts.MethodGet, "/named/:id", echoRecord(&recorded{}))
	e.HandleNamed("named.post", consts.MethodPost, "/named/:id", echoRecord(&recorded{}))

	var foundUnnamed, foundNamed, foundNamedPost bool
	for _, r := range e.Routes() {
		switch {
		case r.Method == consts.MethodGet && r.Path == "/unnamed/:id":
			foundUnnamed = true
			assert.DeepEqual(t, "", r.Name) // old unnamed registration stays compatible
		case r.Method == consts.MethodGet && r.Path == "/named/:id":
			foundNamed = true
			assert.DeepEqual(t, "named", r.Name)
		case r.Method == consts.MethodPost && r.Path == "/named/:id":
			foundNamedPost = true
			assert.DeepEqual(t, "named.post", r.Name)
		}
	}
	assert.Assert(t, foundUnnamed && foundNamed && foundNamedPost)
}

func TestURLForTrailingSlashDirectMatch(t *testing.T) {
	e := NewEngine(config.NewOptions(nil))
	rec := &recorded{}
	// trailing-slash route must be hit directly, not via a redirect
	e.HandleNamed("cmd", consts.MethodGet, "/cmd/:tool/", echoRecord(rec))

	u := mustURLFor(t, e, "cmd", map[string]string{"tool": "hello"})
	assert.DeepEqual(t, "/cmd/hello/", u.Path())
	w := performRequest(e, consts.MethodGet, u.String())
	assert.DeepEqual(t, consts.StatusOK, w.Code)
	assert.DeepEqual(t, "/cmd/:tool/", rec.fullPath)
}

func TestURLForRemoveExtraSlash(t *testing.T) {
	opt := config.NewOptions(nil)
	opt.RemoveExtraSlash = true
	e := NewEngine(opt)
	rec := &recorded{}
	g := e.Group("/api")
	g.HandleNamed("file", consts.MethodGet, "/files/*filepath", echoRecord(rec))

	u := mustURLFor(t, e, "file", map[string]string{"filepath": "a/b/c.txt"})
	w := performRequest(e, consts.MethodGet, u.String())
	assert.DeepEqual(t, consts.StatusOK, w.Code)
	assert.DeepEqual(t, "/api/files/*filepath", rec.fullPath)
	assert.DeepEqual(t, "a/b/c.txt", rec.filepath)

	// "."/".." cannot survive CleanPath
	if _, err := e.URLFor("file", map[string]string{"filepath": "a/../b"}); !errors.Is(err, ErrRouteParamInvalid) {
		t.Fatalf("want ErrRouteParamInvalid, got %v", err)
	}
}

func TestURLForRedirectFixedPathDoesNotMaskRejection(t *testing.T) {
	opt := config.NewOptions(nil)
	opt.RedirectFixedPath = true
	e := NewEngine(opt)
	e.HandleNamed("u", consts.MethodGet, "/users/:id", echoRecord(&recorded{}))
	// value that would 404/redirect rather than match the param route directly
	_, err := e.URLFor("u", map[string]string{"id": ""})
	assert.Assert(t, errors.Is(err, ErrRouteNoRoundTrip) || errors.Is(err, ErrRouteParamInvalid), "got %v", err)
}

func TestURLForSingleSegmentSpecialChars(t *testing.T) {
	e := NewEngine(config.NewOptions(nil))
	rec := &recorded{}
	e.HandleNamed("search", consts.MethodGet, "/search/:q", echoRecord(rec))

	// '?' and '#' must not start a query/fragment, '%' must not be reinterpreted,
	// '+' and Unicode must round-trip inside a single ":param" segment.
	want := "a?b#c%中+d e"
	u := mustURLFor(t, e, "search", map[string]string{"q": want})
	assert.Assert(t, !strings.Contains(u.Path(), "?"), u.Path())
	assert.Assert(t, !strings.Contains(u.Path(), "#"), u.Path())
	assert.DeepEqual(t, "", u.RawQuery()) // nothing leaked into the query

	w := performRequest(e, consts.MethodGet, u.String())
	assert.DeepEqual(t, consts.StatusOK, w.Code)
	assert.DeepEqual(t, "/search/:q", rec.fullPath)
	assert.DeepEqual(t, want, rec.q)
}

func TestURLForRootStatic(t *testing.T) {
	e := NewEngine(config.NewOptions(nil))
	e.HandleNamed("root", consts.MethodGet, "/", echoRecord(&recorded{}))
	u := mustURLFor(t, e, "root", nil)
	assert.DeepEqual(t, "/", u.String())
	w := performRequest(e, consts.MethodGet, u.String())
	assert.DeepEqual(t, consts.StatusOK, w.Code)
}

func TestURLForValuesArePerCall(t *testing.T) {
	e := NewEngine(config.NewOptions(nil))
	e.HandleNamed("u", consts.MethodGet, "/users/:id", echoRecord(&recorded{}))

	u1 := mustURLFor(t, e, "u", map[string]string{"id": "1"})
	params := map[string]string{"id": "2"}
	u2 := mustURLFor(t, e, "u", params)
	// mutating the caller map after generation does not affect the produced url
	params["id"] = "3"
	assert.DeepEqual(t, "/users/1", u1.String())
	assert.DeepEqual(t, "/users/2", u2.String())
}

func TestURLForConcurrentReadRace(t *testing.T) {
	e := NewEngine(config.NewOptions(nil))
	// stateless handler: it must not introduce test-level shared writes
	ok := func(c context.Context, ctx *app.RequestContext) { ctx.String(consts.StatusOK, "ok") }
	e.HandleNamed("u", consts.MethodGet, "/users/:id", ok)
	e.HandleNamed("src", consts.MethodGet, "/src/*filepath", ok)

	const goroutines, iterations = 16, 60
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				id := "42"
				u, err := e.URLFor("u", map[string]string{"id": id}, WithQuery("g", "x"), WithQuery("g", "y"))
				if err != nil {
					t.Errorf("URLFor u: %v", err)
					return
				}
				w := performRequest(e, consts.MethodGet, u.String())
				if w.Code != consts.StatusOK {
					t.Errorf("status = %d", w.Code)
					return
				}
				uf, err := e.URLFor("src", map[string]string{"filepath": "a/b/c.png"})
				if err != nil {
					t.Errorf("URLFor src: %v", err)
					return
				}
				if wf := performRequest(e, consts.MethodGet, uf.String()); wf.Code != consts.StatusOK {
					t.Errorf("catch-all status = %d", wf.Code)
					return
				}
				_ = e.Routes()
			}
		}(g)
	}
	wg.Wait()

	// after concurrent reads, a new generation (additional named route) is published
	rec := &recorded{}
	genBefore := e.routeRegistryLoad().gen
	e.HandleNamed("post", consts.MethodGet, "/posts/:pid", echoRecord(rec))
	assert.Assert(t, e.routeRegistryLoad().gen > genBefore)
	up := mustURLFor(t, e, "post", map[string]string{"pid": "7"})
	wp := performRequest(e, consts.MethodGet, up.String())
	assert.DeepEqual(t, consts.StatusOK, wp.Code)
	assert.DeepEqual(t, "/posts/:pid", rec.fullPath)
}
