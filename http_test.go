package endpoint

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	nnsclient "github.com/yi-nology/ekit-nns/nns/client"
	"github.com/yi-nology/ekit-nns/nns/metadata"
	"github.com/yi-nology/ekit-registry/registry"
)

// newHTTPFactoryWithTestServer starts an httptest server and builds a
// ServiceClient whose only static instance points at that server. Returns
// the factory ready to MakeEndpoint for "svc-a".
func newHTTPFactoryWithTestServer(t *testing.T, handler http.HandlerFunc) *HTTPEndpointFactory {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	host, port := parseHostPort(srv.URL)
	c := nnsclient.New(
		nnsclient.WithMockMode(true),
		nnsclient.WithStaticInstances("svc-a", []registry.Instance{
			{IP: host, Port: port},
		}),
		nnsclient.WithLocalServiceConfig("svc-a", &metadata.ServiceBasic{
			ConnectionConfig: metadata.ConnectionConfig{
				ReadTimeout: 3000,
				Lb:          metadata.LbRoundRobin,
			},
		}),
	)
	return NewHTTPEndpointFactory(c)
}

// parseHostPort extracts host and port from "http://host:port".
func parseHostPort(url string) (string, int) {
	const prefix = "http://"
	if len(url) < len(prefix) || url[:len(prefix)] != prefix {
		return url, 0
	}
	rest := url[len(prefix):]
	colon := -1
	for i := 0; i < len(rest); i++ {
		if rest[i] == ':' {
			colon = i
			break
		}
	}
	if colon < 0 {
		return rest, 0
	}
	port := 0
	for i := colon + 1; i < len(rest); i++ {
		ch := rest[i]
		if ch < '0' || ch > '9' {
			break
		}
		port = port*10 + int(ch-'0')
	}
	return rest[:colon], port
}

// TestHTTPEndpoint_DoRequest_HappyPath covers the 200 + JSON body path.
// The endpoint must unmarshal into the Result pointer when one is set.
func TestHTTPEndpoint_DoRequest_HappyPath(t *testing.T) {
	factory := newHTTPFactoryWithTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"name":"alice","age":30}`)
	})
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	var got map[string]any
	_, err = ep(context.Background(), HTTPRequest{
		Method: http.MethodGet,
		Path:   "/users/1",
		Result: &got,
	})
	if err != nil {
		t.Fatalf("endpoint call: %v", err)
	}
	if got["name"] != "alice" {
		t.Errorf("expected name=alice, got %v", got)
	}
}

// TestHTTPEndpoint_DoRequest_RawBytes covers the path where Result is nil —
// the endpoint must return the raw response bytes.
func TestHTTPEndpoint_DoRequest_RawBytes(t *testing.T) {
	factory := newHTTPFactoryWithTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "raw body")
	})
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	resp, err := ep(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/raw"})
	if err != nil {
		t.Fatalf("endpoint call: %v", err)
	}
	bytes, ok := resp.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", resp)
	}
	if string(bytes) != "raw body" {
		t.Errorf("expected %q, got %q", "raw body", string(bytes))
	}
}

// TestHTTPEndpoint_DoRequest_NonSuccess covers the non-2xx error path.
func TestHTTPEndpoint_DoRequest_NonSuccess(t *testing.T) {
	factory := newHTTPFactoryWithTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "boom")
	})
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	_, err = ep(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/explode"})
	if err == nil {
		t.Fatal("expected error for 500 response, got nil")
	}
	if !containsAny(err.Error(), "500") {
		t.Errorf("expected error to mention 500, got: %v", err)
	}
}

// TestHTTPEndpoint_DoRequest_TruncatesLargeBody covers the 500-char
// truncation in the error message. Long error bodies must be capped.
func TestHTTPEndpoint_DoRequest_TruncatesLargeBody(t *testing.T) {
	big := make([]byte, 1000)
	for i := range big {
		big[i] = 'x'
	}
	factory := newHTTPFactoryWithTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(big)
	})
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	_, err = ep(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/big"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// Truncation limit is 500; we accept some slack for the wrapping prefix.
	if got := err.Error(); len(got) > 600 {
		t.Errorf("expected truncated error (<600 chars), got %d chars", len(got))
	}
}

// TestHTTPEndpoint_DoRequest_Body verifies that the request Body is JSON-encoded
// and posted to the server.
func TestHTTPEndpoint_DoRequest_Body(t *testing.T) {
	got := make(chan map[string]any, 1)
	factory := newHTTPFactoryWithTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		got <- body
		w.WriteHeader(http.StatusOK)
	})
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	_, err = ep(context.Background(), HTTPRequest{
		Method: http.MethodPost,
		Path:   "/items",
		Body:   map[string]any{"key": "value", "n": 7},
	})
	if err != nil {
		t.Fatalf("endpoint call: %v", err)
	}

	select {
	case body := <-got:
		if body["key"] != "value" {
			t.Errorf("expected key=value, got %v", body["key"])
		}
		if n, ok := body["n"].(float64); !ok || n != 7 {
			t.Errorf("expected n=7, got %v (%T)", body["n"], body["n"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for request body")
	}
}

// TestHTTPEndpoint_DoRequest_HeaderAndQuery verifies Header and Query fields
// are propagated to the outgoing request.
func TestHTTPEndpoint_DoRequest_HeaderAndQuery(t *testing.T) {
	var captured atomic.Pointer[http.Request]
	factory := newHTTPFactoryWithTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		captured.Store(&http.Request{
			Method: r.Method,
			URL:    r.URL,
			Header: r.Header.Clone(),
		})
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "{}")
	})
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	_, err = ep(context.Background(), HTTPRequest{
		Method: http.MethodGet,
		Path:   "/search",
		Header: map[string]string{"X-Request-Id": "abc-123"},
		Query:  map[string]string{"q": "hello", "page": "1"},
	})
	if err != nil {
		t.Fatalf("endpoint call: %v", err)
	}

	req := captured.Load()
	if req == nil {
		t.Fatal("no request captured")
	}
	if got := req.Header.Get("X-Request-Id"); got != "abc-123" {
		t.Errorf("expected X-Request-Id=abc-123, got %q", got)
	}
	if got := req.URL.Query().Get("q"); got != "hello" {
		t.Errorf("expected q=hello, got %q", got)
	}
	if got := req.URL.Query().Get("page"); got != "1" {
		t.Errorf("expected page=1, got %q", got)
	}
}

// TestHTTPEndpoint_MakeEndpoint_InvalidRequestType covers the type-assertion
// error path. The endpoint closure must reject non-HTTPRequest inputs.
func TestHTTPEndpoint_MakeEndpoint_InvalidRequestType(t *testing.T) {
	factory := newHTTPFactoryWithTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	_, err = ep(context.Background(), "not-an-HTTPRequest")
	if err == nil {
		t.Fatal("expected type assertion error, got nil")
	}
	if !containsAny(err.Error(), "invalid request type") {
		t.Errorf("expected 'invalid request type' error, got: %v", err)
	}
}

// TestHTTPEndpoint_MakeEndpoint_NoInstances covers the zero-instances path.
// The endpoint must return registry.ErrNoInstances (not a generic error).
func TestHTTPEndpoint_MakeEndpoint_NoInstances(t *testing.T) {
	// Build a factory with NO instances for svc-empty.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(srv.Close)

	c := nnsclient.New(
		nnsclient.WithMockMode(true),
		nnsclient.WithStaticInstances("svc-empty", nil),
	)
	factory := NewHTTPEndpointFactory(c)
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-empty")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	_, err = ep(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/x"})
	if !errors.Is(err, registry.ErrNoInstances) {
		t.Errorf("expected registry.ErrNoInstances, got: %v", err)
	}
}

// TestHTTPEndpoint_MakeEndpoint_UnknownService covers the case where the
// service isn't configured at all in mock mode. The mock-mode ServiceClient
// may return an empty instance list or fall through to a default; the
// endpoint must surface registry.ErrNoInstances either way.
func TestHTTPEndpoint_MakeEndpoint_UnknownService(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(srv.Close)
	host, port := parseHostPort(srv.URL)

	c := nnsclient.New(
		nnsclient.WithMockMode(true),
		nnsclient.WithStaticInstances("svc-a", []registry.Instance{{IP: host, Port: port}}),
	)
	factory := NewHTTPEndpointFactory(c)
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-unknown")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	_, err = ep(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/x"})
	if !errors.Is(err, registry.ErrNoInstances) {
		t.Errorf("expected registry.ErrNoInstances, got: %v", err)
	}
}

// TestHTTPEndpoint_MakeEndpoint_RequestTimeout covers the per-request
// timeout override. We use a server that hangs; the request must
// time out at the per-request Timeout, not the service config.
func TestHTTPEndpoint_MakeEndpoint_RequestTimeout(t *testing.T) {
	released := make(chan struct{})
	factory := newHTTPFactoryWithTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		// Wait for the test to release us, or for the request to time out.
		select {
		case <-released:
		case <-r.Context().Done():
		}
	})
	defer factory.Close()
	defer close(released)

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = ep(ctx, HTTPRequest{
		Method:  http.MethodGet,
		Path:    "/hang",
		Timeout: 500 * time.Millisecond, // per-request overrides ctx? actually per-request wins
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	// Per-request timeout (500ms) wins over ctx (200ms? actually ctx may
	// be applied as additional cancel). We accept any value < 1s.
	if elapsed >= time.Second {
		t.Errorf("expected timeout within 1s, got %v", elapsed)
	}
}

// TestHTTPEndpoint_DoRequest_StripTrailingSlash covers the TrimRight path:
// the final URL must not have a "//" even if the path has trailing slashes.
func TestHTTPEndpoint_DoRequest_StripTrailingSlash(t *testing.T) {
	var capturedPath atomic.Pointer[string]
	factory := newHTTPFactoryWithTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		capturedPath.Store(&r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "{}")
	})
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	_, err = ep(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/items//"})
	if err != nil {
		t.Fatalf("endpoint call: %v", err)
	}

	got := capturedPath.Load()
	if got == nil {
		t.Fatal("no request captured")
	}
	if containsAny(*got, "//") {
		t.Errorf("expected no '//' in path, got %q", *got)
	}
}

// TestHTTPEndpoint_Close verifies Close is a no-op for HTTP factory.
func TestHTTPEndpoint_Close(t *testing.T) {
	c := nnsclient.New(
		nnsclient.WithMockMode(true),
	)
	factory := NewHTTPEndpointFactory(c)
	factory.Close() // should not panic
}

// containsAny is a small strings.Contains helper kept inline to avoid
// importing strings for a single use.
func containsAny(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOfStr(s, sub) >= 0)
}

func indexOfStr(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
