package fetch

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PeacexF/MassD/internal/stats"
)

func testClient(o Options) *Client {
	o.BaseBackoff = time.Millisecond
	o.MaxBackoff = 5 * time.Millisecond
	return New(o)
}

func TestRetriesTransientStatus(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		io.WriteString(w, "ok")
	}))
	defer srv.Close()

	c := testClient(Options{})
	body, err := c.Bytes(context.Background(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "ok" || calls.Load() != 3 {
		t.Fatalf("body=%q calls=%d", body, calls.Load())
	}
}

func TestDoesNotRetryClientErrors(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "nope", http.StatusNotFound)
	}))
	defer srv.Close()

	c := testClient(Options{})
	_, err := c.Bytes(context.Background(), srv.URL)
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != 404 {
		t.Fatalf("err = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("404 was retried %d times", calls.Load()-1)
	}
}

func TestGivesUpAfterMaxRetries(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	c := testClient(Options{MaxRetries: 2})
	if _, err := c.Bytes(context.Background(), srv.URL); err == nil {
		t.Fatal("expected an error")
	}
	if calls.Load() != 3 {
		t.Fatalf("attempts = %d, want 3", calls.Load())
	}
}

func TestHonorsRetryAfter(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		io.WriteString(w, "ok")
	}))
	defer srv.Close()

	c := testClient(Options{})
	start := time.Now()
	if _, err := c.Bytes(context.Background(), srv.URL); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < time.Second {
		t.Fatal("Retry-After was ignored")
	}
}

func TestCountsRequestsAndBytes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "0123456789")
	}))
	defer srv.Close()

	st := stats.New()
	c := testClient(Options{Stats: st})
	if _, err := c.Bytes(context.Background(), srv.URL); err != nil {
		t.Fatal(err)
	}
	if st.Requests.Load() != 1 || st.Bytes.Load() != 10 {
		t.Fatalf("requests=%d bytes=%d", st.Requests.Load(), st.Bytes.Load())
	}
}

func TestEnforcesBodySizeLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(make([]byte, 1<<16))
	}))
	defer srv.Close()

	c := testClient(Options{MaxBodyBytes: 1024})
	_, err := c.Bytes(context.Background(), srv.URL)
	if !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("err = %v, want ErrBodyTooLarge", err)
	}
}

func TestRespectsContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	c := testClient(Options{Timeout: time.Second})
	if _, err := c.Bytes(ctx, srv.URL); err == nil {
		t.Fatal("expected cancellation error")
	}
}

func TestRateLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "x")
	}))
	defer srv.Close()

	c := testClient(Options{RateLimit: 20, Burst: 1})
	start := time.Now()
	for range 4 {
		if _, err := c.Bytes(context.Background(), srv.URL); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("4 requests at 20/s took %s", elapsed)
	}
}

func TestConcurrencySlotsAreReleased(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "x")
	}))
	defer srv.Close()

	c := testClient(Options{Concurrency: 1})
	for range 5 {
		resp, err := c.Get(context.Background(), srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

func TestParseLinks(t *testing.T) {
	h := `<https://api.github.com/repositories?since=369>; rel="next", <https://api.github.com/repositories{?since}>; rel="first"`
	links := ParseLinks(h)
	if links["next"] != "https://api.github.com/repositories?since=369" {
		t.Fatalf("next = %q", links["next"])
	}
	if len(ParseLinks("")) != 0 {
		t.Fatal("empty header should yield no links")
	}
	if len(ParseLinks("garbage")) != 0 {
		t.Fatal("malformed header should yield no links")
	}
}
