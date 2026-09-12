// Package fetch is the shared HTTP layer: retries, backoff, rate limiting,
// concurrency limits, size limits and byte accounting. Sources do not implement
// any of this themselves.
package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PeacexF/MassD/internal/stats"
)

const DefaultUserAgent = "MassD/0.1 (+https://github.com/PeacexF/MassD)"

type Options struct {
	UserAgent    string
	Timeout      time.Duration
	MaxRetries   int
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
	MaxBodyBytes int64
	RateLimit    float64
	Burst        int
	PerHost      bool
	Concurrency  int
	Headers      map[string]string
	Stats        *stats.Stats
	Transport    http.RoundTripper
}

func (o Options) withDefaults() Options {
	if o.UserAgent == "" {
		o.UserAgent = DefaultUserAgent
	}
	if o.Timeout == 0 {
		o.Timeout = 60 * time.Second
	}
	if o.MaxRetries == 0 {
		o.MaxRetries = 4
	}
	if o.BaseBackoff == 0 {
		o.BaseBackoff = 500 * time.Millisecond
	}
	if o.MaxBackoff == 0 {
		o.MaxBackoff = 30 * time.Second
	}
	if o.Burst <= 0 {
		o.Burst = 1
	}
	if o.Concurrency <= 0 {
		o.Concurrency = 8
	}
	return o
}

type Client struct {
	opts    Options
	http    *http.Client
	sem     chan struct{}
	global  *limiter
	mu      sync.Mutex
	perHost map[string]*limiter
}

func New(opts Options) *Client {
	opts = opts.withDefaults()

	tr := opts.Transport
	if tr == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.MaxIdleConnsPerHost = opts.Concurrency
		t.MaxConnsPerHost = opts.Concurrency * 2
		t.IdleConnTimeout = 90 * time.Second
		t.ResponseHeaderTimeout = opts.Timeout
		tr = t
	}

	c := &Client{
		opts:    opts,
		http:    &http.Client{Transport: tr},
		sem:     make(chan struct{}, opts.Concurrency),
		perHost: map[string]*limiter{},
	}
	if opts.RateLimit > 0 && !opts.PerHost {
		c.global = newLimiter(opts.RateLimit, opts.Burst)
	}
	return c
}

// WithOverrides clones the client with adjusted options, sharing nothing but
// the transport. Sources use it to apply their own rate limits.
func (c *Client) WithOverrides(mutate func(*Options)) *Client {
	o := c.opts
	if o.Headers != nil {
		h := make(map[string]string, len(o.Headers))
		for k, v := range o.Headers {
			h[k] = v
		}
		o.Headers = h
	}
	o.Transport = c.http.Transport
	mutate(&o)
	return New(o)
}

func (c *Client) UserAgent() string { return c.opts.UserAgent }

type HTTPError struct {
	StatusCode int
	Status     string
	URL        string
	Snippet    string
	Header     http.Header
}

func (e *HTTPError) Error() string {
	if e.Snippet != "" {
		return fmt.Sprintf("%s: %s: %s", e.URL, e.Status, e.Snippet)
	}
	return fmt.Sprintf("%s: %s", e.URL, e.Status)
}

// ErrBodyTooLarge is returned when a response exceeds MaxBodyBytes.
var ErrBodyTooLarge = errors.New("response body exceeds size limit")

func (c *Client) NewRequest(ctx context.Context, method, rawURL string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, rawURL, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.opts.UserAgent)
	for k, v := range c.opts.Headers {
		req.Header.Set(k, v)
	}
	return req, nil
}

// Do sends req with retries and returns a streaming response. The caller closes
// the body. Only transient failures are retried.
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	replayable := req.Body == nil || req.GetBody != nil

	for attempt := 0; ; attempt++ {
		if err := c.acquire(ctx, req.URL); err != nil {
			return nil, err
		}

		attemptReq := req
		if attempt > 0 && req.GetBody != nil {
			b, err := req.GetBody()
			if err != nil {
				c.release()
				return nil, err
			}
			attemptReq = req.Clone(ctx)
			attemptReq.Body = b
		}

		reqCtx, cancel := context.WithTimeout(ctx, c.opts.Timeout)
		resp, err := c.http.Do(attemptReq.WithContext(reqCtx))
		if c.opts.Stats != nil {
			c.opts.Stats.Requests.Add(1)
		}

		switch {
		case err != nil:
			cancel()
			c.release()
			if !replayable || !retryableErr(err) || attempt >= c.opts.MaxRetries {
				return nil, fmt.Errorf("%s %s: %w", req.Method, req.URL.Redacted(), err)
			}
		case retryableStatus(resp.StatusCode):
			wait := retryAfter(resp)
			drain(resp)
			cancel()
			c.release()
			httpErr := &HTTPError{StatusCode: resp.StatusCode, Status: resp.Status, URL: req.URL.Redacted(), Header: resp.Header}
			if !replayable || attempt >= c.opts.MaxRetries {
				return nil, httpErr
			}
			if wait > 0 {
				if err := sleep(ctx, wait); err != nil {
					return nil, err
				}
				continue
			}
		case resp.StatusCode >= 400:
			snippet := readSnippet(resp)
			cancel()
			c.release()
			return nil, &HTTPError{StatusCode: resp.StatusCode, Status: resp.Status, URL: req.URL.Redacted(), Snippet: snippet, Header: resp.Header}
		default:
			resp.Body = &trackedBody{
				rc:     resp.Body,
				limit:  c.opts.MaxBodyBytes,
				st:     c.opts.Stats,
				cancel: cancel,
				done:   c.release,
			}
			return resp, nil
		}

		if err := sleep(ctx, c.backoff(attempt)); err != nil {
			return nil, err
		}
	}
}

func (c *Client) Get(ctx context.Context, rawURL string) (*http.Response, error) {
	req, err := c.NewRequest(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

// Bytes reads a whole response into memory. Only for small, bounded payloads.
func (c *Client) Bytes(ctx context.Context, rawURL string) ([]byte, error) {
	resp, err := c.Get(ctx, rawURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (c *Client) acquire(ctx context.Context, u *url.URL) error {
	lim := c.global
	if c.opts.PerHost && c.opts.RateLimit > 0 {
		c.mu.Lock()
		l, ok := c.perHost[u.Host]
		if !ok {
			l = newLimiter(c.opts.RateLimit, c.opts.Burst)
			c.perHost[u.Host] = l
		}
		c.mu.Unlock()
		lim = l
	}
	if lim != nil {
		if err := lim.wait(ctx); err != nil {
			return err
		}
	}
	select {
	case c.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) release() {
	select {
	case <-c.sem:
	default:
	}
}

func (c *Client) backoff(attempt int) time.Duration {
	d := c.opts.BaseBackoff << min(attempt, 16)
	if d > c.opts.MaxBackoff || d <= 0 {
		d = c.opts.MaxBackoff
	}
	// Full jitter keeps parallel workers from retrying in lockstep.
	return time.Duration(float64(d) * (0.5 + rand.Float64()/2))
}

// trackedBody accounts for downloaded bytes, enforces the size limit and
// releases the concurrency slot exactly once, on Close.
type trackedBody struct {
	rc     io.ReadCloser
	limit  int64
	read   int64
	st     *stats.Stats
	cancel context.CancelFunc
	done   func()
	closed bool
}

func (b *trackedBody) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	if n > 0 {
		b.read += int64(n)
		if b.st != nil {
			b.st.Bytes.Add(int64(n))
		}
		if b.limit > 0 && b.read > b.limit {
			return n, ErrBodyTooLarge
		}
	}
	return n, err
}

func (b *trackedBody) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true
	err := b.rc.Close()
	b.cancel()
	b.done()
	return err
}

func retryableStatus(code int) bool {
	switch code {
	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	}
	return false
}

func retryableErr(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		// A cancelled parent context is final; a per-attempt timeout is not.
		return errors.Is(err, context.DeadlineExceeded)
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) || strings.Contains(err.Error(), "connection reset")
}

func retryAfter(resp *http.Response) time.Duration {
	v := resp.Header.Get("Retry-After")
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

func drain(resp *http.Response) {
	io.CopyN(io.Discard, resp.Body, 4<<10)
	resp.Body.Close()
}

func readSnippet(resp *http.Response) string {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	resp.Body.Close()
	return strings.TrimSpace(strings.ReplaceAll(string(b), "\n", " "))
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
