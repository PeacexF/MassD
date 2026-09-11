// Package rss collects RSS, Atom and RDF feeds. Incremental collection relies
// on conditional GETs (ETag / Last-Modified) plus unique (feed, guid) rows, so
// re-running a feed costs one request and inserts nothing new.
package rss

import (
	"bufio"
	"context"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/PeacexF/MassD/internal/db"
	"github.com/PeacexF/MassD/internal/fetch"
	"github.com/PeacexF/MassD/internal/record"
	"github.com/PeacexF/MassD/internal/source"
)

type Source struct{}

func init() { source.Register(&Source{}) }

func (s *Source) Name() string { return "rss" }

func (s *Source) Describe() string { return "RSS/Atom/RDF feeds listed in config" }

type feedState struct {
	ETag         string `json:"etag,omitempty"`
	LastModified string `json:"last_modified,omitempty"`
	LastFetched  string `json:"last_fetched,omitempty"`
	LastStatus   int    `json:"last_status,omitempty"`
}

type state struct {
	Feeds map[string]feedState `json:"feeds"`
}

func (s *Source) Collect(ctx context.Context, env *source.Environment) error {
	feeds, err := feedList(env)
	if err != nil {
		return err
	}
	if len(feeds) == 0 {
		env.Log.Warn("no feeds configured; set rss.feeds or rss.feeds_file")
		return nil
	}

	var st state
	if err := env.LoadState(&st); err != nil {
		return err
	}
	if st.Feeds == nil {
		st.Feeds = map[string]feedState{}
	}

	maxBody := int64(env.Config.Int("max_feed_bytes", 32<<20))
	client := env.HTTP.WithOverrides(func(o *fetch.Options) {
		o.MaxBodyBytes = maxBody
		o.PerHost = true
		if o.RateLimit <= 0 {
			o.RateLimit = 4
		}
	})

	workers := env.Config.Int("workers", env.Opts.Workers)
	if workers <= 0 || workers > len(feeds) {
		workers = min(max(len(feeds), 1), 32)
	}

	type job struct {
		url  string
		prev feedState
	}
	type result struct {
		url   string
		state feedState
	}
	jobs := make(chan job)
	results := make(chan result, workers)

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				next, err := s.collectFeed(ctx, env, client, j.url, j.prev)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					env.Error(ctx, err, "feed "+j.url)
				}
				select {
				case results <- result{url: j.url, state: next}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, url := range feeds {
			select {
			case jobs <- job{url: url, prev: st.Feeds[url]}:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() { wg.Wait(); close(results) }()

	// Each feed is dispatched once with its prior state, so the map is only
	// ever touched by this goroutine.
	done := 0
	for r := range results {
		st.Feeds[r.url] = r.state
		done++
		if done%64 == 0 {
			if err := env.SaveState(ctx, st); err != nil {
				return err
			}
		}
	}
	if ctx.Err() != nil {
		_ = env.SaveState(ctx, st)
		return ctx.Err()
	}
	return env.SaveState(ctx, st)
}

func (s *Source) collectFeed(ctx context.Context, env *source.Environment, client *fetch.Client, url string, prev feedState) (feedState, error) {
	next := prev
	next.LastFetched = db.Now()

	req, err := client.NewRequest(ctx, http.MethodGet, url, nil)
	if err != nil {
		return next, err
	}
	if prev.ETag != "" {
		req.Header.Set("If-None-Match", prev.ETag)
	}
	if prev.LastModified != "" {
		req.Header.Set("If-Modified-Since", prev.LastModified)
	}
	req.Header.Set("Accept", "application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.8")

	resp, err := client.Do(req)
	if err != nil {
		return next, err
	}
	defer resp.Body.Close()

	next.LastStatus = resp.StatusCode
	if resp.StatusCode == http.StatusNotModified {
		env.Log.Debug("feed unchanged", "feed", url)
		return next, nil
	}
	next.ETag = resp.Header.Get("ETag")
	next.LastModified = resp.Header.Get("Last-Modified")

	feed, err := Parse(resp.Body)
	if err != nil {
		return next, err
	}

	now := db.Now()
	batch := make([]record.Record, 0, len(feed.Items)+1)
	batch = append(batch, record.New("rss_feeds").
		Set("url", url).
		Set("title", null(feed.Title)).
		Set("link", null(feed.Link)).
		Set("description", null(feed.Description)).
		Set("language", null(feed.Language)).
		Set("last_status", resp.StatusCode).
		Set("last_fetched_at", now).
		Set("item_count", len(feed.Items)).
		Set("updated_at", now).
		OnConflict(record.Replace))

	for _, it := range feed.Items {
		if it.GUID == "" {
			continue
		}
		batch = append(batch, record.New("rss_items").
			Set("feed", url).
			Set("guid", it.GUID).
			Set("url", null(it.URL)).
			Set("title", null(it.Title)).
			Set("author", null(it.Author)).
			Set("summary", null(it.Summary)).
			Set("content", null(it.Content)).
			Set("categories", null(strings.Join(it.Categories, ", "))).
			Set("published_at", nullTime(it.PublishedAt)).
			Set("updated_at", nullTime(it.UpdatedAt)).
			Set("collected_at", now).
			Set("run_id", env.RunID).
			OnConflict(record.Ignore))
	}

	env.Log.Debug("feed collected", "feed", url, "items", len(batch)-1)
	return next, env.Emit(ctx, batch...)
}

func feedList(env *source.Environment) ([]string, error) {
	seen := map[string]bool{}
	var out []string
	add := func(u string) {
		u = strings.TrimSpace(u)
		if u == "" || strings.HasPrefix(u, "#") || seen[u] {
			return
		}
		seen[u] = true
		out = append(out, u)
	}

	for _, u := range env.Config.Strings("feeds") {
		add(u)
	}
	if path := env.Config.String("feeds_file", ""); path != "" {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
		for sc.Scan() {
			add(sc.Text())
		}
		if err := sc.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func null(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339)
}
