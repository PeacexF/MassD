// Package commoncrawl treats Common Crawl as a data source rather than a crawl
// target. The index is streamed from the published CDX shards, and individual
// documents are retrieved from WARC with ranged requests.
package commoncrawl

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/PeacexF/MassD/internal/db"
	"github.com/PeacexF/MassD/internal/fetch"
	"github.com/PeacexF/MassD/internal/record"
	"github.com/PeacexF/MassD/internal/source"
)

const (
	defaultDataBase  = "https://data.commoncrawl.org"
	defaultIndexBase = "https://index.commoncrawl.org"
)

type Source struct{}

func init() { source.Register(&Source{}) }

func (s *Source) Name() string { return "commoncrawl" }

func (s *Source) Describe() string {
	return "Common Crawl CDX index shards and WARC documents"
}

type state struct {
	Crawl       string `json:"crawl"`
	Shard       int    `json:"shard"`
	Line        int64  `json:"line"`
	PagesCursor int64  `json:"pages_cursor"`
}

type collector struct {
	env       *source.Environment
	client    *fetch.Client
	dataBase  string
	indexBase string
	crawl     string
	st        *state
}

func (s *Source) Collect(ctx context.Context, env *source.Environment) error {
	datasets := env.Config.Strings("datasets")
	if len(datasets) == 0 {
		datasets = []string{"index"}
	}

	var st state
	if err := env.LoadState(&st); err != nil {
		return err
	}

	client := env.HTTP.WithOverrides(func(o *fetch.Options) {
		o.MaxBodyBytes = int64(env.Config.Int("max_response_bytes", 0))
		if o.RateLimit <= 0 {
			o.RateLimit = float64(env.Config.Int("rate_limit", 20))
		}
	})

	c := &collector{
		env:       env,
		client:    client,
		dataBase:  strings.TrimSuffix(env.Config.String("data_url", defaultDataBase), "/"),
		indexBase: strings.TrimSuffix(env.Config.String("index_url", defaultIndexBase), "/"),
		st:        &st,
	}

	crawl, err := c.resolveCrawl(ctx)
	if err != nil {
		return err
	}
	c.crawl = crawl
	if st.Crawl != crawl {
		env.Log.Info("switching crawl", "from", st.Crawl, "to", crawl)
		st.Crawl, st.Shard, st.Line = crawl, 0, 0
	}

	for _, ds := range datasets {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch strings.ToLower(strings.TrimSpace(ds)) {
		case "index":
			err = c.collectIndex(ctx)
		case "pages", "warc":
			err = c.collectPages(ctx)
		default:
			err = fmt.Errorf("unknown commoncrawl dataset %q", ds)
		}
		if errors.Is(err, source.ErrLimit) {
			return err
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			env.Error(ctx, err, "dataset "+ds)
		}
	}
	return nil
}

type crawlInfo struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	CDXAPI string `json:"cdx-api"`
	From   string `json:"from"`
	To     string `json:"to"`
}

// resolveCrawl picks the configured crawl, or the newest published one.
func (c *collector) resolveCrawl(ctx context.Context) (string, error) {
	if id := c.env.Config.String("crawl", ""); id != "" && !strings.EqualFold(id, "latest") {
		return id, nil
	}

	body, err := c.client.Bytes(ctx, c.indexBase+"/collinfo.json")
	if err != nil {
		return "", fmt.Errorf("list crawls: %w", err)
	}
	var crawls []crawlInfo
	if err := json.Unmarshal(body, &crawls); err != nil {
		return "", fmt.Errorf("parse collinfo.json: %w", err)
	}
	if len(crawls) == 0 {
		return "", errors.New("collinfo.json listed no crawls")
	}

	now := db.Now()
	batch := make([]record.Record, 0, len(crawls))
	for _, info := range crawls {
		batch = append(batch, record.New("commoncrawl_crawls").
			Set("id", info.ID).
			Set("name", null(info.Name)).
			Set("cdx_api", null(info.CDXAPI)).
			Set("from_at", null(info.From)).
			Set("to_at", null(info.To)).
			Set("collected_at", now).
			OnConflict(record.Replace))
	}
	if err := c.env.Emit(ctx, batch...); err != nil {
		return "", err
	}
	// collinfo.json is ordered newest first.
	return crawls[0].ID, nil
}

func (c *collector) shardURL(n int) string {
	return fmt.Sprintf("%s/cc-index/collections/%s/indexes/cdx-%05d.gz", c.dataBase, c.crawl, n)
}

func (c *collector) collectIndex(ctx context.Context) error {
	env := c.env
	shardCount := env.Config.Int("shard_count", 300)
	maxShards := max(env.Config.Int("max_shards", 1), 1)
	checkpoint := int64(max(env.Config.Int("checkpoint_lines", 50000), 1))

	for done := 0; done < maxShards; done++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if c.st.Shard >= shardCount {
			env.Log.Info("crawl index fully ingested", "crawl", c.crawl, "shards", shardCount)
			return nil
		}

		shard := c.st.Shard
		lines, err := c.ingestShard(ctx, shard, checkpoint)
		if err != nil {
			return err
		}

		now := db.Now()
		if err := env.Emit(ctx, record.New("commoncrawl_shards").
			Set("crawl", c.crawl).
			Set("shard", fmt.Sprintf("cdx-%05d", shard)).
			Set("lines", lines).
			Set("bytes", 0).
			Set("completed_at", now).
			Set("run_id", env.RunID).
			OnConflict(record.Replace)); err != nil {
			return err
		}

		c.st.Shard, c.st.Line = shard+1, 0
		if err := env.SaveState(ctx, c.st); err != nil {
			return err
		}
		env.Log.Info("shard complete", "crawl", c.crawl, "shard", shard, "lines", lines)
	}
	return nil
}

// ingestShard streams one gzipped CDX shard. Resuming inside a shard re-reads
// its earlier lines, which costs bandwidth but never duplicates rows and keeps
// state to a single small cursor.
func (c *collector) ingestShard(ctx context.Context, shard int, checkpoint int64) (int64, error) {
	env := c.env
	url := c.shardURL(shard)
	skip := c.st.Line

	resp, err := c.client.Get(ctx, url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	zr, err := gzip.NewReader(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("gunzip %s: %w", url, err)
	}
	defer zr.Close()

	if skip > 0 {
		env.Log.Info("resuming shard", "shard", shard, "skipping", skip)
	}

	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 0, 256<<10), 8<<20)

	now := db.Now()
	batch := make([]record.Record, 0, 1024)
	var lines, malformed int64

	// flushed tracks the last line whose row actually reached the queue. On any
	// failure the cursor rewinds to it: a partially emitted batch must never
	// look like progress, or resuming would skip rows that were never written.
	var flushed int64

	flush := func() error {
		if len(batch) == 0 {
			flushed = lines
			return nil
		}
		err := env.Emit(ctx, batch...)
		batch = batch[:0]
		if err != nil {
			return err
		}
		flushed = lines
		return nil
	}

	abort := func(cause error) (int64, error) {
		c.st.Line = flushed
		_ = env.SaveState(ctx, c.st)
		return flushed, cause
	}

	for sc.Scan() {
		lines++
		if lines <= skip {
			continue
		}
		if ctx.Err() != nil {
			return abort(ctx.Err())
		}

		entry, ok := parseCDXLine(sc.Text())
		if !ok {
			malformed++
			continue
		}
		batch = append(batch, entry.record(c.crawl, now, env.RunID))

		if len(batch) == cap(batch) {
			if err := flush(); err != nil {
				return abort(err)
			}
		}
		if lines%checkpoint == 0 {
			if err := flush(); err != nil {
				return abort(err)
			}
			c.st.Line = flushed
			if err := env.SaveState(ctx, c.st); err != nil {
				return lines, err
			}
		}
	}
	if err := sc.Err(); err != nil {
		_ = flush()
		return abort(fmt.Errorf("read %s: %w", url, err))
	}
	if err := flush(); err != nil {
		return abort(err)
	}
	if malformed > 0 {
		env.Log.Warn("skipped malformed cdx lines", "shard", shard, "count", malformed)
	}
	return lines, nil
}
