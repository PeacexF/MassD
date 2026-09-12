package commoncrawl

import (
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/PeacexF/MassD/internal/db"
	"github.com/PeacexF/MassD/internal/htmlx"
	"github.com/PeacexF/MassD/internal/record"
	"github.com/PeacexF/MassD/internal/warc"
)

type pageTarget struct {
	indexID  int64
	url      string
	crawl    string
	stamp    string
	filename string
	offset   int64
	length   int64
	digest   string
}

// collectPages retrieves documents that the index already knows about. Each one
// is a single ranged request against a WARC file, so throughput is bounded by
// request concurrency rather than by Common Crawl's size.
func (c *collector) collectPages(ctx context.Context) error {
	env := c.env
	batchSize := env.Config.Int("page_batch", 500)
	workers := min(max(env.Config.Int("page_workers", env.Opts.Workers), 1), 32)
	storeText := env.Config.Bool("store_text", false)
	textLimit := env.Config.Int("text_limit", 4096)
	collectLinks := env.Config.Bool("collect_links", true)

	targets, err := c.pageTargets(ctx, batchSize)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		env.Log.Info("no index rows left to fetch", "cursor", c.st.PagesCursor)
		c.st.PagesCursor = 0
		return env.SaveState(ctx, c.st)
	}

	results := make([]record.Record, len(targets))
	failures := make([]error, len(targets))

	var (
		next atomic.Int64
		wg   sync.WaitGroup
	)
	for range min(workers, len(targets)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= len(targets) || ctx.Err() != nil {
					return
				}
				rec, err := c.fetchPage(ctx, targets[i], storeText, textLimit, collectLinks)
				if err != nil {
					failures[i] = err
					continue
				}
				results[i] = rec
			}
		}()
	}
	wg.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}

	advanceTo := c.st.PagesCursor
	contiguous := true
	batch := make([]record.Record, 0, len(targets))
	for i, t := range targets {
		if failures[i] != nil {
			contiguous = false
			env.Error(ctx, failures[i], "warc "+t.url)
			continue
		}
		if results[i].Table != "" {
			batch = append(batch, results[i])
		}
		if contiguous {
			advanceTo = t.indexID
		}
	}

	if err := env.Emit(ctx, batch...); err != nil {
		return err
	}
	c.st.PagesCursor = advanceTo
	return env.SaveState(ctx, c.st)
}

// pageTargets walks the index by id, skipping rows already retrieved.
func (c *collector) pageTargets(ctx context.Context, limit int) ([]pageTarget, error) {
	mime := c.env.Config.String("page_mime", "text/html")
	rows, err := c.env.DB.QueryContext(ctx, `
		SELECT i.id, i.url, i.crawl, i.stamp, i.filename, i.offset, i.length, COALESCE(i.digest,'')
		FROM commoncrawl_index i
		WHERE i.id > ? AND i.status = 200 AND i.filename IS NOT NULL
		  AND i.length IS NOT NULL AND i.offset IS NOT NULL
		  AND (? = '' OR i.mime LIKE ? || '%')
		  AND NOT EXISTS (SELECT 1 FROM commoncrawl_pages p WHERE p.index_id = i.id)
		ORDER BY i.id LIMIT ?`,
		c.st.PagesCursor, mime, mime, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []pageTarget
	for rows.Next() {
		var t pageTarget
		if err := rows.Scan(&t.indexID, &t.url, &t.crawl, &t.stamp, &t.filename, &t.offset, &t.length, &t.digest); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (c *collector) fetchPage(ctx context.Context, t pageTarget, storeText bool, textLimit int, collectLinks bool) (record.Record, error) {
	warcURL := c.dataBase + "/" + t.filename
	req, err := c.client.NewRequest(ctx, http.MethodGet, warcURL, nil)
	if err != nil {
		return record.Record{}, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", t.offset, t.offset+t.length-1))

	resp, err := c.client.Do(req)
	if err != nil {
		return record.Record{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		return record.Record{}, fmt.Errorf("%s: expected 206, got %s", warcURL, resp.Status)
	}

	zr, err := gzip.NewReader(resp.Body)
	if err != nil {
		return record.Record{}, fmt.Errorf("gunzip warc record: %w", err)
	}
	defer zr.Close()

	rec, err := warc.NewReader(zr).Next()
	if err != nil {
		return record.Record{}, fmt.Errorf("read warc record: %w", err)
	}
	if rec.Type() != warc.TypeResponse {
		return record.Record{}, fmt.Errorf("warc record is %q, not a response", rec.Type())
	}

	httpResp, err := rec.HTTPResponse()
	if err != nil {
		return record.Record{}, fmt.Errorf("parse http response: %w", err)
	}
	defer httpResp.Body.Close()

	base, _ := url.Parse(t.url)
	doc, err := htmlx.Extract(httpResp.Body, htmlx.Options{
		Base:         base,
		CollectLinks: collectLinks,
		MaxTextBytes: textLimit,
	})
	if err != nil && doc == nil {
		return record.Record{}, fmt.Errorf("extract html: %w", err)
	}

	text := any(nil)
	if storeText && doc.Text != "" {
		text = doc.Text
	}

	return record.New("commoncrawl_pages").
		Set("index_id", t.indexID).
		Set("crawl", null(t.crawl)).
		Set("url", firstNonEmpty(rec.TargetURI(), t.url)).
		Set("stamp", null(t.stamp)).
		Set("status", httpResp.StatusCode).
		Set("content_type", null(httpResp.Header.Get("Content-Type"))).
		Set("content_length", contentLength(httpResp)).
		Set("title", null(doc.Title)).
		Set("description", null(doc.Description)).
		Set("language", null(doc.Language)).
		Set("canonical_url", null(doc.Canonical)).
		Set("link_count", len(doc.Links)).
		Set("text_length", doc.TextLength).
		Set("text", text).
		Set("digest", null(firstNonEmpty(rec.Digest(), t.digest))).
		Set("warc_filename", t.filename).
		Set("warc_offset", t.offset).
		Set("collected_at", db.Now()).
		Set("run_id", c.env.RunID).
		OnConflict(record.Ignore), nil
}

func contentLength(resp *http.Response) any {
	if resp.ContentLength < 0 {
		return nil
	}
	return resp.ContentLength
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
