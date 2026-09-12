package commoncrawl

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PeacexF/MassD/internal/collect"
	"github.com/PeacexF/MassD/internal/db"
	"github.com/PeacexF/MassD/internal/fetch"
	"github.com/PeacexF/MassD/internal/source"
)

const crawlID = "CC-MAIN-2026-TEST"

const pageHTML = `<!doctype html><html lang="en"><head><title>Example Domain</title>
<meta name="description" content="An example page"></head>
<body><p>Hello from WARC.</p><a href="/next">next</a><a href="https://other.example/">other</a></body></html>`

func warcRecord(uri, digest, payload string) []byte {
	httpBlock := fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: %d\r\n\r\n%s",
		len(payload), payload)
	rec := fmt.Sprintf("WARC/1.0\r\nWARC-Type: response\r\nWARC-Target-URI: %s\r\n"+
		"WARC-Date: 2026-02-02T00:00:00Z\r\nWARC-Payload-Digest: %s\r\n"+
		"Content-Type: application/http; msgtype=response\r\nContent-Length: %d\r\n\r\n%s\r\n\r\n",
		uri, digest, len(httpBlock), httpBlock)

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	io.WriteString(zw, rec)
	zw.Close()
	return buf.Bytes()
}

func gzipLines(lines []string) []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	io.WriteString(zw, strings.Join(lines, "\n")+"\n")
	zw.Close()
	return buf.Bytes()
}

type warcSlice struct {
	offset int64
	length int64
}

type fixture struct {
	srv    *httptest.Server
	slices []warcSlice
}

func newFixture(t *testing.T, entries int) *fixture {
	t.Helper()

	// One WARC record per index entry, at its own offset, with padding between
	// so that a wrong range yields garbage rather than accidentally working.
	f := &fixture{}
	var warcFile []byte
	for i := range entries {
		warcFile = append(warcFile, bytes.Repeat([]byte{0}, 64)...)
		member := warcRecord(
			fmt.Sprintf("https://example.com/page?i=%d", i),
			fmt.Sprintf("sha1:TESTDIGEST%04d", i),
			fmt.Sprintf("%s<!-- %d -->", pageHTML, i),
		)
		f.slices = append(f.slices, warcSlice{offset: int64(len(warcFile)), length: int64(len(member))})
		warcFile = append(warcFile, member...)
	}
	warcFile = append(warcFile, bytes.Repeat([]byte{0}, 64)...)

	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	f.srv = srv
	t.Cleanup(srv.Close)

	mux.HandleFunc("/collinfo.json", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]map[string]string{
			{"id": crawlID, "name": "Test Crawl", "cdx-api": srv.URL + "/" + crawlID + "-index",
				"from": "2026-02-01T00:00:00", "to": "2026-02-14T00:00:00"},
			{"id": "CC-MAIN-2025-OLD", "name": "Older Crawl"},
		})
	})

	lines := make([]string, 0, entries)
	for i := range entries {
		meta := map[string]string{
			"url":           fmt.Sprintf("https://example.com/page?i=%d", i),
			"mime":          "text/html",
			"mime-detected": "text/html",
			"status":        "200",
			"digest":        fmt.Sprintf("SHA1DIGEST%04d", i),
			"length":        fmt.Sprint(f.slices[i].length),
			"offset":        fmt.Sprint(f.slices[i].offset),
			"filename":      "crawl-data/" + crawlID + "/warc/test.warc.gz",
			"languages":     "eng",
			"charset":       "UTF-8",
		}
		blob, _ := json.Marshal(meta)
		lines = append(lines, fmt.Sprintf("com,example)/page?i=%d 2026020201234%d %s", i, i%10, blob))
	}
	// One malformed line must not stop the shard.
	lines = append(lines, "this line is not valid cdx")
	shard := gzipLines(lines)

	mux.HandleFunc("/cc-index/collections/"+crawlID+"/indexes/", func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "cdx-00000.gz") {
			http.NotFound(w, r)
			return
		}
		w.Write(shard)
	})

	mux.HandleFunc("/crawl-data/"+crawlID+"/warc/test.warc.gz", func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "test.warc.gz", time.Time{}, bytes.NewReader(warcFile))
	})
	return f
}

func testDB(t *testing.T) *db.DB {
	t.Helper()
	d, err := db.Open(filepath.Join(t.TempDir(), "t.db"), db.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	if _, err := d.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return d
}

func run(t *testing.T, d *db.DB, cfg source.Config, opts source.Options) collect.Result {
	t.Helper()
	opts.Resume = true
	if opts.BatchSize == 0 {
		opts.BatchSize = 200
	}
	if opts.Workers == 0 {
		opts.Workers = 4
	}
	res, err := collect.Run(context.Background(), d, &Source{}, collect.Options{
		Options: opts,
		HTTP:    fetch.New(fetch.Options{}),
		Config:  cfg,
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func baseConfig(f *fixture) source.Config {
	return source.Config{
		"data_url":  f.srv.URL,
		"index_url": f.srv.URL,
		"crawl":     "latest",
	}
}

func TestCollectIndexShard(t *testing.T) {
	f := newFixture(t, 250)
	d := testDB(t)

	res := run(t, d, baseConfig(f), source.Options{})
	if res.Status != "completed" {
		t.Fatalf("status = %s", res.Status)
	}

	var rows int
	d.QueryRow(`SELECT COUNT(*) FROM commoncrawl_index`).Scan(&rows)
	if rows != 250 {
		t.Fatalf("index rows = %d, want 250", rows)
	}

	var url, mime, langs, fetchedAt string
	var status, length int64
	if err := d.QueryRow(`SELECT url, mime, languages, fetched_at, status, length
	                      FROM commoncrawl_index WHERE surt='com,example)/page?i=0'`).
		Scan(&url, &mime, &langs, &fetchedAt, &status, &length); err != nil {
		t.Fatal(err)
	}
	if url != "https://example.com/page?i=0" || mime != "text/html" || langs != "eng" || status != 200 {
		t.Fatalf("row: %s %s %s %d", url, mime, langs, status)
	}
	if fetchedAt != "2026-02-02T01:23:40Z" {
		t.Fatalf("fetched_at = %q", fetchedAt)
	}

	// The newest crawl is chosen and every listed crawl is recorded.
	var crawls int
	d.QueryRow(`SELECT COUNT(*) FROM commoncrawl_crawls`).Scan(&crawls)
	if crawls != 2 {
		t.Fatalf("crawls = %d", crawls)
	}

	var shard string
	var lines int64
	if err := d.QueryRow(`SELECT shard, lines FROM commoncrawl_shards WHERE crawl=?`, crawlID).Scan(&shard, &lines); err != nil {
		t.Fatal(err)
	}
	if shard != "cdx-00000" || lines != 251 {
		t.Fatalf("shard = %s lines = %d", shard, lines)
	}

	state, _ := collect.LoadState(context.Background(), d, "commoncrawl")
	var st state2
	json.Unmarshal(state, &st)
	if st.Crawl != crawlID || st.Shard != 1 || st.Line != 0 {
		t.Fatalf("state = %+v", st)
	}
}

type state2 struct {
	Crawl       string `json:"crawl"`
	Shard       int    `json:"shard"`
	Line        int64  `json:"line"`
	PagesCursor int64  `json:"pages_cursor"`
}

func TestIndexResumesWithoutLosingRows(t *testing.T) {
	f := newFixture(t, 250)
	d := testDB(t)

	cfg := baseConfig(f)
	cfg["checkpoint_lines"] = 50

	// Stop part way through the shard, then finish it in a second run.
	first := run(t, d, cfg, source.Options{MaxRecords: 100, BatchSize: 25})
	if first.Stats.Inserted == 0 || first.Stats.Inserted > 100 {
		t.Fatalf("first run inserted %d", first.Stats.Inserted)
	}

	state, _ := collect.LoadState(context.Background(), d, "commoncrawl")
	var st state2
	json.Unmarshal(state, &st)
	if st.Line == 0 || st.Shard != 0 {
		t.Fatalf("expected a mid-shard cursor, got %+v", st)
	}
	if st.Line > first.Stats.Inserted {
		t.Fatalf("cursor at line %d claims more progress than the %d rows written",
			st.Line, first.Stats.Inserted)
	}

	run(t, d, cfg, source.Options{BatchSize: 25})

	var rows int
	d.QueryRow(`SELECT COUNT(*) FROM commoncrawl_index`).Scan(&rows)
	if rows != 250 {
		t.Fatalf("after resume rows = %d, want all 250", rows)
	}
}

func TestCollectPagesFromWARC(t *testing.T) {
	f := newFixture(t, 5)
	d := testDB(t)

	cfg := baseConfig(f)
	run(t, d, cfg, source.Options{})

	pagesCfg := baseConfig(f)
	pagesCfg["datasets"] = []any{"pages"}
	pagesCfg["store_text"] = true
	res := run(t, d, pagesCfg, source.Options{})
	if res.Status != "completed" {
		t.Fatalf("status = %s", res.Status)
	}

	var pages int
	d.QueryRow(`SELECT COUNT(*) FROM commoncrawl_pages`).Scan(&pages)
	if pages != 5 {
		t.Fatalf("pages = %d, want 5", pages)
	}

	var title, desc, lang, text, digest, ctype string
	var links, status int
	if err := d.QueryRow(`SELECT title, description, language, text, digest, content_type, link_count, status
	                      FROM commoncrawl_pages ORDER BY index_id LIMIT 1`).
		Scan(&title, &desc, &lang, &text, &digest, &ctype, &links, &status); err != nil {
		t.Fatal(err)
	}
	if title != "Example Domain" || desc != "An example page" || lang != "en" {
		t.Fatalf("metadata: %q %q %q", title, desc, lang)
	}
	if links != 2 || status != 200 || !strings.Contains(ctype, "text/html") {
		t.Fatalf("links=%d status=%d ctype=%q", links, status, ctype)
	}
	if !strings.Contains(text, "Hello from WARC.") {
		t.Fatalf("text = %q", text)
	}
	if digest != "sha1:TESTDIGEST0000" {
		t.Fatalf("digest = %q", digest)
	}

	var distinct int
	d.QueryRow(`SELECT COUNT(DISTINCT url) FROM commoncrawl_pages`).Scan(&distinct)
	if distinct != 5 {
		t.Fatalf("distinct page urls = %d, want 5", distinct)
	}

	// Already-retrieved rows are not fetched again.
	run(t, d, pagesCfg, source.Options{})
	var after int
	d.QueryRow(`SELECT COUNT(*) FROM commoncrawl_pages`).Scan(&after)
	if after != pages {
		t.Fatalf("second pages run added %d rows", after-pages)
	}
}

func TestPagesRespectTextStorageSetting(t *testing.T) {
	f := newFixture(t, 2)
	d := testDB(t)
	run(t, d, baseConfig(f), source.Options{})

	cfg := baseConfig(f)
	cfg["datasets"] = []any{"pages"}
	run(t, d, cfg, source.Options{})

	var text any
	var textLen int
	if err := d.QueryRow(`SELECT text, text_length FROM commoncrawl_pages LIMIT 1`).Scan(&text, &textLen); err != nil {
		t.Fatal(err)
	}
	if text != nil {
		t.Fatalf("text stored despite store_text=false: %v", text)
	}
	if textLen == 0 {
		t.Fatal("text_length should be recorded even when text is not stored")
	}
}

func TestParseCDXLine(t *testing.T) {
	line := `com,example)/ 20260202012345 {"url":"https://example.com/","mime":"text/html",` +
		`"status":"200","digest":"ABC","length":"1234","offset":"56","filename":"crawl-data/x.warc.gz"}`
	entry, ok := parseCDXLine(line)
	if !ok {
		t.Fatal("valid line rejected")
	}
	if entry.SURT != "com,example)/" || entry.Stamp != "20260202012345" {
		t.Fatalf("entry = %+v", entry)
	}
	if entry.Meta.URL != "https://example.com/" || entry.Meta.Offset != "56" {
		t.Fatalf("meta = %+v", entry.Meta)
	}

	for _, bad := range []string{
		"",
		"only-one-field",
		"surt stamp not-json",
		`surt stamp {"mime":"text/html"}`,
	} {
		if _, ok := parseCDXLine(bad); ok {
			t.Fatalf("accepted malformed line %q", bad)
		}
	}
}

func TestNumberAndNullConversions(t *testing.T) {
	if number("") != nil || number("abc") != nil {
		t.Fatal("unparseable numbers must be NULL")
	}
	if n, _ := number("42").(int64); n != 42 {
		t.Fatal("number should parse integers")
	}
	if null("") != nil || null("x") != "x" {
		t.Fatal("null conversion is wrong")
	}
	if stampTime("bad") != nil {
		t.Fatal("bad stamps must be NULL")
	}
}
