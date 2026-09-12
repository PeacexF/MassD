package rss

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/PeacexF/MassD/internal/collect"
	"github.com/PeacexF/MassD/internal/db"
	"github.com/PeacexF/MassD/internal/fetch"
	"github.com/PeacexF/MassD/internal/source"
)

func runSource(t *testing.T, d *db.DB, cfg source.Config) collect.Result {
	t.Helper()
	res, err := collect.Run(context.Background(), d, &Source{}, collect.Options{
		Options: source.Options{BatchSize: 100, Workers: 2, Resume: true},
		HTTP:    fetch.New(fetch.Options{}),
		Config:  cfg,
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestCollectIsIncremental(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("ETag", `"v1"`)
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "application/rss+xml")
		io.WriteString(w, rss2)
	}))
	defer srv.Close()

	d, err := db.Open(filepath.Join(t.TempDir(), "t.db"), db.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err := d.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}

	cfg := source.Config{"feeds": []any{srv.URL}}

	first := runSource(t, d, cfg)
	if first.Stats.Inserted != 3 {
		t.Fatalf("first run inserted %d, want 3 (feed + 2 items)", first.Stats.Inserted)
	}

	second := runSource(t, d, cfg)
	if second.Stats.Inserted != 0 {
		t.Fatalf("304 response should insert nothing, got %d", second.Stats.Inserted)
	}
	if hits.Load() != 2 {
		t.Fatalf("server hits = %d, want 2", hits.Load())
	}

	var items int
	if err := d.QueryRow(`SELECT COUNT(*) FROM rss_items`).Scan(&items); err != nil {
		t.Fatal(err)
	}
	if items != 2 {
		t.Fatalf("rss_items = %d, want 2", items)
	}

	var title, author string
	if err := d.QueryRow(`SELECT title, author FROM rss_items WHERE guid='tag:example.com,2026:1'`).Scan(&title, &author); err != nil {
		t.Fatal(err)
	}
	if author != "Ada" {
		t.Fatalf("author = %q", author)
	}
}

func TestCollectRecordsFeedErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "gone", http.StatusGone)
	}))
	defer srv.Close()

	d, err := db.Open(filepath.Join(t.TempDir(), "t.db"), db.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err := d.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}

	res := runSource(t, d, source.Config{"feeds": []any{srv.URL}})
	if res.Status != "completed" {
		t.Fatalf("one bad feed must not fail the run, status = %s", res.Status)
	}
	var n int
	if err := d.QueryRow(`SELECT COUNT(*) FROM _errors WHERE run_id=?`, res.RunID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("_errors rows = %d, want 1", n)
	}
}
