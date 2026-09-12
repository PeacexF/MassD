package gdelt

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PeacexF/MassD/internal/collect"
	"github.com/PeacexF/MassD/internal/db"
	"github.com/PeacexF/MassD/internal/fetch"
	"github.com/PeacexF/MassD/internal/source"
)

func eventLine(id int) string {
	cols := make([]string, evMinFields)
	for i := range cols {
		cols[i] = ""
	}
	cols[evGlobalEventID] = fmt.Sprint(id)
	cols[evDay] = "20260202"
	cols[evYear] = "2026"
	cols[evActor1Code] = "USA"
	cols[evActor1Name] = "UNITED STATES"
	cols[evEventCode] = "043"
	cols[evEventRootCode] = "04"
	cols[evQuadClass] = "1"
	cols[evGoldstein] = "1.9"
	cols[evNumMentions] = "3"
	cols[evAvgTone] = "-2.5"
	cols[evActGeoCountry] = "US"
	cols[evActGeoLat] = "38.8951"
	cols[evActGeoLong] = "-77.0364"
	cols[evDateAdded] = "20260202154500"
	cols[evSourceURL] = fmt.Sprintf("https://news.example/%d", id)
	return strings.Join(cols, "\t")
}

func mentionLine(id int) string {
	cols := make([]string, mnMinFields)
	for i := range cols {
		cols[i] = ""
	}
	cols[mnGlobalEventID] = fmt.Sprint(id)
	cols[mnMentionTime] = "20260202154500"
	cols[mnMentionType] = "1"
	cols[mnSourceName] = "news.example"
	cols[mnIdentifier] = fmt.Sprintf("https://news.example/%d", id)
	cols[mnSentenceID] = "2"
	cols[mnConfidence] = "80"
	cols[mnDocTone] = "-1.25"
	return strings.Join(cols, "\t")
}

func zipOf(t *testing.T, name string, lines []string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(w, strings.Join(lines, "\n")+"\n"); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func gdeltServer(t *testing.T, stamp string) *httptest.Server {
	t.Helper()
	var events, mentions []string
	for i := 1; i <= 50; i++ {
		events = append(events, eventLine(i))
		mentions = append(mentions, mentionLine(i))
	}
	exportZip := zipOf(t, stamp+".export.CSV", events)
	mentionsZip := zipOf(t, stamp+".mentions.CSV", mentions)

	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	mux.HandleFunc("/lastupdate.txt", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "%d abc %s/%s.export.CSV.zip\n", len(exportZip), srv.URL, stamp)
		fmt.Fprintf(w, "%d def %s/%s.mentions.CSV.zip\n", len(mentionsZip), srv.URL, stamp)
	})
	mux.HandleFunc("/"+stamp+".export.CSV.zip", func(w http.ResponseWriter, r *http.Request) {
		w.Write(exportZip)
	})
	mux.HandleFunc("/"+stamp+".mentions.CSV.zip", func(w http.ResponseWriter, r *http.Request) {
		w.Write(mentionsZip)
	})
	t.Cleanup(srv.Close)
	return srv
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

func TestCollectEventsAndMentions(t *testing.T) {
	srv := gdeltServer(t, "20260202154500")
	d := testDB(t)
	cfg := source.Config{"base_url": srv.URL}

	res := runSource(t, d, cfg)
	if res.Status != "completed" {
		t.Fatalf("status = %s", res.Status)
	}

	var events, mentions, files int
	d.QueryRow(`SELECT COUNT(*) FROM gdelt_events`).Scan(&events)
	d.QueryRow(`SELECT COUNT(*) FROM gdelt_mentions`).Scan(&mentions)
	d.QueryRow(`SELECT COUNT(*) FROM gdelt_files`).Scan(&files)
	if events != 50 || mentions != 50 || files != 2 {
		t.Fatalf("events=%d mentions=%d files=%d", events, mentions, files)
	}

	var country, addedAt, sourceURL string
	var tone float64
	if err := d.QueryRow(`SELECT action_geo_country, date_added_at, source_url, avg_tone
	                      FROM gdelt_events WHERE global_event_id=1`).Scan(&country, &addedAt, &sourceURL, &tone); err != nil {
		t.Fatal(err)
	}
	if country != "US" || tone != -2.5 || sourceURL != "https://news.example/1" {
		t.Fatalf("row: %s %s %v", country, sourceURL, tone)
	}
	if addedAt != "2026-02-02T15:45:00Z" {
		t.Fatalf("date_added_at = %q", addedAt)
	}

	// Unset GDELT columns must be NULL, not empty strings.
	var nulls int
	d.QueryRow(`SELECT COUNT(*) FROM gdelt_events WHERE actor2_code IS NULL`).Scan(&nulls)
	if nulls != 50 {
		t.Fatalf("expected empty fields to be NULL, got %d", nulls)
	}
}

func TestCollectSkipsAlreadyProcessedFiles(t *testing.T) {
	srv := gdeltServer(t, "20260202154500")
	d := testDB(t)
	cfg := source.Config{"base_url": srv.URL}

	runSource(t, d, cfg)
	second := runSource(t, d, cfg)
	if second.Stats.Fetched != 0 {
		t.Fatalf("second run fetched %d records, want 0", second.Stats.Fetched)
	}
}

func TestFilterFilesOrdersAndSelects(t *testing.T) {
	files := []fileRef{
		{URL: "b", Kind: "mentions", Stamp: "20260202150000"},
		{URL: "a", Kind: "export", Stamp: "20260202150000"},
		{URL: "c", Kind: "gkg", Stamp: "20260202150000"},
		{URL: "old", Kind: "export", Stamp: "20260101000000"},
	}
	got := filterFiles(files, []string{"export", "mentions"}, "20260201000000", 0)
	if len(got) != 2 || got[0].URL != "a" || got[1].URL != "b" {
		t.Fatalf("got %+v", got)
	}
	if len(filterFiles(files, []string{"export"}, "", 1)) != 1 {
		t.Fatal("max_files not applied")
	}
}

func TestRefForRecognisesDatasets(t *testing.T) {
	ref := refFor("http://data.gdeltproject.org/gdeltv2/20260202154500.export.CSV.zip")
	if ref.Kind != "export" || ref.Stamp != "20260202154500" {
		t.Fatalf("ref = %+v", ref)
	}
	if refFor("http://example.com/readme.txt").Kind != "" {
		t.Fatal("non-dataset files must be ignored")
	}
}

func TestShortRowsAreSkipped(t *testing.T) {
	if _, ok := eventRecord([]string{"1", "2"}, "f", "now", 1); ok {
		t.Fatal("truncated event rows must be rejected")
	}
	if _, ok := mentionRecord([]string{"1"}, "f", "now", 1); ok {
		t.Fatal("truncated mention rows must be rejected")
	}
}
