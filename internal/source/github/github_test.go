package github

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/PeacexF/MassD/internal/collect"
	"github.com/PeacexF/MassD/internal/db"
	"github.com/PeacexF/MassD/internal/fetch"
	"github.com/PeacexF/MassD/internal/source"
)

var graphqlCalls atomic.Int32

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

func apiServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var eventHits atomic.Int32

	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	mux.HandleFunc("/repositories", func(w http.ResponseWriter, r *http.Request) {
		since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
		var repos []map[string]any
		if since < 200 {
			for i := since + 1; i <= since+100; i++ {
				repos = append(repos, map[string]any{
					"id": i, "node_id": fmt.Sprintf("N%d", i),
					"name": fmt.Sprintf("repo%d", i), "full_name": fmt.Sprintf("owner/repo%d", i),
					"description": "a repo", "fork": i%2 == 0,
					"html_url": fmt.Sprintf("https://github.com/owner/repo%d", i),
					"url":      fmt.Sprintf("%s/repos/owner/repo%d", srv.URL, i),
					"owner":    map[string]any{"id": 7, "login": "owner", "type": "User"},
				})
			}
			w.Header().Set("Link", fmt.Sprintf(`<%s/repositories?since=%d&per_page=100>; rel="next"`, srv.URL, since+100))
		}
		w.Header().Set("X-RateLimit-Remaining", "4999")
		json.NewEncoder(w).Encode(repos)
	})

	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		eventHits.Add(1)
		w.Header().Set("ETag", `W/"events1"`)
		w.Header().Set("X-RateLimit-Remaining", "4999")
		if r.Header.Get("If-None-Match") == `W/"events1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		var events []map[string]any
		if page == 1 {
			for i := 1; i <= 30; i++ {
				events = append(events, map[string]any{
					"id": fmt.Sprintf("%d", 40000000000+i), "type": "PushEvent",
					"actor":      map[string]any{"id": 5, "login": "someone"},
					"repo":       map[string]any{"id": 9, "name": "owner/repo"},
					"org":        map[string]any{"login": "org"},
					"public":     true,
					"created_at": "2026-02-02T10:00:00Z",
					"payload":    map[string]any{"size": i},
				})
			}
		}
		json.NewEncoder(w).Encode(events)
	})

	mux.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			http.Error(w, "auth required", http.StatusUnauthorized)
			return
		}
		var req struct {
			Query     string `json:"query"`
			Variables struct {
				IDs []string `json:"ids"`
			} `json:"variables"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		graphqlCalls.Add(1)

		nodes := make([]map[string]any, 0, len(req.Variables.IDs))
		for _, id := range req.Variables.IDs {
			n, _ := strconv.ParseInt(strings.TrimPrefix(id, "N"), 10, 64)
			node := map[string]any{
				"databaseId": n, "nameWithOwner": fmt.Sprintf("owner/repo%d", n),
				"stargazerCount": 1234, "forkCount": 12,
				"watchers":         map[string]any{"totalCount": 3},
				"issues":           map[string]any{"totalCount": 4},
				"diskUsage":        900,
				"primaryLanguage":  map[string]any{"name": "Go"},
				"defaultBranchRef": map[string]any{"name": "main"},
				"licenseInfo":      map[string]any{"spdxId": "MIT"},
				"isArchived":       false,
				"createdAt":        "2020-01-01T00:00:00Z",
				"updatedAt":        "2026-01-01T00:00:00Z",
				"pushedAt":         "2026-02-01T00:00:00Z",
			}
			if strings.Contains(req.Query, "repositoryTopics") {
				node["repositoryTopics"] = map[string]any{"nodes": []any{
					map[string]any{"topic": map[string]any{"name": "data"}},
					map[string]any{"topic": map[string]any{"name": "sqlite"}},
				}}
			}
			nodes = append(nodes, node)
		}
		json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
			"rateLimit": map[string]any{"cost": 1, "remaining": 4999, "limit": 5000,
				"resetAt": "2026-02-02T12:00:00Z"},
			"nodes": nodes,
		}})
	})
	return srv, &eventHits
}

func TestCollectRepositoriesPaginates(t *testing.T) {
	srv, _ := apiServer(t)
	d := testDB(t)

	res := runSource(t, d, source.Config{
		"api_url": srv.URL, "datasets": []any{"repositories"}, "rate_limit": 0,
	})
	if res.Status != "completed" {
		t.Fatalf("status = %s", res.Status)
	}

	var n int
	d.QueryRow(`SELECT COUNT(*) FROM github_repositories`).Scan(&n)
	if n != 200 {
		t.Fatalf("repositories = %d, want 200", n)
	}

	var full, owner string
	var fork int
	if err := d.QueryRow(`SELECT full_name, owner, fork FROM github_repositories WHERE id=2`).Scan(&full, &owner, &fork); err != nil {
		t.Fatal(err)
	}
	if full != "owner/repo2" || owner != "owner" || fork != 1 {
		t.Fatalf("row: %s %s %d", full, owner, fork)
	}

	state, err := collect.LoadState(context.Background(), d, "github")
	if err != nil {
		t.Fatal(err)
	}
	var st state2
	json.Unmarshal(state, &st)
	if st.RepositoriesSince != 200 {
		t.Fatalf("cursor = %d, want 200", st.RepositoriesSince)
	}
}

type state2 struct {
	RepositoriesSince int64  `json:"repositories_since"`
	EventsETag        string `json:"events_etag"`
}

func TestCollectEventsUsesETag(t *testing.T) {
	srv, hits := apiServer(t)
	d := testDB(t)
	cfg := source.Config{"api_url": srv.URL, "datasets": []any{"events"}, "event_pages": 1}

	first := runSource(t, d, cfg)
	if first.Stats.Inserted != 30 {
		t.Fatalf("inserted = %d, want 30", first.Stats.Inserted)
	}

	second := runSource(t, d, cfg)
	if second.Stats.Inserted != 0 {
		t.Fatalf("second run inserted %d, want 0", second.Stats.Inserted)
	}
	if hits.Load() != 2 {
		t.Fatalf("event requests = %d, want 2", hits.Load())
	}

	var payload, created string
	if err := d.QueryRow(`SELECT payload, created_at FROM github_events ORDER BY id LIMIT 1`).Scan(&payload, &created); err != nil {
		t.Fatal(err)
	}
	if payload == "" || created != "2026-02-02T10:00:00Z" {
		t.Fatalf("payload=%q created=%q", payload, created)
	}
}

func TestRepoDetailsBatchesOverGraphQL(t *testing.T) {
	srv, _ := apiServer(t)
	d := testDB(t)
	graphqlCalls.Store(0)

	runSource(t, d, source.Config{"api_url": srv.URL, "datasets": []any{"repositories"}, "max_pages": 1})
	res := runSource(t, d, source.Config{
		"api_url": srv.URL, "graphql_url": srv.URL + "/graphql", "token": "test-token",
		"datasets": []any{"repo_details"}, "detail_batch": 250, "topics": 5,
	})
	if res.Status != "completed" {
		t.Fatalf("status = %s", res.Status)
	}

	var snapshots, stars int
	d.QueryRow(`SELECT COUNT(*) FROM github_repository_snapshots`).Scan(&snapshots)
	d.QueryRow(`SELECT stars FROM github_repository_snapshots WHERE repository_id=1`).Scan(&stars)
	if snapshots != 100 || stars != 1234 {
		t.Fatalf("snapshots=%d stars=%d", snapshots, stars)
	}
	// 100 repositories must cost one query, not one query per repository.
	if n := graphqlCalls.Load(); n != 1 {
		t.Fatalf("graphql calls = %d, want 1 for 100 repositories", n)
	}

	var topics, license, branch string
	d.QueryRow(`SELECT topics, license, default_branch FROM github_repository_snapshots LIMIT 1`).Scan(&topics, &license, &branch)
	if topics != "data,sqlite" || license != "MIT" || branch != "main" {
		t.Fatalf("topics=%q license=%q branch=%q", topics, license, branch)
	}

	// Repositories snapshotted this run are not re-fetched by the next one.
	graphqlCalls.Store(0)
	again := runSource(t, d, source.Config{
		"api_url": srv.URL, "graphql_url": srv.URL + "/graphql", "token": "test-token",
		"datasets": []any{"repo_details"}, "detail_batch": 250, "refresh_after": "168h",
	})
	if again.Stats.Inserted != 0 || graphqlCalls.Load() != 0 {
		t.Fatalf("fresh snapshots were refetched: inserted=%d calls=%d", again.Stats.Inserted, graphqlCalls.Load())
	}
}

func TestRepoDetailsRequiresToken(t *testing.T) {
	srv, _ := apiServer(t)
	d := testDB(t)

	runSource(t, d, source.Config{"api_url": srv.URL, "datasets": []any{"repositories"}, "max_pages": 1})
	res := runSource(t, d, source.Config{
		"api_url": srv.URL, "graphql_url": srv.URL + "/graphql",
		"datasets": []any{"repo_details"},
	})
	var n int
	d.QueryRow(`SELECT COUNT(*) FROM _errors WHERE run_id=? AND error LIKE '%token%'`, res.RunID).Scan(&n)
	if n != 1 {
		t.Fatalf("expected a recorded token error, got %d", n)
	}
}

func TestChunkTargets(t *testing.T) {
	targets := make([]detailTarget, 250)
	chunks := chunkTargets(targets, nodesPerQuery)
	if len(chunks) != 3 || len(chunks[0]) != 100 || len(chunks[2]) != 50 {
		t.Fatalf("chunks = %d sizes %d/%d", len(chunks), len(chunks[0]), len(chunks[len(chunks)-1]))
	}
}
