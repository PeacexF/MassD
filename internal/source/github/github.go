// Package github collects public GitHub metadata. It starts with the two
// endpoints that give the most records per request: the public repository
// enumeration and the public event stream.
package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PeacexF/MassD/internal/db"
	"github.com/PeacexF/MassD/internal/fetch"
	"github.com/PeacexF/MassD/internal/record"
	"github.com/PeacexF/MassD/internal/source"
)

const defaultAPI = "https://api.github.com"

type Source struct{}

func init() { source.Register(&Source{}) }

func (s *Source) Name() string { return "github" }

func (s *Source) Describe() string {
	return "public repositories, repository snapshots and the public event stream"
}

type state struct {
	RepositoriesSince int64  `json:"repositories_since"`
	EventsETag        string `json:"events_etag,omitempty"`
	DetailsCursor     int64  `json:"details_cursor,omitempty"`
}

func (s *Source) Collect(ctx context.Context, env *source.Environment) error {
	datasets := env.Config.Strings("datasets")
	if len(datasets) == 0 {
		datasets = []string{"repositories", "events"}
	}

	api := strings.TrimSuffix(env.Config.String("api_url", defaultAPI), "/")
	token := env.Config.String("token", "")

	client := env.HTTP.WithOverrides(func(o *fetch.Options) {
		if o.Headers == nil {
			o.Headers = map[string]string{}
		}
		o.Headers["Accept"] = "application/vnd.github+json"
		o.Headers["X-GitHub-Api-Version"] = "2022-11-28"
		if token != "" {
			o.Headers["Authorization"] = "Bearer " + token
		}
		o.MaxBodyBytes = int64(env.Config.Int("max_response_bytes", 64<<20))
		// Unauthenticated access is 60 requests/hour, so stay gentle by default.
		if o.RateLimit <= 0 {
			o.RateLimit = 10
		}
	})
	if token == "" {
		env.Log.Warn("no github token configured; unauthenticated limits are 60 requests/hour")
	}

	var st state
	if err := env.LoadState(&st); err != nil {
		return err
	}

	c := &collector{
		env:        env,
		client:     client,
		api:        api,
		graphqlURL: env.Config.String("graphql_url", strings.TrimSuffix(api, "/api/v3")+"/graphql"),
		token:      token,
		st:         &st,
		maxWait:    env.Config.Duration("rate_limit_max_wait", 15*time.Minute),
	}

	for _, ds := range datasets {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var err error
		switch strings.ToLower(strings.TrimSpace(ds)) {
		case "repositories":
			err = c.repositories(ctx)
		case "events":
			err = c.events(ctx)
		case "repo_details", "snapshots":
			err = c.repoDetails(ctx)
		default:
			err = fmt.Errorf("unknown github dataset %q", ds)
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

type collector struct {
	env        *source.Environment
	client     *fetch.Client
	api        string
	graphqlURL string
	token      string
	st         *state
	maxWait    time.Duration
}

// get decodes a JSON response and returns the response so callers can read
// pagination and rate-limit headers.
func (c *collector) get(ctx context.Context, url string, headers map[string]string, v any) (*http.Response, error) {
	req, err := c.client.NewRequest(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	for k, val := range headers {
		req.Header.Set(k, val)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified || v == nil {
		return resp, nil
	}
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		return resp, fmt.Errorf("decode %s: %w", url, err)
	}
	return resp, nil
}

// respectRateLimit pauses when the quota is exhausted rather than burning
// retries against a limit that only time can clear.
func (c *collector) respectRateLimit(ctx context.Context, h http.Header) error {
	if h.Get("X-RateLimit-Remaining") != "0" {
		return nil
	}
	reset, err := strconv.ParseInt(h.Get("X-RateLimit-Reset"), 10, 64)
	if err != nil {
		return nil
	}
	wait := time.Until(time.Unix(reset, 0)) + time.Second
	if wait <= 0 {
		return nil
	}
	if wait > c.maxWait {
		return fmt.Errorf("github rate limit resets in %s, longer than the %s budget", wait.Truncate(time.Second), c.maxWait)
	}
	c.env.Log.Info("github rate limit reached, waiting", "for", wait.Truncate(time.Second))

	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type repo struct {
	ID          int64  `json:"id"`
	NodeID      string `json:"node_id"`
	Name        string `json:"name"`
	FullName    string `json:"full_name"`
	Description string `json:"description"`
	Fork        bool   `json:"fork"`
	HTMLURL     string `json:"html_url"`
	URL         string `json:"url"`
	Owner       struct {
		ID    int64  `json:"id"`
		Login string `json:"login"`
		Type  string `json:"type"`
	} `json:"owner"`
}

func (c *collector) repositories(ctx context.Context) error {
	env := c.env
	maxPages := env.Config.Int("max_pages", 0)
	next := fmt.Sprintf("%s/repositories?since=%d&per_page=100", c.api, c.st.RepositoriesSince)

	for page := 0; next != "" && (maxPages == 0 || page < maxPages); page++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		var repos []repo
		resp, err := c.get(ctx, next, nil, &repos)
		if err != nil {
			return err
		}
		if len(repos) == 0 {
			env.Log.Info("repository enumeration caught up", "since", c.st.RepositoriesSince)
			return nil
		}

		now := db.Now()
		batch := make([]record.Record, 0, len(repos))
		for _, r := range repos {
			batch = append(batch, repoRecord(r, now, env.RunID))
			if r.ID > c.st.RepositoriesSince {
				c.st.RepositoriesSince = r.ID
			}
		}
		if err := env.Emit(ctx, batch...); err != nil {
			return err
		}
		if err := env.SaveState(ctx, c.st); err != nil {
			return err
		}

		next = fetch.ParseLinks(resp.Header.Get("Link"))["next"]
		if err := c.respectRateLimit(ctx, resp.Header); err != nil {
			return err
		}
	}
	return nil
}

func repoRecord(r repo, now string, runID int64) record.Record {
	return record.New("github_repositories").
		Set("id", r.ID).
		Set("node_id", null(r.NodeID)).
		Set("full_name", r.FullName).
		Set("name", null(r.Name)).
		Set("owner", null(r.Owner.Login)).
		Set("owner_id", r.Owner.ID).
		Set("owner_type", null(r.Owner.Type)).
		Set("description", null(r.Description)).
		Set("fork", boolInt(r.Fork)).
		Set("html_url", null(r.HTMLURL)).
		Set("api_url", null(r.URL)).
		Set("collected_at", now).
		Set("run_id", runID).
		OnConflict(record.Ignore)
}

type event struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	Actor struct {
		ID    int64  `json:"id"`
		Login string `json:"login"`
	} `json:"actor"`
	Repo struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	} `json:"repo"`
	Org struct {
		Login string `json:"login"`
	} `json:"org"`
	Public    bool            `json:"public"`
	CreatedAt string          `json:"created_at"`
	Payload   json.RawMessage `json:"payload"`
}

func (c *collector) events(ctx context.Context) error {
	env := c.env
	storePayload := env.Config.Bool("store_payload", true)
	pages := env.Config.Int("event_pages", 3)

	headers := map[string]string{}
	if c.st.EventsETag != "" {
		headers["If-None-Match"] = c.st.EventsETag
	}

	for page := 1; page <= pages; page++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		url := fmt.Sprintf("%s/events?per_page=100&page=%d", c.api, page)
		var events []event
		reqHeaders := headers
		if page > 1 {
			reqHeaders = nil
		}
		resp, err := c.get(ctx, url, reqHeaders, &events)
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusNotModified {
			env.Log.Debug("event stream unchanged")
			return nil
		}
		if page == 1 {
			c.st.EventsETag = resp.Header.Get("ETag")
		}
		if len(events) == 0 {
			break
		}

		now := db.Now()
		batch := make([]record.Record, 0, len(events))
		for _, e := range events {
			if e.ID == "" {
				continue
			}
			r := record.New("github_events").
				Set("id", e.ID).
				Set("type", null(e.Type)).
				Set("actor_id", e.Actor.ID).
				Set("actor_login", null(e.Actor.Login)).
				Set("repo_id", e.Repo.ID).
				Set("repo_name", null(e.Repo.Name)).
				Set("org_login", null(e.Org.Login)).
				Set("public", boolInt(e.Public)).
				Set("created_at", null(e.CreatedAt))
			if storePayload && len(e.Payload) > 0 {
				r = r.Set("payload", string(e.Payload))
			} else {
				r = r.Set("payload", nil)
			}
			batch = append(batch, r.
				Set("collected_at", now).
				Set("run_id", env.RunID).
				OnConflict(record.Ignore))
		}

		if err := env.Emit(ctx, batch...); err != nil {
			return err
		}
		if err := env.SaveState(ctx, c.st); err != nil {
			return err
		}
		if err := c.respectRateLimit(ctx, resp.Header); err != nil {
			return err
		}
	}
	return nil
}

type detailTarget struct {
	id     int64
	nodeID string
}

// repoDetails turns previously enumerated repositories into point-in-time
// snapshots. One GraphQL query covers 100 repositories for roughly the cost of
// a single REST call, so this is the difference between thousands and hundreds
// of thousands of snapshots per hour.
func (c *collector) repoDetails(ctx context.Context) error {
	env := c.env
	if c.token == "" {
		return errors.New("repo_details needs a token: the GraphQL API rejects anonymous requests")
	}

	batchSize := env.Config.Int("detail_batch", 1000)
	topics := env.Config.Int("topics", 0)
	refresh := env.Config.Duration("refresh_after", 7*24*time.Hour)
	workers := min(max(env.Config.Int("detail_workers", env.Opts.Workers), 1), 16)

	targets, err := c.detailTargets(ctx, batchSize, refresh)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		// Nothing left in this pass; restart the sweep so snapshots keep
		// accumulating over time.
		c.st.DetailsCursor = 0
		return env.SaveState(ctx, c.st)
	}

	chunks := chunkTargets(targets, nodesPerQuery)
	query := repoQuery(topics)
	results := make([][]gqlRepo, len(chunks))
	failures := make([]error, len(chunks))

	var (
		next    atomic.Int64
		limitMu sync.Mutex
		limit   rateLimit
		wg      sync.WaitGroup
	)
	for range min(workers, len(chunks)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= len(chunks) || ctx.Err() != nil {
					return
				}

				ids := make([]string, len(chunks[i]))
				for j, t := range chunks[i] {
					ids[j] = t.nodeID
				}

				var data struct {
					RateLimit rateLimit `json:"rateLimit"`
					Nodes     []gqlRepo `json:"nodes"`
				}
				gqlErrs, err := c.graphql(ctx, query, map[string]any{"ids": ids}, &data)
				for _, ge := range gqlErrs {
					env.Log.Debug("graphql node error", "error", ge.String())
				}
				if err != nil {
					failures[i] = err
					return
				}
				results[i] = data.Nodes

				limitMu.Lock()
				limit = data.RateLimit
				limitMu.Unlock()
			}
		}()
	}
	wg.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}

	now := db.Now()
	advanceTo := int64(0)
	contiguous := true
	for i, chunk := range chunks {
		if failures[i] != nil {
			if contiguous {
				contiguous = false
			}
			env.Error(ctx, failures[i], "repo details graphql")
			continue
		}

		batch := make([]record.Record, 0, len(results[i]))
		for j, node := range results[i] {
			id := node.DatabaseID
			if id == 0 && j < len(chunk) {
				id = chunk[j].id
			}
			if id == 0 || node.NameWithOwner == "" {
				continue
			}
			batch = append(batch, snapshotRecord(id, node, now, env.RunID))
		}
		if err := env.Emit(ctx, batch...); err != nil {
			return err
		}
		if contiguous {
			advanceTo = chunk[len(chunk)-1].id
		}
	}

	if advanceTo > c.st.DetailsCursor {
		c.st.DetailsCursor = advanceTo
	}
	if err := env.SaveState(ctx, c.st); err != nil {
		return err
	}

	limitMu.Lock()
	defer limitMu.Unlock()
	if limit.Limit > 0 {
		env.Log.Info("graphql quota", "cost", limit.Cost, "remaining", limit.Remaining,
			"limit", limit.Limit, "resets", limit.ResetAt.Format(time.RFC3339))
		if limit.Remaining == 0 {
			return fmt.Errorf("graphql quota exhausted until %s", limit.ResetAt.Format(time.RFC3339))
		}
	}
	return nil
}

// detailTargets walks repositories by id and skips any that already have a
// recent snapshot. The EXISTS clause is an index seek, not a scan.
func (c *collector) detailTargets(ctx context.Context, limit int, refresh time.Duration) ([]detailTarget, error) {
	query := `SELECT id, node_id FROM github_repositories
	          WHERE id > ? AND node_id IS NOT NULL AND node_id != ''
	          ORDER BY id LIMIT ?`
	args := []any{c.st.DetailsCursor, limit}

	if refresh > 0 {
		query = `SELECT id, node_id FROM github_repositories r
		         WHERE r.id > ? AND r.node_id IS NOT NULL AND r.node_id != ''
		           AND NOT EXISTS (
		             SELECT 1 FROM github_repository_snapshots s
		             WHERE s.repository_id = r.id AND s.observed_at > ?)
		         ORDER BY r.id LIMIT ?`
		cutoff := time.Now().UTC().Add(-refresh).Format(time.RFC3339Nano)
		args = []any{c.st.DetailsCursor, cutoff, limit}
	}

	rows, err := c.env.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []detailTarget
	for rows.Next() {
		var t detailTarget
		if err := rows.Scan(&t.id, &t.nodeID); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func chunkTargets(targets []detailTarget, size int) [][]detailTarget {
	var out [][]detailTarget
	for start := 0; start < len(targets); start += size {
		out = append(out, targets[start:min(start+size, len(targets))])
	}
	return out
}

func snapshotRecord(id int64, node gqlRepo, now string, runID int64) record.Record {
	return record.New("github_repository_snapshots").
		Set("repository_id", id).
		Set("observed_at", now).
		Set("stars", node.Stars).
		Set("forks", node.Forks).
		Set("watchers", node.Watchers.TotalCount).
		Set("open_issues", node.Issues.TotalCount).
		Set("size_kb", node.diskUsage()).
		Set("language", null(node.language())).
		Set("default_branch", null(node.branch())).
		Set("topics", null(node.topics())).
		Set("license", null(node.license())).
		Set("archived", boolInt(node.IsArchived)).
		Set("created_at", null(node.CreatedAt)).
		Set("updated_at", null(node.UpdatedAt)).
		Set("pushed_at", null(node.PushedAt)).
		Set("run_id", runID).
		OnConflict(record.Ignore)
}

func null(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
