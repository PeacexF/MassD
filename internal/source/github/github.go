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

	c := &collector{env: env, client: client, api: api, st: &st,
		maxWait: env.Config.Duration("rate_limit_max_wait", 15*time.Minute)}

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
	env     *source.Environment
	client  *fetch.Client
	api     string
	st      *state
	maxWait time.Duration
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

type repoDetail struct {
	repo
	Stargazers    int      `json:"stargazers_count"`
	Forks         int      `json:"forks_count"`
	Watchers      int      `json:"subscribers_count"`
	OpenIssues    int      `json:"open_issues_count"`
	Size          int      `json:"size"`
	Language      string   `json:"language"`
	DefaultBranch string   `json:"default_branch"`
	Topics        []string `json:"topics"`
	Archived      bool     `json:"archived"`
	CreatedAt     string   `json:"created_at"`
	UpdatedAt     string   `json:"updated_at"`
	PushedAt      string   `json:"pushed_at"`
	License       struct {
		SPDX string `json:"spdx_id"`
	} `json:"license"`
}

// repoDetails turns previously enumerated repositories into point-in-time
// snapshots, one request per repository.
func (c *collector) repoDetails(ctx context.Context) error {
	env := c.env
	limit := env.Config.Int("detail_batch", 200)

	rows, err := env.DB.QueryContext(ctx,
		`SELECT id, full_name FROM github_repositories WHERE id > ? ORDER BY id LIMIT ?`,
		c.st.DetailsCursor, limit)
	if err != nil {
		return err
	}
	type target struct {
		id       int64
		fullName string
	}
	var targets []target
	for rows.Next() {
		var t target
		if err := rows.Scan(&t.id, &t.fullName); err != nil {
			rows.Close()
			return err
		}
		targets = append(targets, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	if len(targets) == 0 {
		// Wrap around so repeated runs keep refreshing existing repositories.
		c.st.DetailsCursor = 0
		return env.SaveState(ctx, c.st)
	}

	for _, t := range targets {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		var d repoDetail
		resp, err := c.get(ctx, c.api+"/repos/"+t.fullName, nil, &d)
		if err != nil {
			var httpErr *fetch.HTTPError
			if errors.As(err, &httpErr) && (httpErr.StatusCode == http.StatusNotFound || httpErr.StatusCode == http.StatusForbidden) {
				c.st.DetailsCursor = t.id
				env.Error(ctx, err, "repo details "+t.fullName)
				continue
			}
			return err
		}

		now := db.Now()
		rec := record.New("github_repository_snapshots").
			Set("repository_id", t.id).
			Set("observed_at", now).
			Set("stars", d.Stargazers).
			Set("forks", d.Forks).
			Set("watchers", d.Watchers).
			Set("open_issues", d.OpenIssues).
			Set("size_kb", d.Size).
			Set("language", null(d.Language)).
			Set("default_branch", null(d.DefaultBranch)).
			Set("topics", null(strings.Join(d.Topics, ","))).
			Set("license", null(d.License.SPDX)).
			Set("archived", boolInt(d.Archived)).
			Set("created_at", null(d.CreatedAt)).
			Set("updated_at", null(d.UpdatedAt)).
			Set("pushed_at", null(d.PushedAt)).
			Set("run_id", env.RunID).
			OnConflict(record.Ignore)

		if err := env.Emit(ctx, rec); err != nil {
			return err
		}
		c.st.DetailsCursor = t.id
		if err := env.SaveState(ctx, c.st); err != nil {
			return err
		}
		if err := c.respectRateLimit(ctx, resp.Header); err != nil {
			return err
		}
	}
	return nil
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
