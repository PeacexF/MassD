package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// GraphQL bills points rather than requests: roughly one point per 100 nodes
// returned. Fetching repository metadata 100 nodes at a time therefore costs
// about the same as a single REST call that returns one repository.
const nodesPerQuery = 100

type gqlRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}

type gqlError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Path    []any  `json:"path"`
}

func (e gqlError) String() string {
	if len(e.Path) == 0 {
		return e.Message
	}
	return fmt.Sprintf("%s (at %v)", e.Message, e.Path)
}

type rateLimit struct {
	Cost      int       `json:"cost"`
	Remaining int       `json:"remaining"`
	Limit     int       `json:"limit"`
	ResetAt   time.Time `json:"resetAt"`
}

// graphql posts a query and decodes data into out. GitHub reports per-node
// failures as a 200 with an errors array alongside usable data, so errors are
// returned for logging without discarding the rows that did arrive.
func (c *collector) graphql(ctx context.Context, query string, vars map[string]any, out any) ([]gqlError, error) {
	body, err := json.Marshal(gqlRequest{Query: query, Variables: vars})
	if err != nil {
		return nil, err
	}

	req, err := c.client.NewRequest(ctx, http.MethodPost, c.graphqlURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var envelope struct {
		Data   json.RawMessage `json:"data"`
		Errors []gqlError      `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("decode graphql response: %w", err)
	}
	if len(envelope.Data) == 0 || string(envelope.Data) == "null" {
		if len(envelope.Errors) > 0 {
			return envelope.Errors, fmt.Errorf("graphql: %s", envelope.Errors[0].String())
		}
		return nil, fmt.Errorf("graphql: empty response")
	}
	if out != nil {
		if err := json.Unmarshal(envelope.Data, out); err != nil {
			return envelope.Errors, fmt.Errorf("decode graphql data: %w", err)
		}
	}
	return envelope.Errors, nil
}

const repoNodesQuery = `query($ids:[ID!]!){
  rateLimit{cost remaining limit resetAt}
  nodes(ids:$ids){
    ... on Repository{
      databaseId
      nameWithOwner
      stargazerCount
      forkCount
      watchers{totalCount}
      issues(states:OPEN){totalCount}
      diskUsage
      primaryLanguage{name}
      defaultBranchRef{name}
      licenseInfo{spdxId}
      isArchived
      createdAt
      updatedAt
      pushedAt%s
    }
  }
}`

// topicsFragment is opt-in: a connection with `first: n` adds n nodes per
// repository to the query cost, which at 100 repositories per query is the
// difference between 1 point and n points.
func repoQuery(topics int) string {
	if topics <= 0 {
		return fmt.Sprintf(repoNodesQuery, "")
	}
	return fmt.Sprintf(repoNodesQuery,
		fmt.Sprintf("\n      repositoryTopics(first:%d){nodes{topic{name}}}", topics))
}

type gqlRepo struct {
	DatabaseID    int64  `json:"databaseId"`
	NameWithOwner string `json:"nameWithOwner"`
	Stars         int    `json:"stargazerCount"`
	Forks         int    `json:"forkCount"`
	Watchers      struct {
		TotalCount int `json:"totalCount"`
	} `json:"watchers"`
	Issues struct {
		TotalCount int `json:"totalCount"`
	} `json:"issues"`
	DiskUsage       *int `json:"diskUsage"`
	PrimaryLanguage *struct {
		Name string `json:"name"`
	} `json:"primaryLanguage"`
	DefaultBranchRef *struct {
		Name string `json:"name"`
	} `json:"defaultBranchRef"`
	LicenseInfo *struct {
		SPDXID string `json:"spdxId"`
	} `json:"licenseInfo"`
	RepositoryTopics struct {
		Nodes []struct {
			Topic struct {
				Name string `json:"name"`
			} `json:"topic"`
		} `json:"nodes"`
	} `json:"repositoryTopics"`
	IsArchived bool   `json:"isArchived"`
	CreatedAt  string `json:"createdAt"`
	UpdatedAt  string `json:"updatedAt"`
	PushedAt   string `json:"pushedAt"`
}

func (r gqlRepo) topics() string {
	if len(r.RepositoryTopics.Nodes) == 0 {
		return ""
	}
	names := make([]string, 0, len(r.RepositoryTopics.Nodes))
	for _, n := range r.RepositoryTopics.Nodes {
		if n.Topic.Name != "" {
			names = append(names, n.Topic.Name)
		}
	}
	return strings.Join(names, ",")
}

func (r gqlRepo) language() string {
	if r.PrimaryLanguage == nil {
		return ""
	}
	return r.PrimaryLanguage.Name
}

func (r gqlRepo) branch() string {
	if r.DefaultBranchRef == nil {
		return ""
	}
	return r.DefaultBranchRef.Name
}

func (r gqlRepo) license() string {
	if r.LicenseInfo == nil {
		return ""
	}
	return r.LicenseInfo.SPDXID
}

func (r gqlRepo) diskUsage() any {
	if r.DiskUsage == nil {
		return nil
	}
	return *r.DiskUsage
}
