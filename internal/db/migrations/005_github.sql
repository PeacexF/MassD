CREATE TABLE IF NOT EXISTS github_repositories (
    id            INTEGER PRIMARY KEY,
    node_id       TEXT,
    full_name     TEXT NOT NULL UNIQUE,
    name          TEXT,
    owner         TEXT,
    owner_id      INTEGER,
    owner_type    TEXT,
    description   TEXT,
    fork          INTEGER,
    html_url      TEXT,
    api_url       TEXT,
    collected_at  TEXT NOT NULL,
    run_id        INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_github_repos_owner ON github_repositories(owner);

-- Append-only observations, so the dataset gains history
CREATE TABLE IF NOT EXISTS github_repository_snapshots (
    id             INTEGER PRIMARY KEY,
    repository_id  INTEGER NOT NULL,
    observed_at    TEXT NOT NULL,
    stars          INTEGER,
    forks          INTEGER,
    watchers       INTEGER,
    open_issues    INTEGER,
    size_kb        INTEGER,
    language       TEXT,
    default_branch TEXT,
    topics         TEXT,
    license        TEXT,
    archived       INTEGER,
    created_at     TEXT,
    updated_at     TEXT,
    pushed_at      TEXT,
    run_id         INTEGER NOT NULL,
    UNIQUE(repository_id, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_github_snapshots_repo ON github_repository_snapshots(repository_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_github_snapshots_stars ON github_repository_snapshots(stars DESC);

CREATE TABLE IF NOT EXISTS github_events (
    id           TEXT PRIMARY KEY,
    type         TEXT,
    actor_id     INTEGER,
    actor_login  TEXT,
    repo_id      INTEGER,
    repo_name    TEXT,
    org_login    TEXT,
    public       INTEGER,
    created_at   TEXT,
    payload      TEXT,
    collected_at TEXT NOT NULL,
    run_id       INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_github_events_created ON github_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_github_events_repo ON github_events(repo_name);
CREATE INDEX IF NOT EXISTS idx_github_events_type ON github_events(type, created_at DESC);
