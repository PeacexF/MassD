-- System metadata. Internal tables are prefixed with "_" so that source tables
-- stay easy to spot in a plain `sqlite3 massive.db` session.

CREATE TABLE IF NOT EXISTS _migrations (
    version    INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    applied_at TEXT NOT NULL
);

-- One row per collection execution.
CREATE TABLE IF NOT EXISTS _runs (
    id          INTEGER PRIMARY KEY,
    source      TEXT NOT NULL,
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    status      TEXT NOT NULL,          -- running | completed | failed | cancelled
    records     INTEGER NOT NULL DEFAULT 0,
    errors      INTEGER NOT NULL DEFAULT 0,
    duplicates  INTEGER NOT NULL DEFAULT 0,
    requests    INTEGER NOT NULL DEFAULT 0,
    bytes       INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_runs_source_started ON _runs(source, started_at DESC);

-- Resumable, source-defined collection state (JSON).
CREATE TABLE IF NOT EXISTS _source_state (
    source     TEXT PRIMARY KEY,
    state      TEXT,
    updated_at TEXT NOT NULL
);

-- Failures recorded without aborting the whole run.
CREATE TABLE IF NOT EXISTS _errors (
    id          INTEGER PRIMARY KEY,
    run_id      INTEGER NOT NULL,
    source      TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    error       TEXT NOT NULL,
    context     TEXT
);

CREATE INDEX IF NOT EXISTS idx_errors_run ON _errors(run_id);
CREATE INDEX IF NOT EXISTS idx_errors_source_time ON _errors(source, occurred_at DESC);
