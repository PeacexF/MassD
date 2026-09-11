-- Synthetic source used to exercise the pipeline end to end.
CREATE TABLE IF NOT EXISTS test_records (
    id           INTEGER PRIMARY KEY,
    seq          INTEGER NOT NULL UNIQUE,
    payload      TEXT NOT NULL,
    hash         TEXT NOT NULL,
    run_id       INTEGER NOT NULL,
    collected_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_test_records_run ON test_records(run_id);
