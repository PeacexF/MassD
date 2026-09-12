CREATE TABLE IF NOT EXISTS gdelt_events (
    global_event_id       INTEGER PRIMARY KEY,
    day                   INTEGER,
    month_year            INTEGER,
    year                  INTEGER,
    fraction_date         REAL,
    actor1_code           TEXT,
    actor1_name           TEXT,
    actor1_country        TEXT,
    actor1_known_group    TEXT,
    actor1_type1          TEXT,
    actor2_code           TEXT,
    actor2_name           TEXT,
    actor2_country        TEXT,
    actor2_known_group    TEXT,
    actor2_type1          TEXT,
    is_root_event         INTEGER,
    event_code            TEXT,
    event_base_code       TEXT,
    event_root_code       TEXT,
    quad_class            INTEGER,
    goldstein_scale       REAL,
    num_mentions          INTEGER,
    num_sources           INTEGER,
    num_articles          INTEGER,
    avg_tone              REAL,
    actor1_geo_country    TEXT,
    actor1_geo_lat        REAL,
    actor1_geo_long       REAL,
    actor2_geo_country    TEXT,
    actor2_geo_lat        REAL,
    actor2_geo_long       REAL,
    action_geo_type       INTEGER,
    action_geo_fullname   TEXT,
    action_geo_country    TEXT,
    action_geo_adm1       TEXT,
    action_geo_lat        REAL,
    action_geo_long       REAL,
    action_geo_feature_id TEXT,
    date_added            INTEGER,
    date_added_at         TEXT,
    source_url            TEXT,
    source_file           TEXT NOT NULL,
    collected_at          TEXT NOT NULL,
    run_id                INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gdelt_events_day ON gdelt_events(day);
CREATE INDEX IF NOT EXISTS idx_gdelt_events_added ON gdelt_events(date_added_at);
CREATE INDEX IF NOT EXISTS idx_gdelt_events_country ON gdelt_events(action_geo_country, day);
CREATE INDEX IF NOT EXISTS idx_gdelt_events_root ON gdelt_events(event_root_code, day);

CREATE TABLE IF NOT EXISTS gdelt_mentions (
    id                  INTEGER PRIMARY KEY,
    global_event_id     INTEGER NOT NULL,
    event_time_date     INTEGER,
    mention_time_date   INTEGER,
    mention_time_at     TEXT,
    mention_type        INTEGER,
    mention_source_name TEXT,
    mention_identifier  TEXT,
    sentence_id         INTEGER,
    in_raw_text         INTEGER,
    confidence          INTEGER,
    mention_doc_len     INTEGER,
    mention_doc_tone    REAL,
    source_file         TEXT NOT NULL,
    collected_at        TEXT NOT NULL,
    run_id              INTEGER NOT NULL,
    UNIQUE(global_event_id, mention_identifier, sentence_id)
);

CREATE INDEX IF NOT EXISTS idx_gdelt_mentions_event ON gdelt_mentions(global_event_id);
CREATE INDEX IF NOT EXISTS idx_gdelt_mentions_time ON gdelt_mentions(mention_time_at);

CREATE TABLE IF NOT EXISTS gdelt_files (
    url          TEXT PRIMARY KEY,
    kind         TEXT NOT NULL,
    stamp        TEXT NOT NULL,
    rows         INTEGER NOT NULL DEFAULT 0,
    bytes        INTEGER NOT NULL DEFAULT 0,
    collected_at TEXT NOT NULL,
    run_id       INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gdelt_files_stamp ON gdelt_files(stamp);
