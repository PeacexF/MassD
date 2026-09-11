CREATE TABLE IF NOT EXISTS rss_feeds (
    id              INTEGER PRIMARY KEY,
    url             TEXT NOT NULL UNIQUE,
    title           TEXT,
    link            TEXT,
    description     TEXT,
    language        TEXT,
    last_status     INTEGER,
    last_fetched_at TEXT,
    item_count      INTEGER NOT NULL DEFAULT 0,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rss_items (
    id           INTEGER PRIMARY KEY,
    feed         TEXT NOT NULL,
    guid         TEXT NOT NULL,
    url          TEXT,
    title        TEXT,
    author       TEXT,
    summary      TEXT,
    content      TEXT,
    categories   TEXT,
    published_at TEXT,
    updated_at   TEXT,
    collected_at TEXT NOT NULL,
    run_id       INTEGER NOT NULL,
    UNIQUE(feed, guid)
);

CREATE INDEX IF NOT EXISTS idx_rss_items_published ON rss_items(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_rss_items_url ON rss_items(url);
CREATE INDEX IF NOT EXISTS idx_rss_items_feed ON rss_items(feed, published_at DESC);
