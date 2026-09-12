CREATE TABLE IF NOT EXISTS commoncrawl_crawls (
    id           TEXT PRIMARY KEY,
    name         TEXT,
    cdx_api      TEXT,
    from_at      TEXT,
    to_at        TEXT,
    collected_at TEXT NOT NULL
);

-- One row per captured URL. This is the high-volume table: a single crawl
-- contains a few billion of these, and each one costs almost no parsing.
CREATE TABLE IF NOT EXISTS commoncrawl_index (
    id            INTEGER PRIMARY KEY,
    crawl         TEXT NOT NULL,
    surt          TEXT NOT NULL,
    url           TEXT NOT NULL,
    stamp         TEXT NOT NULL,
    fetched_at    TEXT,
    status        INTEGER,
    mime          TEXT,
    mime_detected TEXT,
    charset       TEXT,
    languages     TEXT,
    digest        TEXT,
    length        INTEGER,
    offset        INTEGER,
    filename      TEXT,
    collected_at  TEXT NOT NULL,
    run_id        INTEGER NOT NULL,
    UNIQUE(crawl, surt, stamp)
);

CREATE INDEX IF NOT EXISTS idx_cc_index_url ON commoncrawl_index(url);
CREATE INDEX IF NOT EXISTS idx_cc_index_digest ON commoncrawl_index(digest);
CREATE INDEX IF NOT EXISTS idx_cc_index_status ON commoncrawl_index(status, mime);

-- Shard-level progress, so an interrupted ingestion resumes at the right file.
CREATE TABLE IF NOT EXISTS commoncrawl_shards (
    crawl        TEXT NOT NULL,
    shard        TEXT NOT NULL,
    lines        INTEGER NOT NULL DEFAULT 0,
    bytes        INTEGER NOT NULL DEFAULT 0,
    completed_at TEXT,
    run_id       INTEGER NOT NULL,
    PRIMARY KEY (crawl, shard)
);

-- Pages fetched from WARC by ranged request, one row per retrieved document.
CREATE TABLE IF NOT EXISTS commoncrawl_pages (
    id             INTEGER PRIMARY KEY,
    index_id       INTEGER,
    crawl          TEXT,
    url            TEXT NOT NULL,
    stamp          TEXT,
    status         INTEGER,
    content_type   TEXT,
    content_length INTEGER,
    title          TEXT,
    description    TEXT,
    language       TEXT,
    canonical_url  TEXT,
    link_count     INTEGER,
    text_length    INTEGER,
    text           TEXT,
    digest         TEXT,
    warc_filename  TEXT,
    warc_offset    INTEGER,
    collected_at   TEXT NOT NULL,
    run_id         INTEGER NOT NULL,
    UNIQUE(url, digest)
);

CREATE INDEX IF NOT EXISTS idx_cc_pages_index ON commoncrawl_pages(index_id);
CREATE INDEX IF NOT EXISTS idx_cc_pages_crawl ON commoncrawl_pages(crawl);
