# Configuration

Configuration is optional: every value has a default, and command line flags
override the file. Start from the example:

```bash
cp config/config.example.yaml config/config.yaml
```

Without `--config`, these paths are tried in order:
`config/config.yaml`, `config.yaml`, `config/config.yml`.

## Global settings

| Key | Default | Meaning |
| --- | --- | --- |
| `database` | `./data/massive.db` | SQLite file; created if missing |
| `workers` | `8` | fetch concurrency |
| `batch_size` | `10000` | records per transaction |
| `timeout` | `60s` | per-request timeout |
| `max_retries` | `4` | retries for transient failures |
| `rate_limit` | `0` | global requests/second; 0 is unlimited |

### `sqlite`

| Key | Default | Meaning |
| --- | --- | --- |
| `cache_size_kb` | `65536` | page cache per connection |
| `mmap_size_mb` | `256` | memory-mapped I/O; 0 disables |
| `busy_timeout` | `30s` | wait on a locked database |
| `max_conns` | `4` | connections; only readers benefit |

`journal_mode=WAL`, `synchronous=NORMAL`, `foreign_keys=ON` and
`temp_store=MEMORY` are always applied. Tune the rest by benchmarking rather
than guessing, and do not trade integrity for write speed.

## Flags

```
--db PATH            --config PATH        --workers N
--batch-size N       --timeout DUR        --max-records N
--since VALUE        --until VALUE        --rate-limit N
--resume=false       --reset              --verbose
```

`--since` and `--until` accept RFC 3339, `YYYY-MM-DD`, or a duration measured
back from now (`48h`).

`--resume=false` ignores saved state for this run; `--reset` deletes it first.

## Sources

Each key under `sources:` is a source name. `enabled: true` includes it in
`collector run all`; naming a source explicitly always runs it.

### rss

| Key | Default | Meaning |
| --- | --- | --- |
| `feeds` | – | inline list of feed URLs |
| `feeds_file` | – | file with one URL per line, `#` for comments |
| `workers` | global | feeds fetched in parallel |
| `max_feed_bytes` | `33554432` | response size limit |

Re-running is cheap: each feed is requested with `If-None-Match` /
`If-Modified-Since`, and `UNIQUE(feed, guid)` drops items already stored.

### gdelt

| Key | Default | Meaning |
| --- | --- | --- |
| `mode` | `latest` | `latest` reads the newest 15-minute files; `backfill` walks the master file list |
| `datasets` | `[export, mentions]` | which GDELT tables to ingest |
| `max_files` | `0` | cap files per run; 0 is unlimited |
| `base_url` | `http://data.gdeltproject.org/gdeltv2` | endpoint |
| `max_file_bytes` | `536870912` | download size limit |

Backfill honours `--since` and `--until`, and state advances one file at a time,
so an interrupted backfill resumes at the right file.

### github

| Key | Default | Meaning |
| --- | --- | --- |
| `token` | – | use `${GITHUB_TOKEN}`; never write a token into the file |
| `datasets` | `[repositories, events]` | also `repo_details` |
| `max_pages` | `0` | repository pages per run; 0 until the API runs out |
| `event_pages` | `3` | event pages per run (the API exposes 10) |
| `store_payload` | `true` | keep the raw event payload JSON |
| `detail_batch` | `1000` | repositories considered per `repo_details` run |
| `detail_workers` | global | concurrent GraphQL queries |
| `refresh_after` | `168h` | skip repositories snapshotted more recently |
| `topics` | `0` | topics per repository; each one costs GraphQL quota |

Unauthenticated REST is 60 requests/hour, authenticated is 5,000.
`repo_details` uses GraphQL, which requires a token and bills points instead of
requests: 100 repositories per query costs about one point, so a token is worth
roughly a hundredfold more snapshots per hour.

### commoncrawl

| Key | Default | Meaning |
| --- | --- | --- |
| `crawl` | `latest` | crawl id such as `CC-MAIN-2026-34`, or the newest published one |
| `datasets` | `[index]` | also `pages` |
| `max_shards` | `1` | CDX shards per run |
| `shard_count` | `300` | shards in a crawl |
| `checkpoint_lines` | `50000` | lines between resume checkpoints |
| `page_batch` | `500` | index rows fetched from WARC per run |
| `page_workers` | global | concurrent ranged requests |
| `page_mime` | `text/html` | MIME prefix filter for `pages`; empty means any |
| `collect_links` | `true` | count and resolve links while extracting |
| `store_text` | `false` | keep extracted text (`text_length` is always stored) |
| `text_limit` | `4096` | bytes of text retained when `store_text` is on |
| `data_url` | `https://data.commoncrawl.org` | bulk data host |
| `index_url` | `https://index.commoncrawl.org` | crawl listing host |

The `index` dataset streams gzipped CDX shards and never holds one in memory.
Progress is a shard number plus a line offset, so an interrupted shard resumes
where it stopped; resuming re-reads the shard's earlier lines, which costs
bandwidth but cannot duplicate rows or skip them.

`pages` works from index rows already in the database, fetching each document
with a `Range` request against its WARC file. It skips rows that already have a
page, so it can be run repeatedly to work through the index.

Switching `crawl` resets shard progress; the `pages` cursor is independent.

### Storage cost

Measured on a real shard of `CC-MAIN-2026-34`:

| | |
| --- | --- |
| rows | 492,034 |
| database | 327 MB (about 665 bytes per row) |
| table | 201 MB |
| indexes | 126 MB (39%) |

A full crawl holds billions of captures, so ingesting one whole is a multi
terabyte decision. Start with a bounded partition, measure, then scale. If an
index is not worth its size for your queries, drop it:

```sql
DROP INDEX idx_cc_index_digest;   -- content-duplicate lookups
DROP INDEX idx_cc_index_status;   -- status/mime filtering
```

## Secrets

Keep tokens in the environment and reference them as `${VAR}`. `config.yaml` is
git-ignored; `config.example.yaml` is the file that gets committed.
