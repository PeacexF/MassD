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

## Secrets

Keep tokens in the environment and reference them as `${VAR}`. `config.yaml` is
git-ignored; `config.example.yaml` is the file that gets committed.
