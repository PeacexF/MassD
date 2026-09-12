# MassD — Massive Data Collector

Continuously collect large amounts of publicly accessible data and store all of
it in one queryable SQLite database.

SQLite is the canonical storage format. JSONL is an export format, not an
ingestion format. Adding a new source should be cheap, and a new source should
immediately contribute millions of queryable records to the same database.

## Quick start

```bash
go build -o bin/collector ./cmd/collector

./bin/collector init                 # create the database and apply migrations
./bin/collector sources              # what can be collected
./bin/collector run rss              # collect one source
./bin/collector run all              # collect every enabled source
./bin/collector status               # per-source progress

sqlite3 data/massive.db              # the database is plain SQLite
```

Export any query to JSONL, streaming, without loading the result into memory:

```bash
python3 scripts/export.py data/massive.db \
    --query "SELECT * FROM gdelt_events WHERE avg_tone < -5" \
    --output angry_news.jsonl

python3 scripts/export.py data/massive.db --table rss_items --output items.jsonl.gz
```

## Sources

| Source | Records | Notes |
| --- | --- | --- |
| `rss` | feed items | RSS, Atom and RDF; conditional GETs make re-runs nearly free |
| `gdelt` | events, mentions | GDELT 2.0 publishes new files every 15 minutes |
| `github` | repositories, events, snapshots | REST for enumeration and events, GraphQL for snapshots |
| `test` | synthetic | no network; used to verify the pipeline |

### GitHub and the two APIs

The public repository enumeration (`/repositories?since=`) and the public event
stream exist only in REST, so those datasets use REST. Repository snapshots use
GraphQL instead: `nodes(ids: [...])` returns 100 repositories for roughly the
cost of one REST call, which is the difference between thousands and hundreds of
thousands of snapshots per hour.

GraphQL rejects anonymous requests, so `repo_details` needs a token. A classic
token with no scopes is enough for public data:

```bash
export GITHUB_TOKEN=...
```

## Configuration

Everything has a default; the config file is optional.

```bash
cp config/config.example.yaml config/config.yaml
```

Common flags:

```
--db PATH          --workers N        --batch-size N
--max-records N    --since VALUE      --until VALUE
--resume=false     --reset            --verbose
```

See [docs/configuration.md](docs/configuration.md).

## Design

```
source adapter -> fetch -> parse -> normalize -> bounded queue -> one writer -> SQLite
```

Fetching is concurrent, writing is not: a single writer batches records into
transactions, and each transaction commits the records **and** the source's
resume state together. Killing the collector at any moment therefore loses at
most one unfinished batch, which the next run simply repeats.

See [docs/architecture.md](docs/architecture.md) and
[.github/notes/PLAN.md](.github/notes/PLAN.md).

## Development

```bash
go test ./...          # Go tests
go test -race ./...    # concurrency
pytest scripts         # exporter tests (pip install pytest)
```

## License

See [LICENSE](LICENSE).
