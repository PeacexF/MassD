# Architecture

```
                  Scheduler (cron / GitHub Actions)
                              |
                          Go CLI
                              |
                 +------------+------------+
                 |                         |
           source adapter            source state
                 |
        discover -> fetch -> parse -> normalize
                 |
           bounded queue          (backpressure)
                 |
            single DB writer      (batched transactions)
                 |
               SQLite  ->  sqlite3 / export.py -> JSONL
```

## Packages

| Package | Responsibility |
| --- | --- |
| `cmd/collector` | CLI, flags, signal handling |
| `internal/source` | the `Source` interface and the `Environment` handed to adapters |
| `internal/source/<name>` | one adapter per source; acquisition and parsing only |
| `internal/collect` | run lifecycle, bounded queue, run bookkeeping, state |
| `internal/db` | connection setup, pragmas, migrations, batched writer |
| `internal/record` | the record type and INSERT generation |
| `internal/fetch` | retries, backoff, rate limiting, size limits, byte accounting |
| `internal/config` | optional YAML configuration |
| `internal/stats` | run counters and the throughput report |

## The source contract

```go
type Source interface {
    Name() string
    Collect(ctx context.Context, env *Environment) error
}
```

An adapter owns acquisition and parsing for one source and nothing else. It
never opens the database, never manages transactions and never implements HTTP
retries. Through `env` it can:

- `Emit(ctx, records...)` — queue rows for the writer
- `LoadState` / `SaveState` — resume where the last run stopped
- `Error(ctx, err, context)` — record a failure without ending the run
- `HTTP` — the shared client, optionally cloned with stricter limits
- `DB` — read-only queries (a crawl frontier, ids already collected)
- `Config`, `Opts`, `Log`

Adapters register themselves:

```go
func init() { source.Register(&Source{}) }
```

and `cmd/collector` imports them for their side effects, so adding a source
touches one new package plus one import line and one migration.

## Concurrency

Fetch workers run in parallel; exactly one goroutine writes to SQLite. They are
connected by a bounded channel, so when the writer falls behind, `Emit` blocks
and the fetchers slow down on their own. Nothing grows without limit.

## Crash safety

Each transaction contains a batch of records *and* the state update that covers
them:

```
BEGIN -> insert batch -> update _source_state -> COMMIT
```

If the process dies before `COMMIT`, both the rows and the state advance are
rolled back and the next run repeats that batch. Duplicate rows are then dropped
by each source's uniqueness constraint, so repeating a batch is harmless.

`SIGINT` cancels the run context: the in-flight batch is committed, the run is
recorded as `cancelled`, and the collector exits. A second `SIGINT` exits
immediately.

## Metadata tables

| Table | Contents |
| --- | --- |
| `_runs` | one row per execution: status, records, duplicates, errors, requests, bytes |
| `_source_state` | resumable per-source JSON cursor |
| `_errors` | non-fatal failures, with run id and context |
| `_migrations` | applied schema versions |

## Storage conventions

Frequently queried fields get narrow typed columns; raw payloads are kept only
where the original response is worth preserving (for example
`github_events.payload`). Entities and their observations are separate tables
(`github_repositories` and `github_repository_snapshots`), so the dataset gains
history instead of only tracking current state.

Timestamps are RFC 3339 in UTC, which sorts lexicographically and works with
SQLite's date functions.

## Adding a source

1. `internal/db/migrations/NNN_<name>.sql` — tables, indexes, uniqueness
2. `internal/source/<name>/<name>.go` — implement `Source`, register in `init`
3. import it in `cmd/collector/main.go`
4. add a section to `config/config.example.yaml`
5. test the parser against fixtures and the adapter against `httptest`
