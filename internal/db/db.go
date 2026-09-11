// Package db owns SQLite access: connection setup, pragmas, migrations and the
// batched writer. The database stays a plain SQLite file: no ORM, nothing that
// stops `sqlite3 massive.db` from working as expected.
package db

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite driver; keeps CGO_ENABLED=0 builds working
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Options tunes the SQLite connection; zero values get ingestion-friendly
// defaults that do not trade away durability.
type Options struct {
	CacheSizeKB int
	MMapSizeMB  int
	BusyTimeout time.Duration
	// MaxConns only helps concurrent readers; SQLite allows a single writer.
	MaxConns int
	ReadOnly bool
}

func (o Options) withDefaults() Options {
	if o.CacheSizeKB == 0 {
		o.CacheSizeKB = 64 * 1024 // 64 MiB
	}
	if o.BusyTimeout == 0 {
		o.BusyTimeout = 30 * time.Second
	}
	if o.MaxConns == 0 {
		o.MaxConns = 4
	}
	return o
}

type DB struct {
	*sql.DB
	Path string
}

func Open(path string, opts Options) (*DB, error) {
	opts = opts.withDefaults()

	if !opts.ReadOnly {
		if dir := filepath.Dir(path); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("create database directory: %w", err)
			}
		}
	}

	// Pragmas travel in the DSN so they apply to every pooled connection, not
	// just the first one database/sql happens to open.
	q := url.Values{}
	add := func(p string) { q.Add("_pragma", p) }
	add("journal_mode(WAL)")
	add("synchronous(NORMAL)")
	add("foreign_keys(ON)")
	add("temp_store(MEMORY)")
	add("busy_timeout(" + strconv.FormatInt(opts.BusyTimeout.Milliseconds(), 10) + ")")
	add("cache_size(-" + strconv.Itoa(opts.CacheSizeKB) + ")")
	if opts.MMapSizeMB > 0 {
		add("mmap_size(" + strconv.Itoa(opts.MMapSizeMB*1024*1024) + ")")
	}
	// Take the write lock at BEGIN instead of on first write, so a write
	// transaction never has to upgrade (and fail) mid-batch.
	q.Set("_txlock", "immediate")
	if opts.ReadOnly {
		q.Set("mode", "ro")
		q.Del("_txlock")
	}

	dsn := "file:" + path + "?" + q.Encode()
	sqldb, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	sqldb.SetMaxOpenConns(opts.MaxConns)
	sqldb.SetMaxIdleConns(opts.MaxConns)
	sqldb.SetConnMaxLifetime(0)

	if err := sqldb.Ping(); err != nil {
		sqldb.Close()
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	return &DB{DB: sqldb, Path: path}, nil
}

type migration struct {
	version int
	name    string
	sql     string
}

func loadMigrations() ([]migration, error) {
	entries, err := fs.Glob(migrationFS, "migrations/*.sql")
	if err != nil {
		return nil, err
	}
	sort.Strings(entries)

	out := make([]migration, 0, len(entries))
	seen := map[int]string{}
	for _, e := range entries {
		base := filepath.Base(e)
		numPart, rest, ok := strings.Cut(base, "_")
		if !ok {
			return nil, fmt.Errorf("migration %q: expected NNN_name.sql", base)
		}
		n, err := strconv.Atoi(numPart)
		if err != nil {
			return nil, fmt.Errorf("migration %q: bad version prefix: %w", base, err)
		}
		if prev, dup := seen[n]; dup {
			return nil, fmt.Errorf("migration version %d used twice: %s and %s", n, prev, base)
		}
		seen[n] = base
		body, err := migrationFS.ReadFile(e)
		if err != nil {
			return nil, err
		}
		out = append(out, migration{version: n, name: strings.TrimSuffix(rest, ".sql"), sql: string(body)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

// Migrate applies pending migrations and reports how many ran. Each runs in one
// transaction with its bookkeeping row, so a crash cannot half-apply a schema.
func (d *DB) Migrate(ctx context.Context) (int, error) {
	migrations, err := loadMigrations()
	if err != nil {
		return 0, err
	}

	if _, err := d.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS _migrations (
		version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL)`); err != nil {
		return 0, fmt.Errorf("create _migrations: %w", err)
	}

	applied := map[int]bool{}
	rows, err := d.QueryContext(ctx, `SELECT version FROM _migrations`)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return 0, err
		}
		applied[v] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()

	n := 0
	for _, m := range migrations {
		if applied[m.version] {
			continue
		}
		tx, err := d.BeginTx(ctx, nil)
		if err != nil {
			return n, err
		}
		if _, err := tx.ExecContext(ctx, m.sql); err != nil {
			tx.Rollback()
			return n, fmt.Errorf("migration %03d_%s: %w", m.version, m.name, err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO _migrations (version, name, applied_at) VALUES (?,?,?)`,
			m.version, m.name, Now()); err != nil {
			tx.Rollback()
			return n, err
		}
		if err := tx.Commit(); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// Now is the database-wide timestamp format: UTC RFC3339, which sorts
// lexicographically and is understood by SQLite's date functions.
func Now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// Checkpoint truncates the WAL so the main file is self-contained before upload.
func (d *DB) Checkpoint(ctx context.Context) error {
	_, err := d.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`)
	return err
}
