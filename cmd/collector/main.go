// Command collector acquires data from registered sources and writes it into a
// single SQLite database.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/PeacexF/MassD/internal/collect"
	"github.com/PeacexF/MassD/internal/config"
	"github.com/PeacexF/MassD/internal/db"
	"github.com/PeacexF/MassD/internal/fetch"
	"github.com/PeacexF/MassD/internal/source"
	"github.com/PeacexF/MassD/internal/stats"

	_ "github.com/PeacexF/MassD/internal/source/testsrc"
)

var version = "dev"

const usage = `collector - massive data collector

Usage:
  collector <command> [flags]

Commands:
  init                 create the database and apply migrations
  migrate              apply pending migrations
  sources              list registered sources
  status               show per-source collection state
  run <source>|all     collect from one source, or every enabled source
  version              print version

Flags:
  --db PATH            SQLite database (default from config, else ./data/massive.db)
  --config PATH        YAML config file
  --workers N          fetch concurrency
  --batch-size N       records per transaction
  --timeout DUR        per-request timeout (e.g. 30s)
  --max-records N      stop after N records
  --since VALUE        lower time bound (RFC3339, YYYY-MM-DD or a duration like 48h)
  --until VALUE        upper time bound
  --rate-limit N       requests per second (0 = unlimited)
  --resume=false       ignore saved state and start over
  --reset              delete saved state for the source before running
  --verbose            debug logging
`

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		fmt.Print(usage)
		return errors.New("no command given")
	}

	cmd := os.Args[1]
	fs, opts := newFlags(cmd)
	positional, err := parseArgs(fs, os.Args[2:])
	if err != nil {
		return err
	}

	switch cmd {
	case "help", "-h", "--help":
		fmt.Print(usage)
		return nil
	case "version":
		fmt.Println("collector", version)
		return nil
	}

	cfg, err := config.Load(opts.configPath)
	if err != nil {
		return err
	}
	if err := opts.applyConfig(cfg, fs); err != nil {
		return err
	}
	log := newLogger(opts.verbose)

	ctx, stop := signalContext(log)
	defer stop()

	switch cmd {
	case "init", "migrate":
		return cmdMigrate(ctx, opts, cmd == "init")
	case "sources":
		return cmdSources(cfg)
	case "status":
		return cmdStatus(ctx, opts)
	case "run":
		return cmdRun(ctx, cfg, opts, positional, log)
	default:
		fmt.Print(usage)
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// parseArgs allows flags and positional arguments to interleave, so both
// `run test --verbose` and `run --verbose test` work.
func parseArgs(fs *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		args = fs.Args()
		if len(args) == 0 {
			return positional, nil
		}
		positional = append(positional, args[0])
		args = args[1:]
	}
}

func newLogger(verbose bool) *slog.Logger {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}

// signalContext cancels on the first interrupt so the current batch commits,
// and exits hard on the second.
func signalContext(log *slog.Logger) (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		log.Warn("interrupt received, finishing current batch")
		cancel()
		<-ch
		log.Error("second interrupt, exiting immediately")
		os.Exit(130)
	}()
	return ctx, func() { signal.Stop(ch); cancel() }
}

func cmdMigrate(ctx context.Context, o *options, initDB bool) error {
	d, err := db.Open(o.dbPath, o.sqliteOptions())
	if err != nil {
		return err
	}
	defer d.Close()

	n, err := d.Migrate(ctx)
	if err != nil {
		return err
	}
	if initDB {
		fmt.Printf("database ready: %s\n", d.Path)
	}
	fmt.Printf("migrations applied: %d\n", n)
	return nil
}

func cmdSources(cfg *config.Config) error {
	names := source.Names()
	if len(names) == 0 {
		fmt.Println("no sources registered")
		return nil
	}
	for _, s := range source.All() {
		state := "disabled"
		if cfg.Enabled(s.Name()) {
			state = "enabled"
		}
		fmt.Printf("%-14s %-9s %s\n", s.Name(), state, source.Describe(s))
	}
	return nil
}

func cmdStatus(ctx context.Context, o *options) error {
	info, err := os.Stat(o.dbPath)
	if err != nil {
		return fmt.Errorf("no database at %s (run `collector init`)", o.dbPath)
	}
	d, err := db.Open(o.dbPath, o.sqliteOptions())
	if err != nil {
		return err
	}
	defer d.Close()

	fmt.Printf("database: %s (%s)\n\n", o.dbPath, stats.Bytes(info.Size()))
	runs, err := collect.LastRuns(ctx, d)
	if err != nil {
		return err
	}
	if len(runs) == 0 {
		fmt.Println("no runs recorded yet")
		return nil
	}
	fmt.Printf("%-14s %-10s %-22s %12s %8s  %s\n", "SOURCE", "STATUS", "LAST RUN", "RECORDS", "ERRORS", "STATE")
	for _, r := range runs {
		when := r.FinishedAt
		if when == "" {
			when = r.StartedAt
		}
		if len(when) > 19 {
			when = when[:19] + "Z"
		}
		fmt.Printf("%-14s %-10s %-22s %12s %8d  %s\n",
			r.Source, r.Status, when, stats.Commas(r.Records), r.Errors, truncate(r.State, 48))
	}
	return nil
}

func cmdRun(ctx context.Context, cfg *config.Config, o *options, args []string, log *slog.Logger) error {
	if len(args) == 0 {
		return errors.New("run requires a source name (or `all`)")
	}

	var targets []source.Source
	if args[0] == "all" {
		for _, s := range source.All() {
			if cfg.Enabled(s.Name()) {
				targets = append(targets, s)
			}
		}
		if len(targets) == 0 {
			return errors.New("no sources enabled in config; enable some or name one explicitly")
		}
	} else {
		for _, name := range args {
			s, ok := source.Get(name)
			if !ok {
				return fmt.Errorf("unknown source %q (see `collector sources`)", name)
			}
			targets = append(targets, s)
		}
	}

	d, err := db.Open(o.dbPath, o.sqliteOptions())
	if err != nil {
		return err
	}
	defer d.Close()
	if _, err := d.Migrate(ctx); err != nil {
		return err
	}

	var failures []error
	for _, s := range targets {
		if ctx.Err() != nil {
			break
		}
		if o.reset {
			if err := collect.ClearState(ctx, d, s.Name()); err != nil {
				return err
			}
		}

		st := stats.New()
		httpClient := fetch.New(fetch.Options{
			UserAgent:   cfg.UserAgent,
			Timeout:     o.timeout,
			MaxRetries:  cfg.MaxRetries,
			RateLimit:   o.rateLimit,
			Concurrency: o.workers,
			Stats:       st,
		})

		res, err := collect.Run(ctx, d, s, collect.Options{
			Options: source.Options{
				Workers:    o.workers,
				BatchSize:  o.batchSize,
				MaxRecords: o.maxRecords,
				Since:      o.since,
				Until:      o.until,
				Resume:     o.resume,
				Verbose:    o.verbose,
				Timeout:    o.timeout,
			},
			HTTP:   httpClient,
			Config: source.Config(cfg.Source(s.Name())),
			Log:    log.With("source", s.Name()),
		})

		fmt.Println()
		fmt.Print(res.Stats.Report(s.Name()))
		fmt.Printf("Status: %s\n", res.Status)
		if err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", s.Name(), err))
		}
	}

	if err := d.Checkpoint(context.WithoutCancel(ctx)); err != nil {
		log.Warn("wal checkpoint failed", "error", err)
	}
	return errors.Join(failures...)
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func parseTimeFlag(v string) (time.Time, error) {
	if v == "" {
		return time.Time{}, nil
	}
	if d, err := time.ParseDuration(v); err == nil {
		return time.Now().UTC().Add(-d), nil
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05", "2006-01-02"} {
		if t, err := time.Parse(layout, v); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse time %q", v)
}
