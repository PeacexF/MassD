package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/PeacexF/MassD/internal/config"
	"github.com/PeacexF/MassD/internal/db"
)

type options struct {
	configPath string
	dbPath     string
	workers    int
	batchSize  int
	timeout    time.Duration
	maxRecords int64
	sinceRaw   string
	untilRaw   string
	since      time.Time
	until      time.Time
	rateLimit  float64
	resume     bool
	reset      bool
	verbose    bool

	cfg *config.Config
}

func newFlags(cmd string) (*flag.FlagSet, *options) {
	o := &options{}
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.Usage = func() { fmt.Fprint(os.Stderr, usage) }

	fs.StringVar(&o.configPath, "config", "", "path to config file")
	fs.StringVar(&o.dbPath, "db", "", "path to SQLite database")
	fs.IntVar(&o.workers, "workers", 0, "fetch concurrency")
	fs.IntVar(&o.batchSize, "batch-size", 0, "records per transaction")
	fs.DurationVar(&o.timeout, "timeout", 0, "per-request timeout")
	fs.Int64Var(&o.maxRecords, "max-records", 0, "stop after N records")
	fs.StringVar(&o.sinceRaw, "since", "", "lower time bound")
	fs.StringVar(&o.untilRaw, "until", "", "upper time bound")
	fs.Float64Var(&o.rateLimit, "rate-limit", -1, "requests per second")
	fs.BoolVar(&o.resume, "resume", true, "continue from saved state")
	fs.BoolVar(&o.reset, "reset", false, "discard saved state before running")
	fs.BoolVar(&o.verbose, "verbose", false, "debug logging")
	return fs, o
}

// applyConfig fills unset flags from the config file; explicit flags win.
func (o *options) applyConfig(cfg *config.Config, fs *flag.FlagSet) error {
	o.cfg = cfg
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })

	if !set["db"] {
		o.dbPath = cfg.Database
	}
	if !set["workers"] {
		o.workers = cfg.Workers
	}
	if !set["batch-size"] {
		o.batchSize = cfg.BatchSize
	}
	if !set["timeout"] {
		o.timeout = cfg.ParseTimeout()
	}
	if !set["rate-limit"] || o.rateLimit < 0 {
		o.rateLimit = cfg.RateLimit
	}
	if o.dbPath == "" {
		o.dbPath = "./data/massive.db"
	}
	if o.workers <= 0 {
		o.workers = 8
	}
	if o.batchSize <= 0 {
		o.batchSize = 10000
	}

	var err error
	if o.since, err = parseTimeFlag(o.sinceRaw); err != nil {
		return err
	}
	if o.until, err = parseTimeFlag(o.untilRaw); err != nil {
		return err
	}
	return nil
}

func (o *options) sqliteOptions() db.Options {
	return db.Options{
		CacheSizeKB: o.cfg.SQLite.CacheSizeKB,
		MMapSizeMB:  o.cfg.SQLite.MMapSizeMB,
		BusyTimeout: o.cfg.ParseBusyTimeout(),
		MaxConns:    o.cfg.SQLite.MaxConns,
	}
}
