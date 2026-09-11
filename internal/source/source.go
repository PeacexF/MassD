// Package source defines the contract every data source adapter implements and
// the environment the runner hands it. An adapter owns acquisition and parsing
// for one source; it owns nothing else.
package source

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/PeacexF/MassD/internal/fetch"
	"github.com/PeacexF/MassD/internal/record"
)

type Source interface {
	Name() string
	Collect(ctx context.Context, env *Environment) error
}

// Describer lets a source explain itself in `collector sources`.
type Describer interface {
	Describe() string
}

// ErrLimit is returned by Emit once --max-records is reached. Adapters should
// return it unchanged; the runner treats it as a clean stop.
var ErrLimit = errors.New("record limit reached")

type Options struct {
	Workers    int
	BatchSize  int
	MaxRecords int64
	Since      time.Time
	Until      time.Time
	Resume     bool
	Verbose    bool
	Timeout    time.Duration
}

// Sink is the runner side of the pipeline: a bounded queue feeding one writer.
type Sink interface {
	Emit(ctx context.Context, recs ...record.Record) error
	PutState(ctx context.Context, state string) error
}

// Queryer gives sources read access to the database (a crawler frontier, a
// previously collected id set). Writes always go through Emit so that a single
// writer owns every transaction.
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type Environment struct {
	Source   string
	RunID    int64
	HTTP     *fetch.Client
	Config   Config
	Opts     Options
	Log      *slog.Logger
	Sink     Sink
	DB       Queryer
	rawState json.RawMessage
}

func NewEnvironment(name string, runID int64, sink Sink, q Queryer, http *fetch.Client, cfg Config, opts Options, log *slog.Logger, state json.RawMessage) *Environment {
	return &Environment{
		Source: name, RunID: runID, Sink: sink, DB: q, HTTP: http,
		Config: cfg, Opts: opts, Log: log, rawState: state,
	}
}

func (e *Environment) Emit(ctx context.Context, recs ...record.Record) error {
	return e.Sink.Emit(ctx, recs...)
}

// LoadState decodes persisted state into v. With --resume=false it leaves v
// untouched, so the source starts from scratch.
func (e *Environment) LoadState(v any) error {
	if !e.Opts.Resume || len(e.rawState) == 0 {
		return nil
	}
	return json.Unmarshal(e.rawState, v)
}

func (e *Environment) HasState() bool { return e.Opts.Resume && len(e.rawState) > 0 }

// SaveState queues state to be committed with the records emitted before it.
func (e *Environment) SaveState(ctx context.Context, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	e.rawState = b
	return e.Sink.PutState(ctx, string(b))
}

// Error records a non-fatal failure and lets the run continue.
func (e *Environment) Error(ctx context.Context, err error, where string) {
	if err == nil {
		return
	}
	e.Log.Warn("collection error", "source", e.Source, "context", where, "error", err)
	rec := record.New("_errors").
		Set("run_id", e.RunID).
		Set("source", e.Source).
		Set("occurred_at", time.Now().UTC().Format(time.RFC3339Nano)).
		Set("error", err.Error()).
		Set("context", where)
	_ = e.Sink.Emit(ctx, rec)
}

// Window returns the effective [since, until) filter, defaulting until to now.
func (e *Environment) Window() (time.Time, time.Time) {
	until := e.Opts.Until
	if until.IsZero() {
		until = time.Now().UTC()
	}
	return e.Opts.Since, until
}
