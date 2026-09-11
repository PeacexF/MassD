// Package collect runs one source: it owns the run lifecycle, the bounded
// record queue, the single database writer and the run's bookkeeping.
package collect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/PeacexF/MassD/internal/db"
	"github.com/PeacexF/MassD/internal/fetch"
	"github.com/PeacexF/MassD/internal/record"
	"github.com/PeacexF/MassD/internal/source"
	"github.com/PeacexF/MassD/internal/stats"
)

type Options struct {
	source.Options
	HTTP      *fetch.Client
	Config    source.Config
	Log       *slog.Logger
	QueueSize int
	// FlushInterval bounds how long finished work sits unflushed when a source
	// produces records slowly.
	FlushInterval time.Duration
}

type Result struct {
	RunID  int64
	Status string
	Stats  stats.Snapshot
}

func Run(ctx context.Context, d *db.DB, src source.Source, opts Options) (Result, error) {
	name := src.Name()
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.QueueSize <= 0 {
		opts.QueueSize = 64
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = 30 * time.Second
	}

	st := stats.New()
	runID, err := startRun(ctx, d, name)
	if err != nil {
		return Result{}, err
	}

	var state json.RawMessage
	if opts.Resume {
		if state, err = LoadState(ctx, d, name); err != nil {
			return Result{RunID: runID}, err
		}
	}

	p := newPipeline(d, name, opts, st)
	env := source.NewEnvironment(name, runID, p, d, opts.HTTP, opts.Config, opts.Options, opts.Log, state)

	collectErr := src.Collect(ctx, env)
	writeErr := p.close()
	st.Finish()

	status := "completed"
	switch {
	case errors.Is(collectErr, source.ErrLimit):
		collectErr = nil
	case errors.Is(collectErr, context.Canceled), errors.Is(ctx.Err(), context.Canceled):
		status = "cancelled"
		collectErr = nil
	}
	err = errors.Join(collectErr, writeErr)
	if err != nil {
		status = "failed"
	}

	snap := st.Snapshot()
	// Bookkeeping uses the background context so a cancelled run still records
	// what it achieved.
	if ferr := finishRun(context.WithoutCancel(ctx), d, runID, status, snap); ferr != nil {
		opts.Log.Warn("could not finalize run", "source", name, "error", ferr)
	}
	return Result{RunID: runID, Status: status, Stats: snap}, err
}

func startRun(ctx context.Context, d *db.DB, source string) (int64, error) {
	res, err := d.ExecContext(ctx,
		`INSERT INTO _runs (source, started_at, status) VALUES (?,?,'running')`, source, db.Now())
	if err != nil {
		return 0, fmt.Errorf("start run: %w", err)
	}
	return res.LastInsertId()
}

func finishRun(ctx context.Context, d *db.DB, runID int64, status string, s stats.Snapshot) error {
	_, err := d.ExecContext(ctx,
		`UPDATE _runs SET finished_at=?, status=?, records=?, errors=?, duplicates=?, requests=?, bytes=? WHERE id=?`,
		db.Now(), status, s.Inserted, s.Errors, s.Duplicates, s.Requests, s.Bytes, runID)
	return err
}

type message struct {
	recs  []record.Record
	state *string
}

// pipeline is the bounded queue between fetch workers and the single writer.
// A full queue blocks Emit, which is the backpressure mechanism.
type pipeline struct {
	ch       chan message
	done     chan struct{}
	writeErr error
	st       *stats.Stats
	max      int64
	emitted  atomic.Int64
	log      *slog.Logger
}

func newPipeline(d *db.DB, name string, opts Options, st *stats.Stats) *pipeline {
	p := &pipeline{
		ch:   make(chan message, opts.QueueSize),
		done: make(chan struct{}),
		st:   st,
		max:  opts.MaxRecords,
		log:  opts.Log,
	}
	w := db.NewWriter(d, name, opts.BatchSize, st)

	go func() {
		defer close(p.done)
		defer w.Close()

		ctx := context.WithoutCancel(context.Background())
		ticker := time.NewTicker(opts.FlushInterval)
		defer ticker.Stop()

		for {
			select {
			case m, ok := <-p.ch:
				if !ok {
					if err := w.Flush(ctx); err != nil {
						p.writeErr = err
					}
					return
				}
				if m.state != nil {
					w.SetState(*m.state)
				}
				if err := w.Add(ctx, m.recs...); err != nil {
					p.writeErr = err
					p.drain()
					return
				}
			case <-ticker.C:
				if w.Buffered() > 0 {
					if err := w.Flush(ctx); err != nil {
						p.writeErr = err
						p.drain()
						return
					}
				}
			}
		}
	}()
	return p
}

// drain keeps Emit from blocking forever after the writer has given up.
func (p *pipeline) drain() {
	go func() {
		for range p.ch {
		}
	}()
}

func (p *pipeline) Emit(ctx context.Context, recs ...record.Record) error {
	if len(recs) == 0 {
		return nil
	}
	// The queue crosses a goroutine boundary, so sources are free to reuse
	// their batch buffer as soon as Emit returns.
	recs = append(recs[:0:0], recs...)

	limited := false
	if p.max > 0 {
		n := countData(recs)
		if n > 0 {
			prev := p.emitted.Add(n) - n
			if room := p.max - prev; room < n {
				recs = truncateData(recs, max(room, 0))
				limited = true
			}
		}
	}

	if len(recs) > 0 {
		if err := p.send(ctx, message{recs: recs}); err != nil {
			return err
		}
		p.st.Fetched.Add(countData(recs))
	}
	if limited {
		return source.ErrLimit
	}
	return nil
}

func (p *pipeline) PutState(ctx context.Context, state string) error {
	return p.send(ctx, message{state: &state})
}

func (p *pipeline) send(ctx context.Context, m message) error {
	select {
	case p.ch <- m:
		return nil
	case <-p.done:
		if p.writeErr != nil {
			return p.writeErr
		}
		return errors.New("writer stopped")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *pipeline) close() error {
	close(p.ch)
	<-p.done
	return p.writeErr
}

// countData ignores internal tables so error rows never consume the record
// budget set by --max-records.
func countData(recs []record.Record) int64 {
	var n int64
	for _, r := range recs {
		if !strings.HasPrefix(r.Table, "_") {
			n++
		}
	}
	return n
}

func truncateData(recs []record.Record, room int64) []record.Record {
	out := recs[:0:0]
	for _, r := range recs {
		if strings.HasPrefix(r.Table, "_") {
			out = append(out, r)
			continue
		}
		if room > 0 {
			out = append(out, r)
			room--
		}
	}
	return out
}
