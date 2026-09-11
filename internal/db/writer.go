package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/PeacexF/MassD/internal/record"
	"github.com/PeacexF/MassD/internal/stats"
)

// maxVars stays under SQLITE_MAX_VARIABLE_NUMBER (32766) with room to spare.
const maxVars = 20000

// maxRowsPerStmt bounds generated SQL length for narrow tables.
const maxRowsPerStmt = 500

// Writer batches records into transactions. A single Writer is driven by a
// single goroutine; concurrency belongs upstream in the fetch workers.
//
// Source state is committed in the same transaction as the records that
// preceded it, so a crash rolls back data and state together and the source
// simply repeats the unfinished batch.
type Writer struct {
	db        *DB
	source    string
	batchSize int
	st        *stats.Stats

	buf          []record.Record
	pendingState *string
	stmts        map[string]*sql.Stmt
}

func NewWriter(d *DB, source string, batchSize int, st *stats.Stats) *Writer {
	if batchSize <= 0 {
		batchSize = 10000
	}
	return &Writer{
		db:        d,
		source:    source,
		batchSize: batchSize,
		st:        st,
		buf:       make([]record.Record, 0, batchSize),
		stmts:     map[string]*sql.Stmt{},
	}
}

func (w *Writer) Add(ctx context.Context, recs ...record.Record) error {
	w.buf = append(w.buf, recs...)
	if len(w.buf) >= w.batchSize {
		return w.Flush(ctx)
	}
	return nil
}

// SetState queues resumable state; it lands with the next commit.
func (w *Writer) SetState(state string) { w.pendingState = &state }

func (w *Writer) Buffered() int { return len(w.buf) }

func (w *Writer) Flush(ctx context.Context) error {
	if len(w.buf) == 0 && w.pendingState == nil {
		return nil
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, g := range group(w.buf) {
		if err := w.insertGroup(ctx, tx, g); err != nil {
			return err
		}
	}
	if w.pendingState != nil {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO _source_state (source, state, updated_at) VALUES (?,?,?)
			 ON CONFLICT(source) DO UPDATE SET state=excluded.state, updated_at=excluded.updated_at`,
			w.source, *w.pendingState, Now()); err != nil {
			return fmt.Errorf("persist state: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	w.buf = w.buf[:0]
	w.pendingState = nil
	return nil
}

type recGroup struct {
	proto record.Record
	rows  []record.Record
}

// group collects records sharing an INSERT signature, keeping first-appearance
// order so parent rows are inserted before rows referencing them.
func group(buf []record.Record) []recGroup {
	order := make([]string, 0, 8)
	byKey := make(map[string]*recGroup, 8)
	for _, r := range buf {
		k := r.Signature()
		g, ok := byKey[k]
		if !ok {
			g = &recGroup{proto: r}
			byKey[k] = g
			order = append(order, k)
		}
		g.rows = append(g.rows, r)
	}
	out := make([]recGroup, 0, len(order))
	for _, k := range order {
		out = append(out, *byKey[k])
	}
	return out
}

func (w *Writer) insertGroup(ctx context.Context, tx *sql.Tx, g recGroup) error {
	ncols := len(g.proto.Cols)
	if ncols == 0 {
		return fmt.Errorf("record for table %s has no columns", g.proto.Table)
	}
	perStmt := min(max(maxVars/ncols, 1), maxRowsPerStmt)

	args := make([]any, 0, perStmt*ncols)
	for start := 0; start < len(g.rows); start += perStmt {
		chunk := g.rows[start:min(start+perStmt, len(g.rows))]
		args = args[:0]
		for _, r := range chunk {
			if len(r.Vals) != ncols {
				return fmt.Errorf("record for table %s: %d columns, %d values", r.Table, ncols, len(r.Vals))
			}
			args = append(args, r.Vals...)
		}

		stmt, err := w.prepared(ctx, g.proto, len(chunk))
		if err != nil {
			return err
		}
		res, err := tx.StmtContext(ctx, stmt).ExecContext(ctx, args...)
		if err != nil {
			return fmt.Errorf("insert into %s: %w", g.proto.Table, err)
		}
		if w.st != nil {
			n, _ := res.RowsAffected()
			w.st.Inserted.Add(n)
			if d := int64(len(chunk)) - n; d > 0 {
				w.st.Duplicates.Add(d)
			}
		}
	}
	return nil
}

func (w *Writer) prepared(ctx context.Context, proto record.Record, rows int) (*sql.Stmt, error) {
	query := proto.InsertSQL(rows)
	if s, ok := w.stmts[query]; ok {
		return s, nil
	}
	s, err := w.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("prepare insert into %s: %w", proto.Table, err)
	}
	// Only full-size and remainder shapes recur, so the cache stays small.
	if len(w.stmts) > 256 {
		for k, old := range w.stmts {
			old.Close()
			delete(w.stmts, k)
		}
	}
	w.stmts[query] = s
	return s, nil
}

func (w *Writer) Close() error {
	for k, s := range w.stmts {
		s.Close()
		delete(w.stmts, k)
	}
	return nil
}
