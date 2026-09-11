package collect

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/PeacexF/MassD/internal/db"
	"github.com/PeacexF/MassD/internal/record"
	"github.com/PeacexF/MassD/internal/source"
)

type fakeSource struct {
	name    string
	collect func(context.Context, *source.Environment) error
}

func (f *fakeSource) Name() string { return f.name }
func (f *fakeSource) Collect(ctx context.Context, env *source.Environment) error {
	return f.collect(ctx, env)
}

func testDB(t *testing.T) *db.DB {
	t.Helper()
	d, err := db.Open(filepath.Join(t.TempDir(), "test.db"), db.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	if _, err := d.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return d
}

func testOptions() Options {
	return Options{
		Options: source.Options{BatchSize: 64, Resume: true},
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func emitRange(env *source.Environment, ctx context.Context, from, to int) error {
	for i := from; i < to; i++ {
		r := record.New("test_records").
			Set("seq", i).Set("payload", "p").Set("hash", "h").
			Set("run_id", env.RunID).Set("collected_at", db.Now()).
			OnConflict(record.Ignore)
		if err := env.Emit(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func TestRunPersistsRecordsStateAndBookkeeping(t *testing.T) {
	ctx := context.Background()
	d := testDB(t)

	src := &fakeSource{name: "test", collect: func(ctx context.Context, env *source.Environment) error {
		if err := emitRange(env, ctx, 0, 500); err != nil {
			return err
		}
		return env.SaveState(ctx, map[string]int{"seq": 500})
	}}

	res, err := Run(ctx, d, src, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != "completed" || res.Stats.Inserted != 500 {
		t.Fatalf("unexpected result: %+v", res)
	}

	var status string
	var records int64
	if err := d.QueryRow(`SELECT status, records FROM _runs WHERE id=?`, res.RunID).Scan(&status, &records); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || records != 500 {
		t.Fatalf("_runs row: status=%s records=%d", status, records)
	}

	state, err := LoadState(ctx, d, "test")
	if err != nil || string(state) != `{"seq":500}` {
		t.Fatalf("state = %s err = %v", state, err)
	}
}

func TestRunResumesFromState(t *testing.T) {
	ctx := context.Background()
	d := testDB(t)

	var seen int
	src := &fakeSource{name: "test", collect: func(ctx context.Context, env *source.Environment) error {
		var st struct {
			Seq int `json:"seq"`
		}
		if err := env.LoadState(&st); err != nil {
			return err
		}
		seen = st.Seq
		if err := emitRange(env, ctx, st.Seq, st.Seq+100); err != nil {
			return err
		}
		return env.SaveState(ctx, map[string]int{"seq": st.Seq + 100})
	}}

	if _, err := Run(ctx, d, src, testOptions()); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(ctx, d, src, testOptions()); err != nil {
		t.Fatal(err)
	}
	if seen != 100 {
		t.Fatalf("second run started at %d, want 100", seen)
	}

	var n int
	if err := d.QueryRow(`SELECT COUNT(*) FROM test_records`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 200 {
		t.Fatalf("rows = %d, want 200", n)
	}
}

func TestRunEnforcesMaxRecords(t *testing.T) {
	ctx := context.Background()
	d := testDB(t)

	src := &fakeSource{name: "test", collect: func(ctx context.Context, env *source.Environment) error {
		return emitRange(env, ctx, 0, 1000)
	}}

	opts := testOptions()
	opts.MaxRecords = 250
	res, err := Run(ctx, d, src, opts)
	if err != nil {
		t.Fatal(err)
	}
	if res.Stats.Inserted != 250 {
		t.Fatalf("inserted = %d, want 250", res.Stats.Inserted)
	}
	if res.Status != "completed" {
		t.Fatalf("status = %s", res.Status)
	}
}

func TestRunRecordsErrorsWithoutFailing(t *testing.T) {
	ctx := context.Background()
	d := testDB(t)

	src := &fakeSource{name: "test", collect: func(ctx context.Context, env *source.Environment) error {
		env.Error(ctx, errors.New("boom"), "unit test")
		return emitRange(env, ctx, 0, 10)
	}}

	res, err := Run(ctx, d, src, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != "completed" {
		t.Fatalf("status = %s", res.Status)
	}

	var n int
	var msg string
	if err := d.QueryRow(`SELECT COUNT(*), COALESCE(MAX(error),'') FROM _errors WHERE run_id=?`, res.RunID).Scan(&n, &msg); err != nil {
		t.Fatal(err)
	}
	if n != 1 || msg != "boom" {
		t.Fatalf("errors: n=%d msg=%q", n, msg)
	}
}

func TestRunCancellationCommitsPartialWork(t *testing.T) {
	d := testDB(t)
	ctx, cancel := context.WithCancel(context.Background())

	src := &fakeSource{name: "test", collect: func(ctx context.Context, env *source.Environment) error {
		if err := emitRange(env, ctx, 0, 100); err != nil {
			return err
		}
		if err := env.SaveState(ctx, map[string]int{"seq": 100}); err != nil {
			return err
		}
		cancel()
		return emitRange(env, ctx, 100, 100000)
	}}

	res, err := Run(ctx, d, src, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != "cancelled" {
		t.Fatalf("status = %s, want cancelled", res.Status)
	}
	var n int
	if err := d.QueryRow(`SELECT COUNT(*) FROM test_records`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n < 100 {
		t.Fatalf("expected the committed batch to survive cancellation, got %d rows", n)
	}
	state, err := LoadState(context.Background(), d, "test")
	if err != nil || string(state) != `{"seq":100}` {
		t.Fatalf("state = %s err = %v", state, err)
	}
}
