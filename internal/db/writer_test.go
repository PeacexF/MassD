package db

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/PeacexF/MassD/internal/record"
	"github.com/PeacexF/MassD/internal/stats"
)

func testDB(t *testing.T) *DB {
	t.Helper()
	d, err := Open(filepath.Join(t.TempDir(), "test.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	if _, err := d.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestWriterBatchesAndDeduplicates(t *testing.T) {
	ctx := context.Background()
	d := testDB(t)
	st := stats.New()
	w := NewWriter(d, "test", 1000, st)
	defer w.Close()

	for i := range 2500 {
		r := record.New("test_records").
			Set("seq", i%2000).
			Set("payload", "p").
			Set("hash", "h").
			Set("run_id", 1).
			Set("collected_at", Now()).
			OnConflict(record.Ignore)
		if err := w.Add(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	var n int
	if err := d.QueryRow(`SELECT COUNT(*) FROM test_records`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2000 {
		t.Fatalf("rows = %d, want 2000", n)
	}
	if got := st.Inserted.Load(); got != 2000 {
		t.Fatalf("inserted = %d, want 2000", got)
	}
	if got := st.Duplicates.Load(); got != 500 {
		t.Fatalf("duplicates = %d, want 500", got)
	}
}

func TestWriterCommitsStateWithRecords(t *testing.T) {
	ctx := context.Background()
	d := testDB(t)
	w := NewWriter(d, "test", 10, nil)
	defer w.Close()

	w.SetState(`{"seq":7}`)
	if err := w.Add(ctx, record.New("test_records").
		Set("seq", 1).Set("payload", "p").Set("hash", "h").
		Set("run_id", 1).Set("collected_at", Now())); err != nil {
		t.Fatal(err)
	}

	var state string
	err := d.QueryRow(`SELECT state FROM _source_state WHERE source='test'`).Scan(&state)
	if err == nil {
		t.Fatal("state must not be visible before the batch commits")
	}

	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := d.QueryRow(`SELECT state FROM _source_state WHERE source='test'`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != `{"seq":7}` {
		t.Fatalf("state = %s", state)
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	d := testDB(t)
	n, err := d.Migrate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("second migrate applied %d migrations", n)
	}
}

func TestWideRecordExceedsNoVariableLimit(t *testing.T) {
	ctx := context.Background()
	d := testDB(t)
	if _, err := d.ExecContext(ctx, `CREATE TABLE wide (a TEXT, b TEXT, c TEXT)`); err != nil {
		t.Fatal(err)
	}
	w := NewWriter(d, "test", 100000, nil)
	defer w.Close()

	for range 20000 {
		if err := w.Add(ctx, record.New("wide").Set("a", "1").Set("b", "2").Set("c", "3")); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := d.QueryRow(`SELECT COUNT(*) FROM wide`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 20000 {
		t.Fatalf("rows = %d", n)
	}
}
