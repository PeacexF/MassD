package collect

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/PeacexF/MassD/internal/db"
)

func LoadState(ctx context.Context, d *db.DB, source string) (json.RawMessage, error) {
	var state sql.NullString
	err := d.QueryRowContext(ctx, `SELECT state FROM _source_state WHERE source = ?`, source).Scan(&state)
	switch {
	case err == sql.ErrNoRows:
		return nil, nil
	case err != nil:
		return nil, err
	case !state.Valid:
		return nil, nil
	}
	return json.RawMessage(state.String), nil
}

func ClearState(ctx context.Context, d *db.DB, source string) error {
	_, err := d.ExecContext(ctx, `DELETE FROM _source_state WHERE source = ?`, source)
	return err
}

type RunInfo struct {
	ID         int64
	Source     string
	StartedAt  string
	FinishedAt string
	Status     string
	Records    int64
	Errors     int64
	Duplicates int64
	Requests   int64
	Bytes      int64
	State      string
}

// LastRuns returns the most recent run per source, joined with its saved state.
func LastRuns(ctx context.Context, d *db.DB) ([]RunInfo, error) {
	rows, err := d.QueryContext(ctx, `
		SELECT r.id, r.source, r.started_at, COALESCE(r.finished_at,''), r.status,
		       r.records, r.errors, r.duplicates, r.requests, r.bytes,
		       COALESCE(s.state,'')
		FROM _runs r
		JOIN (SELECT source, MAX(id) AS id FROM _runs GROUP BY source) last
		  ON last.id = r.id
		LEFT JOIN _source_state s ON s.source = r.source
		ORDER BY r.source`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []RunInfo
	for rows.Next() {
		var r RunInfo
		if err := rows.Scan(&r.ID, &r.Source, &r.StartedAt, &r.FinishedAt, &r.Status,
			&r.Records, &r.Errors, &r.Duplicates, &r.Requests, &r.Bytes, &r.State); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
