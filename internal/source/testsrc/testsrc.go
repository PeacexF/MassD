// Package testsrc is a synthetic source: no network, deterministic output. It
// exists to verify batching, dedup, resumption and crash safety.
package testsrc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/PeacexF/MassD/internal/db"
	"github.com/PeacexF/MassD/internal/record"
	"github.com/PeacexF/MassD/internal/source"
)

type Source struct{}

type state struct {
	Seq int64 `json:"seq"`
}

func init() { source.Register(&Source{}) }

func (s *Source) Name() string { return "test" }

func (s *Source) Describe() string {
	return "synthetic records for verifying the pipeline (no network)"
}

func (s *Source) Collect(ctx context.Context, env *source.Environment) error {
	count := int64(env.Config.Int("count", 100000))
	checkpoint := int64(env.Config.Int("checkpoint_every", 10000))
	if checkpoint <= 0 {
		checkpoint = 10000
	}
	delay := env.Config.Duration("delay", 0)

	var st state
	if err := env.LoadState(&st); err != nil {
		return err
	}

	start := st.Seq
	env.Log.Info("generating synthetic records", "from", start, "count", count)

	batch := make([]record.Record, 0, 256)
	for i := start; i < start+count; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		payload := fmt.Sprintf("synthetic record %d", i)
		sum := sha256.Sum256([]byte(payload))

		batch = append(batch, record.New("test_records").
			Set("seq", i).
			Set("payload", payload).
			Set("hash", hex.EncodeToString(sum[:])).
			Set("run_id", env.RunID).
			Set("collected_at", db.Now()).
			OnConflict(record.Ignore))

		if len(batch) == cap(batch) {
			if err := env.Emit(ctx, batch...); err != nil {
				return err
			}
			batch = batch[:0]
		}
		if (i+1)%checkpoint == 0 {
			if len(batch) > 0 {
				if err := env.Emit(ctx, batch...); err != nil {
					return err
				}
				batch = batch[:0]
			}
			if err := env.SaveState(ctx, state{Seq: i + 1}); err != nil {
				return err
			}
		}
		if delay > 0 {
			time.Sleep(delay)
		}
	}

	if len(batch) > 0 {
		if err := env.Emit(ctx, batch...); err != nil {
			return err
		}
	}
	return env.SaveState(ctx, state{Seq: start + count})
}
