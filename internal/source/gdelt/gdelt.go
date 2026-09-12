// Package gdelt collects the GDELT 2.0 event and mention streams. GDELT
// publishes one zipped, tab-separated file per dataset every 15 minutes, which
// makes it high volume and cheap to ingest incrementally.
package gdelt

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"

	"github.com/PeacexF/MassD/internal/db"
	"github.com/PeacexF/MassD/internal/fetch"
	"github.com/PeacexF/MassD/internal/record"
	"github.com/PeacexF/MassD/internal/source"
)

const defaultBase = "http://data.gdeltproject.org/gdeltv2"

type Source struct{}

func init() { source.Register(&Source{}) }

func (s *Source) Name() string { return "gdelt" }

func (s *Source) Describe() string { return "GDELT 2.0 events and mentions (15-minute files)" }

type state struct {
	LastStamp string `json:"last_stamp"`
}

type fileRef struct {
	URL   string
	Kind  string
	Stamp string
}

func (s *Source) Collect(ctx context.Context, env *source.Environment) error {
	base := strings.TrimSuffix(env.Config.String("base_url", defaultBase), "/")
	mode := env.Config.String("mode", "latest")
	wanted := env.Config.Strings("datasets")
	if len(wanted) == 0 {
		wanted = []string{"export", "mentions"}
	}

	var st state
	if err := env.LoadState(&st); err != nil {
		return err
	}

	client := env.HTTP.WithOverrides(func(o *fetch.Options) {
		o.MaxBodyBytes = int64(env.Config.Int("max_file_bytes", 512<<20))
	})

	files, err := s.discover(ctx, env, client, base, mode, st)
	if err != nil {
		return err
	}
	files = filterFiles(files, wanted, st.LastStamp, env.Config.Int("max_files", 0))
	if len(files) == 0 {
		env.Log.Info("no new gdelt files")
		return nil
	}
	env.Log.Info("gdelt files selected", "count", len(files), "mode", mode, "from", files[0].Stamp)

	workers := max(min(env.Opts.Workers, 8), 1)
	for job := range prefetch(ctx, client, files, workers) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if job.err != nil {
			env.Error(ctx, job.err, "download "+job.ref.URL)
			continue
		}
		if seen, err := alreadyCollected(ctx, env, job.ref.URL); err == nil && seen {
			env.Log.Debug("file already collected", "url", job.ref.URL)
			continue
		}
		if err := s.ingest(ctx, env, job.ref, job.data); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			env.Error(ctx, err, "ingest "+job.ref.URL)
			continue
		}
		if job.ref.Stamp > st.LastStamp {
			st.LastStamp = job.ref.Stamp
		}
		if err := env.SaveState(ctx, st); err != nil {
			return err
		}
	}
	return nil
}

func (s *Source) discover(ctx context.Context, env *source.Environment, client *fetch.Client, base, mode string, st state) ([]fileRef, error) {
	listURL := base + "/lastupdate.txt"
	if mode == "backfill" {
		listURL = base + "/masterfilelist.txt"
	}

	resp, err := client.Get(ctx, listURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	since, until := env.Window()
	var sinceStamp, untilStamp string
	if !since.IsZero() {
		sinceStamp = since.UTC().Format("20060102150405")
	}
	if !env.Opts.Until.IsZero() {
		untilStamp = until.UTC().Format("20060102150405")
	}

	var out []fileRef
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		// Each line is: size hash url
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 {
			continue
		}
		raw := fields[len(fields)-1]
		if !strings.HasSuffix(raw, ".zip") {
			continue
		}
		ref := refFor(raw)
		if ref.Kind == "" || ref.Stamp == "" {
			continue
		}
		if sinceStamp != "" && ref.Stamp < sinceStamp {
			continue
		}
		if untilStamp != "" && ref.Stamp > untilStamp {
			continue
		}
		out = append(out, ref)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", listURL, err)
	}
	return out, nil
}

func refFor(raw string) fileRef {
	u, err := url.Parse(raw)
	if err != nil {
		return fileRef{}
	}
	name := path.Base(u.Path)

	ref := fileRef{URL: raw}
	switch {
	case strings.Contains(name, ".export."):
		ref.Kind = "export"
	case strings.Contains(name, ".mentions."):
		ref.Kind = "mentions"
	case strings.Contains(name, ".gkg."):
		ref.Kind = "gkg"
	default:
		return fileRef{}
	}

	if len(name) >= 14 && isDigits(name[:14]) {
		ref.Stamp = name[:14]
	}
	return ref
}

func isDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(s) > 0
}

func filterFiles(files []fileRef, wanted []string, lastStamp string, maxFiles int) []fileRef {
	want := map[string]bool{}
	for _, w := range wanted {
		want[strings.ToLower(strings.TrimSpace(w))] = true
	}

	var out []fileRef
	for _, f := range files {
		if !want[f.Kind] || f.Stamp <= lastStamp {
			continue
		}
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Stamp != out[j].Stamp {
			return out[i].Stamp < out[j].Stamp
		}
		return out[i].Kind < out[j].Kind
	})
	if maxFiles > 0 && len(out) > maxFiles {
		out = out[:maxFiles]
	}
	return out
}

type downloadJob struct {
	ref  fileRef
	data []byte
	err  error
	done chan struct{}
}

// prefetch downloads ahead of the writer while keeping strict file order, so
// state advances monotonically and a crash resumes at the right file.
func prefetch(ctx context.Context, client *fetch.Client, files []fileRef, workers int) <-chan *downloadJob {
	out := make(chan *downloadJob, workers)
	go func() {
		defer close(out)
		for _, f := range files {
			job := &downloadJob{ref: f, done: make(chan struct{})}
			select {
			case out <- job:
			case <-ctx.Done():
				return
			}
			go func() {
				defer close(job.done)
				job.data, job.err = client.Bytes(ctx, job.ref.URL)
			}()
		}
	}()

	ordered := make(chan *downloadJob, workers)
	go func() {
		defer close(ordered)
		for job := range out {
			<-job.done
			select {
			case ordered <- job:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ordered
}

func alreadyCollected(ctx context.Context, env *source.Environment, fileURL string) (bool, error) {
	var one int
	err := env.DB.QueryRowContext(ctx, `SELECT 1 FROM gdelt_files WHERE url = ?`, fileURL).Scan(&one)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Source) ingest(ctx context.Context, env *source.Environment, ref fileRef, data []byte) error {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("open zip: %w", err)
	}

	now := db.Now()
	var rows int64
	batch := make([]record.Record, 0, 512)

	for _, entry := range zr.File {
		if entry.FileInfo().IsDir() {
			continue
		}
		rc, err := entry.Open()
		if err != nil {
			return err
		}
		err = func() error {
			defer rc.Close()
			sc := bufio.NewScanner(rc)
			sc.Buffer(make([]byte, 0, 256<<10), 16<<20)
			for sc.Scan() {
				line := sc.Text()
				if strings.TrimSpace(line) == "" {
					continue
				}
				cols := strings.Split(line, "\t")

				var rec record.Record
				var ok bool
				switch ref.Kind {
				case "export":
					rec, ok = eventRecord(cols, ref.URL, now, env.RunID)
				case "mentions":
					rec, ok = mentionRecord(cols, ref.URL, now, env.RunID)
				}
				if !ok {
					continue
				}

				batch = append(batch, rec)
				rows++
				if len(batch) == cap(batch) {
					if err := env.Emit(ctx, batch...); err != nil {
						return err
					}
					batch = batch[:0]
				}
			}
			return sc.Err()
		}()
		if err != nil {
			return err
		}
	}

	batch = append(batch, record.New("gdelt_files").
		Set("url", ref.URL).
		Set("kind", ref.Kind).
		Set("stamp", ref.Stamp).
		Set("rows", rows).
		Set("bytes", len(data)).
		Set("collected_at", now).
		Set("run_id", env.RunID).
		OnConflict(record.Replace))

	env.Log.Debug("gdelt file ingested", "url", ref.URL, "rows", rows)
	return env.Emit(ctx, batch...)
}
