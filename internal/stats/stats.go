// Package stats holds the counters every run reports. Data acquisition
// throughput is the project's primary metric, so it is measured directly.
package stats

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

type Stats struct {
	Fetched    atomic.Int64
	Inserted   atomic.Int64
	Duplicates atomic.Int64
	Skipped    atomic.Int64
	Errors     atomic.Int64
	Requests   atomic.Int64
	Bytes      atomic.Int64

	start time.Time
	end   atomic.Int64
}

func New() *Stats { return &Stats{start: time.Now()} }

func (s *Stats) Finish() { s.end.CompareAndSwap(0, time.Now().UnixNano()) }

func (s *Stats) Duration() time.Duration {
	if e := s.end.Load(); e != 0 {
		return time.Unix(0, e).Sub(s.start)
	}
	return time.Since(s.start)
}

type Snapshot struct {
	Fetched, Inserted, Duplicates, Skipped, Errors, Requests, Bytes int64
	Duration                                                        time.Duration
}

func (s *Stats) Snapshot() Snapshot {
	return Snapshot{
		Fetched:    s.Fetched.Load(),
		Inserted:   s.Inserted.Load(),
		Duplicates: s.Duplicates.Load(),
		Skipped:    s.Skipped.Load(),
		Errors:     s.Errors.Load(),
		Requests:   s.Requests.Load(),
		Bytes:      s.Bytes.Load(),
		Duration:   s.Duration(),
	}
}

func (s Snapshot) Report(source string) string {
	secs := s.Duration.Seconds()
	if secs <= 0 {
		secs = 0.001
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Source: %s\n", source)
	fmt.Fprintf(&b, "Duration: %s\n\n", formatDuration(s.Duration))
	fmt.Fprintf(&b, "Requests:     %12s\n", Commas(s.Requests))
	fmt.Fprintf(&b, "Downloaded:   %12s\n\n", Bytes(s.Bytes))
	fmt.Fprintf(&b, "Records:\n")
	fmt.Fprintf(&b, "  fetched:    %12s\n", Commas(s.Fetched))
	fmt.Fprintf(&b, "  inserted:   %12s\n", Commas(s.Inserted))
	fmt.Fprintf(&b, "  duplicates: %12s\n", Commas(s.Duplicates))
	fmt.Fprintf(&b, "  skipped:    %12s\n", Commas(s.Skipped))
	fmt.Fprintf(&b, "  errors:     %12s\n\n", Commas(s.Errors))
	fmt.Fprintf(&b, "Throughput:\n")
	fmt.Fprintf(&b, "  %s records/sec\n", Commas(int64(float64(s.Inserted)/secs)))
	fmt.Fprintf(&b, "  %.2f MB/sec\n", float64(s.Bytes)/secs/(1<<20))
	return b.String()
}

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	return fmt.Sprintf("%02d:%02d:%02d", int(d.Hours()), int(d.Minutes())%60, int(d.Seconds())%60)
}

func Commas(n int64) string {
	s := fmt.Sprintf("%d", n)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}

func Bytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit && exp < 4; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(n)/float64(div), "KMGTP"[exp])
}
