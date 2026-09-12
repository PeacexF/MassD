package commoncrawl

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/PeacexF/MassD/internal/record"
)

// cdxLine is one line of a Common Crawl CDX shard:
//
//	com,example)/path 20260202012345 {"url":"...","status":"200",...}
type cdxLine struct {
	SURT  string
	Stamp string
	Meta  cdxMeta
}

type cdxMeta struct {
	URL          string `json:"url"`
	MIME         string `json:"mime"`
	MIMEDetected string `json:"mime-detected"`
	Status       string `json:"status"`
	Digest       string `json:"digest"`
	Length       string `json:"length"`
	Offset       string `json:"offset"`
	Filename     string `json:"filename"`
	Charset      string `json:"charset"`
	Languages    string `json:"languages"`
	Redirect     string `json:"redirect"`
}

func parseCDXLine(line string) (cdxLine, bool) {
	surt, rest, ok := strings.Cut(line, " ")
	if !ok {
		return cdxLine{}, false
	}
	stamp, blob, ok := strings.Cut(rest, " ")
	if !ok || len(blob) == 0 || blob[0] != '{' {
		return cdxLine{}, false
	}

	var meta cdxMeta
	if err := json.Unmarshal([]byte(blob), &meta); err != nil {
		return cdxLine{}, false
	}
	if meta.URL == "" {
		return cdxLine{}, false
	}
	return cdxLine{SURT: surt, Stamp: stamp, Meta: meta}, true
}

func (c cdxLine) record(crawl, now string, runID int64) record.Record {
	return record.New("commoncrawl_index").
		Set("crawl", crawl).
		Set("surt", c.SURT).
		Set("url", c.Meta.URL).
		Set("stamp", c.Stamp).
		Set("fetched_at", stampTime(c.Stamp)).
		Set("status", number(c.Meta.Status)).
		Set("mime", null(c.Meta.MIME)).
		Set("mime_detected", null(c.Meta.MIMEDetected)).
		Set("charset", null(c.Meta.Charset)).
		Set("languages", null(c.Meta.Languages)).
		Set("digest", null(c.Meta.Digest)).
		Set("length", number(c.Meta.Length)).
		Set("offset", number(c.Meta.Offset)).
		Set("filename", null(c.Meta.Filename)).
		Set("collected_at", now).
		Set("run_id", runID).
		OnConflict(record.Ignore)
}

func stampTime(stamp string) any {
	if len(stamp) != 14 {
		return nil
	}
	t, err := time.Parse("20060102150405", stamp)
	if err != nil {
		return nil
	}
	return t.UTC().Format(time.RFC3339)
}

func number(s string) any {
	if s == "" {
		return nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil
	}
	return n
}

func null(s string) any {
	if s == "" {
		return nil
	}
	return s
}
