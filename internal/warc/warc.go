// Package warc reads WARC 1.x records. Common Crawl stores one gzip member per
// record, so a byte range taken from an index entry is itself a valid gzip
// stream containing exactly one record.
package warc

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
)

const (
	TypeResponse = "response"
	TypeRequest  = "request"
	TypeMetadata = "metadata"
	TypeWarcinfo = "warcinfo"
)

type Record struct {
	Version string
	Header  textproto.MIMEHeader
	// Content is valid until the next call to Next.
	Content io.Reader
	Length  int64
}

func (r *Record) Type() string      { return r.Header.Get("WARC-Type") }
func (r *Record) TargetURI() string { return strings.Trim(r.Header.Get("WARC-Target-URI"), "<>") }
func (r *Record) Date() string      { return r.Header.Get("WARC-Date") }
func (r *Record) Digest() string    { return r.Header.Get("WARC-Payload-Digest") }

// HTTPResponse parses a response record's content block, which is a raw HTTP
// response. The caller must read or close the returned body.
func (r *Record) HTTPResponse() (*http.Response, error) {
	if t := r.Type(); t != TypeResponse {
		return nil, fmt.Errorf("warc: record type %q is not a response", t)
	}
	return http.ReadResponse(bufio.NewReader(r.Content), nil)
}

type Reader struct {
	br   *bufio.Reader
	tp   *textproto.Reader
	body *io.LimitedReader
}

func NewReader(r io.Reader) *Reader {
	br := bufio.NewReaderSize(r, 64<<10)
	return &Reader{br: br, tp: textproto.NewReader(br)}
}

var errBadRecord = errors.New("warc: malformed record")

// Next advances to the following record, discarding any unread content of the
// current one.
func (r *Reader) Next() (*Record, error) {
	if r.body != nil {
		if _, err := io.Copy(io.Discard, r.body); err != nil {
			return nil, err
		}
		r.body = nil
		// Records are terminated by CRLFCRLF.
		if err := r.skipBlankLines(2); err != nil {
			return nil, err
		}
	}

	version, err := r.versionLine()
	if err != nil {
		return nil, err
	}

	header, err := r.tp.ReadMIMEHeader()
	if err != nil {
		return nil, fmt.Errorf("%w: header: %v", errBadRecord, err)
	}
	length, err := strconv.ParseInt(header.Get("Content-Length"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: Content-Length: %v", errBadRecord, err)
	}

	r.body = &io.LimitedReader{R: r.br, N: length}
	return &Record{Version: version, Header: header, Content: r.body, Length: length}, nil
}

func (r *Reader) versionLine() (string, error) {
	for {
		line, err := r.tp.ReadLine()
		if err != nil {
			return "", err
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "WARC/") {
			return "", fmt.Errorf("%w: expected WARC version, got %q", errBadRecord, truncate(line))
		}
		return line, nil
	}
}

func (r *Reader) skipBlankLines(n int) error {
	for range n {
		line, err := r.tp.ReadLine()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if strings.TrimSpace(line) != "" {
			return fmt.Errorf("%w: expected record separator, got %q", errBadRecord, truncate(line))
		}
	}
	return nil
}

func truncate(s string) string {
	if len(s) > 40 {
		return s[:40] + "..."
	}
	return s
}
