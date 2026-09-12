package warc

import (
	"bytes"
	"compress/gzip"
	"io"
	"strings"
	"testing"
)

func recordBytes(uri, payload string) string {
	http := "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: " +
		itoa(len(payload)) + "\r\n\r\n" + payload
	return "WARC/1.0\r\n" +
		"WARC-Type: response\r\n" +
		"WARC-Target-URI: " + uri + "\r\n" +
		"WARC-Date: 2026-02-02T00:00:00Z\r\n" +
		"WARC-Payload-Digest: sha1:ABC\r\n" +
		"Content-Type: application/http; msgtype=response\r\n" +
		"Content-Length: " + itoa(len(http)) + "\r\n" +
		"\r\n" + http + "\r\n\r\n"
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func TestReadResponseRecord(t *testing.T) {
	r := NewReader(strings.NewReader(recordBytes("https://example.com/", "<html>hi</html>")))

	rec, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if rec.Type() != TypeResponse {
		t.Fatalf("type = %q", rec.Type())
	}
	if rec.TargetURI() != "https://example.com/" {
		t.Fatalf("uri = %q", rec.TargetURI())
	}
	if rec.Digest() != "sha1:ABC" {
		t.Fatalf("digest = %q", rec.Digest())
	}

	resp, err := rec.HTTPResponse()
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "text/html" {
		t.Fatalf("response = %+v", resp)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "<html>hi</html>" {
		t.Fatalf("body = %q", body)
	}
}

func TestReadMultipleRecords(t *testing.T) {
	stream := recordBytes("https://a.example/", "one") + recordBytes("https://b.example/", "two")
	r := NewReader(strings.NewReader(stream))

	var uris []string
	for {
		rec, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		uris = append(uris, rec.TargetURI())
	}
	if len(uris) != 2 || uris[1] != "https://b.example/" {
		t.Fatalf("uris = %v", uris)
	}
}

func TestSkipsUnreadContent(t *testing.T) {
	// The second record must be found even though the first is never read.
	stream := recordBytes("https://a.example/", strings.Repeat("x", 5000)) +
		recordBytes("https://b.example/", "two")
	r := NewReader(strings.NewReader(stream))

	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	rec, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if rec.TargetURI() != "https://b.example/" {
		t.Fatalf("uri = %q", rec.TargetURI())
	}
}

func TestGzippedRecord(t *testing.T) {
	// Common Crawl stores one gzip member per record, which is what a ranged
	// request returns.
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	io.WriteString(zw, recordBytes("https://example.com/gz", "<p>compressed</p>"))
	zw.Close()

	zr, err := gzip.NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := NewReader(zr).Next()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := rec.HTTPResponse()
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "<p>compressed</p>" {
		t.Fatalf("body = %q", body)
	}
}

func TestRejectsGarbage(t *testing.T) {
	if _, err := NewReader(strings.NewReader("this is not a warc file\r\n")).Next(); err == nil {
		t.Fatal("expected an error")
	}
}

func TestNonResponseRecordCannotBeParsedAsHTTP(t *testing.T) {
	stream := "WARC/1.0\r\nWARC-Type: warcinfo\r\nContent-Length: 4\r\n\r\nhttp\r\n\r\n"
	rec, err := NewReader(strings.NewReader(stream)).Next()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rec.HTTPResponse(); err == nil {
		t.Fatal("expected a type error")
	}
}
