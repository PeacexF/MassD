package htmlx

import (
	"net/url"
	"strings"
	"testing"
)

const page = `<!doctype html>
<html lang="en-GB">
<head>
  <title>  Example
     Page </title>
  <meta name="description" content="A page   about things">
  <link rel="canonical" href="/canonical?a=1#frag">
  <style>body{color:red}</style>
  <script>var x = "not text";</script>
</head>
<body>
  <h1>Heading</h1>
  <p>Some visible text.</p>
  <a href="/one" rel="nofollow">First link</a>
  <a href="https://elsewhere.example/two">Second</a>
  <a href="#anchor">ignored</a>
  <a href="javascript:void(0)">ignored too</a>
  <noscript>hidden</noscript>
</body>
</html>`

func extract(t *testing.T, body string, opts Options) *Doc {
	t.Helper()
	doc, err := Extract(strings.NewReader(body), opts)
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func TestExtractMetadata(t *testing.T) {
	base, _ := url.Parse("https://example.com/dir/page.html")
	doc := extract(t, page, Options{Base: base, CollectLinks: true})

	if doc.Title != "Example Page" {
		t.Fatalf("title = %q", doc.Title)
	}
	if doc.Description != "A page about things" {
		t.Fatalf("description = %q", doc.Description)
	}
	if doc.Language != "en-GB" {
		t.Fatalf("language = %q", doc.Language)
	}
	if doc.Canonical != "https://example.com/canonical?a=1" {
		t.Fatalf("canonical = %q", doc.Canonical)
	}
}

func TestExtractSkipsScriptAndStyle(t *testing.T) {
	doc := extract(t, page, Options{})
	for _, unwanted := range []string{"color:red", "not text", "hidden"} {
		if strings.Contains(doc.Text, unwanted) {
			t.Fatalf("text contains %q: %s", unwanted, doc.Text)
		}
	}
	if !strings.Contains(doc.Text, "Some visible text.") {
		t.Fatalf("text = %q", doc.Text)
	}
}

func TestExtractLinks(t *testing.T) {
	base, _ := url.Parse("https://example.com/dir/page.html")
	doc := extract(t, page, Options{Base: base, CollectLinks: true})

	if len(doc.Links) != 2 {
		t.Fatalf("links = %+v", doc.Links)
	}
	if doc.Links[0].Href != "https://example.com/one" || doc.Links[0].Rel != "nofollow" {
		t.Fatalf("first link = %+v", doc.Links[0])
	}
	if doc.Links[0].Text != "First link" {
		t.Fatalf("anchor text = %q", doc.Links[0].Text)
	}
	if doc.Links[1].Href != "https://elsewhere.example/two" {
		t.Fatalf("second link = %+v", doc.Links[1])
	}
}

func TestExtractHonoursTextLimit(t *testing.T) {
	body := "<html><body>" + strings.Repeat("word ", 10000) + "</body></html>"
	doc := extract(t, body, Options{MaxTextBytes: 100})

	if len(doc.Text) > 200 {
		t.Fatalf("retained %d bytes despite a 100 byte limit", len(doc.Text))
	}
	if doc.TextLength < 50000 {
		t.Fatalf("TextLength = %d, should count past the cap", doc.TextLength)
	}
}

func TestExtractHandlesBrokenMarkup(t *testing.T) {
	doc := extract(t, `<html><head><title>Broken</title><body><p>text<a href="/x">link`,
		Options{CollectLinks: true})
	if doc.Title != "Broken" || !strings.Contains(doc.Text, "text") {
		t.Fatalf("doc = %+v", doc)
	}
	if len(doc.Links) != 1 || doc.Links[0].Href != "/x" {
		t.Fatalf("links = %+v", doc.Links)
	}
}

func TestExtractCapsUnclosedTitle(t *testing.T) {
	// <title> is RCDATA: without a closing tag the whole document is the title.
	doc := extract(t, "<html><head><title>"+strings.Repeat("x", 100000), Options{})
	if len(doc.Title) > maxTitleBytes+1024 {
		t.Fatalf("title grew to %d bytes", len(doc.Title))
	}
}

func TestExtractOpenGraphFallbacks(t *testing.T) {
	doc := extract(t, `<html><head>
		<meta property="og:title" content="OG Title">
		<meta property="og:description" content="OG description">
		</head><body>x</body></html>`, Options{})
	if doc.Title != "OG Title" || doc.Description != "OG description" {
		t.Fatalf("doc = %+v", doc)
	}
}

func TestExtractLinksDisabled(t *testing.T) {
	if doc := extract(t, page, Options{}); len(doc.Links) != 0 {
		t.Fatalf("links collected without CollectLinks: %+v", doc.Links)
	}
}
