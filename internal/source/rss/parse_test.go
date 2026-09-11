package rss

import (
	"strings"
	"testing"
)

const rss2 = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/" xmlns:dc="http://purl.org/dc/elements/1.1/">
<channel>
  <title>Example News</title>
  <link>https://example.com</link>
  <description>Headlines &amp; more</description>
  <language>en-us</language>
  <item>
    <title>First &nbsp;post</title>
    <link>https://example.com/1</link>
    <guid isPermaLink="false">tag:example.com,2026:1</guid>
    <pubDate>Mon, 02 Feb 2026 15:04:05 -0500</pubDate>
    <description>Summary one</description>
    <content:encoded><![CDATA[<p>Body one</p>]]></content:encoded>
    <dc:creator>Ada</dc:creator>
    <category>tech</category>
    <category>news</category>
  </item>
  <item>
    <title>Second post</title>
    <link>https://example.com/2</link>
    <pubDate>Tue, 03 Feb 2026 10:00:00 GMT</pubDate>
  </item>
</channel>
</rss>`

const atom = `<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Atom Example</title>
  <subtitle>a subtitle</subtitle>
  <link rel="self" href="https://example.org/feed"/>
  <link rel="alternate" href="https://example.org/"/>
  <updated>2026-02-02T18:30:02Z</updated>
  <entry>
    <title>Atom entry</title>
    <link rel="alternate" href="https://example.org/post"/>
    <id>urn:uuid:1225c695</id>
    <published>2026-02-01T18:30:02Z</published>
    <updated>2026-02-02T18:30:02Z</updated>
    <summary>Some text.</summary>
    <author><name>Grace</name></author>
    <category term="go"/>
  </entry>
</feed>`

const rdf = `<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns="http://purl.org/rss/1.0/" xmlns:dc="http://purl.org/dc/elements/1.1/">
  <channel rdf:about="https://rdf.example/">
    <title>RDF Feed</title>
    <link>https://rdf.example/</link>
  </channel>
  <item rdf:about="https://rdf.example/a">
    <title>RDF item</title>
    <link>https://rdf.example/a</link>
    <dc:date>2026-02-02T00:00:00Z</dc:date>
  </item>
</rdf:RDF>`

func TestParseRSS2(t *testing.T) {
	f, err := Parse(strings.NewReader(rss2))
	if err != nil {
		t.Fatal(err)
	}
	if f.Title != "Example News" || f.Link != "https://example.com" || f.Language != "en-us" {
		t.Fatalf("channel metadata: %+v", f)
	}
	if len(f.Items) != 2 {
		t.Fatalf("items = %d", len(f.Items))
	}

	it := f.Items[0]
	if it.GUID != "tag:example.com,2026:1" {
		t.Fatalf("guid = %q", it.GUID)
	}
	if it.URL != "https://example.com/1" {
		t.Fatalf("url = %q", it.URL)
	}
	if it.Author != "Ada" {
		t.Fatalf("author = %q", it.Author)
	}
	if it.Content != "<p>Body one</p>" {
		t.Fatalf("content = %q", it.Content)
	}
	if it.Summary != "Summary one" {
		t.Fatalf("summary = %q", it.Summary)
	}
	if strings.Join(it.Categories, ",") != "tech,news" {
		t.Fatalf("categories = %v", it.Categories)
	}
	if it.PublishedAt.UTC().Format("2006-01-02T15:04:05Z") != "2026-02-02T20:04:05Z" {
		t.Fatalf("published = %s", it.PublishedAt)
	}
	if f.Items[1].GUID != "https://example.com/2" {
		t.Fatalf("guid should fall back to the link, got %q", f.Items[1].GUID)
	}
}

func TestParseAtom(t *testing.T) {
	f, err := Parse(strings.NewReader(atom))
	if err != nil {
		t.Fatal(err)
	}
	if f.Title != "Atom Example" || f.Description != "a subtitle" {
		t.Fatalf("feed metadata: %+v", f)
	}
	if f.Link != "https://example.org/" {
		t.Fatalf("rel=alternate should win, got %q", f.Link)
	}
	if len(f.Items) != 1 {
		t.Fatalf("items = %d", len(f.Items))
	}
	it := f.Items[0]
	if it.GUID != "urn:uuid:1225c695" || it.URL != "https://example.org/post" {
		t.Fatalf("entry identity: %+v", it)
	}
	if it.Author != "Grace" || strings.Join(it.Categories, ",") != "go" {
		t.Fatalf("entry metadata: %+v", it)
	}
	if it.PublishedAt.IsZero() || it.UpdatedAt.IsZero() {
		t.Fatalf("timestamps: %+v", it)
	}
}

func TestParseRDF(t *testing.T) {
	f, err := Parse(strings.NewReader(rdf))
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Items) != 1 || f.Items[0].URL != "https://rdf.example/a" {
		t.Fatalf("items = %+v", f.Items)
	}
	if f.Items[0].PublishedAt.IsZero() {
		t.Fatal("dc:date should be parsed")
	}
}

func TestParseLatin1(t *testing.T) {
	body := "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><rss version=\"2.0\"><channel><title>Caf\xe9</title>" +
		"<item><title>Cr\xe8me</title><link>https://e/1</link></item></channel></rss>"
	f, err := Parse(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if f.Title != "Café" {
		t.Fatalf("title = %q", f.Title)
	}
	if f.Items[0].Title != "Crème" {
		t.Fatalf("item title = %q", f.Items[0].Title)
	}
}

func TestParseRejectsNonFeed(t *testing.T) {
	if _, err := Parse(strings.NewReader("not xml at all")); err == nil {
		t.Fatal("expected an error")
	}
}

func TestParseTimeFormats(t *testing.T) {
	for _, v := range []string{
		"Mon, 02 Feb 2026 15:04:05 -0500",
		"2026-02-02T15:04:05Z",
		"2026-02-02 15:04:05",
		"2026-02-02",
	} {
		if parseTime(v).IsZero() {
			t.Errorf("failed to parse %q", v)
		}
	}
	if !parseTime("whenever").IsZero() {
		t.Error("unparseable input should stay zero")
	}
}
