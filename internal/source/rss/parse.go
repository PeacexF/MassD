package rss

import (
	"encoding/xml"
	"fmt"
	"io"
	"strings"
	"time"
)

type Feed struct {
	Title       string
	Link        string
	Description string
	Language    string
	Updated     string
	Items       []Item
}

type Item struct {
	GUID        string
	URL         string
	Title       string
	Author      string
	Summary     string
	Content     string
	Categories  []string
	PublishedAt time.Time
	UpdatedAt   time.Time
}

// rawFeed covers RSS 2.0, RSS 1.0/RDF and Atom in one shape. Field tags match
// local names, so namespace prefixes (dc:, content:) resolve without a map.
type rawFeed struct {
	XMLName xml.Name
	Channel struct {
		Title       string    `xml:"title"`
		Description string    `xml:"description"`
		Language    string    `xml:"language"`
		Updated     string    `xml:"lastBuildDate"`
		Items       []rawItem `xml:"item"`
		Links       []link    `xml:"link"`
	} `xml:"channel"`
	RDFItems []rawItem `xml:"item"`
	Title    string    `xml:"title"`
	Subtitle string    `xml:"subtitle"`
	Updated  string    `xml:"updated"`
	Links    []link    `xml:"link"`
	Entries  []rawItem `xml:"entry"`
}

// link covers both forms: RSS puts the URL in the element text, Atom in href.
type link struct {
	Href string `xml:"href,attr"`
	Rel  string `xml:"rel,attr"`
	Type string `xml:"type,attr"`
	Text string `xml:",chardata"`
}

func (l link) url() string {
	if l.Href != "" {
		return l.Href
	}
	return strings.TrimSpace(l.Text)
}

type category struct {
	Term string `xml:"term,attr"`
	Text string `xml:",chardata"`
}

type author struct {
	Name string `xml:"name"`
	Text string `xml:",chardata"`
}

type rawItem struct {
	Title       string     `xml:"title"`
	Links       []link     `xml:"link"`
	GUID        string     `xml:"guid"`
	ID          string     `xml:"id"`
	Description string     `xml:"description"`
	Summary     string     `xml:"summary"`
	Encoded     string     `xml:"encoded"`
	Content     string     `xml:"content"`
	PubDate     string     `xml:"pubDate"`
	Date        string     `xml:"date"`
	Published   string     `xml:"published"`
	Updated     string     `xml:"updated"`
	Author      author     `xml:"author"`
	Creator     string     `xml:"creator"`
	Categories  []category `xml:"category"`
}

func Parse(r io.Reader) (*Feed, error) {
	dec := xml.NewDecoder(r)
	// Real-world feeds are frequently malformed and full of HTML entities.
	// HTMLAutoClose is deliberately not set: it treats <link> as a void element
	// and swallows the rest of an RSS channel.
	dec.Strict = false
	dec.Entity = xml.HTMLEntity
	dec.CharsetReader = charsetReader

	var raw rawFeed
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("parse feed: %w", err)
	}

	f := &Feed{
		Title:       strings.TrimSpace(raw.Channel.Title),
		Link:        pickLink(raw.Channel.Links),
		Description: strings.TrimSpace(raw.Channel.Description),
		Language:    strings.TrimSpace(raw.Channel.Language),
		Updated:     strings.TrimSpace(raw.Channel.Updated),
	}
	if f.Title == "" {
		f.Title = strings.TrimSpace(raw.Title)
	}
	if f.Description == "" {
		f.Description = strings.TrimSpace(raw.Subtitle)
	}
	if f.Updated == "" {
		f.Updated = strings.TrimSpace(raw.Updated)
	}
	if f.Link == "" {
		f.Link = pickLink(raw.Links)
	}

	items := raw.Channel.Items
	items = append(items, raw.RDFItems...)
	items = append(items, raw.Entries...)

	f.Items = make([]Item, 0, len(items))
	for _, it := range items {
		f.Items = append(f.Items, normalize(it))
	}
	return f, nil
}

func normalize(it rawItem) Item {
	out := Item{
		Title:   strings.TrimSpace(it.Title),
		Summary: strings.TrimSpace(firstNonEmpty(it.Description, it.Summary)),
		Content: strings.TrimSpace(firstNonEmpty(it.Encoded, it.Content)),
		Author:  strings.TrimSpace(firstNonEmpty(it.Author.Name, it.Creator, it.Author.Text)),
	}

	out.URL = pickLink(it.Links)
	out.GUID = strings.TrimSpace(firstNonEmpty(it.GUID, it.ID, out.URL))
	out.PublishedAt = parseTime(firstNonEmpty(it.PubDate, it.Published, it.Date))
	out.UpdatedAt = parseTime(firstNonEmpty(it.Updated, it.Date))

	for _, c := range it.Categories {
		if name := strings.TrimSpace(firstNonEmpty(c.Term, c.Text)); name != "" {
			out.Categories = append(out.Categories, name)
		}
	}
	return out
}

func pickLink(links []link) string {
	var fallback string
	for _, l := range links {
		u := l.url()
		if u == "" {
			continue
		}
		if l.Rel == "alternate" || l.Rel == "" {
			return u
		}
		if fallback == "" && l.Rel != "self" && l.Rel != "hub" {
			fallback = u
		}
	}
	return fallback
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

var timeLayouts = []string{
	time.RFC1123Z,
	time.RFC1123,
	time.RFC3339,
	time.RFC3339Nano,
	time.RFC822Z,
	time.RFC822,
	"Mon, 2 Jan 2006 15:04:05 -0700",
	"Mon, 2 Jan 2006 15:04:05 MST",
	"Mon, 02 Jan 2006 15:04:05 -0700",
	"2006-01-02T15:04:05-07:00",
	"2006-01-02T15:04:05Z0700",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02",
	"January 2, 2006",
	"2 January 2006",
}

func parseTime(v string) time.Time {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}
	}
	for _, layout := range timeLayouts {
		if t, err := time.Parse(layout, v); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

// charsetReader handles the single-byte encodings still common in feeds.
// Anything else is rejected rather than silently mangled.
func charsetReader(label string, input io.Reader) (io.Reader, error) {
	switch strings.ToLower(strings.TrimSpace(label)) {
	case "", "utf-8", "utf8", "us-ascii", "ascii":
		return input, nil
	case "iso-8859-1", "iso8859-1", "latin1", "latin-1", "windows-1252", "cp1252":
		return &latin1Reader{r: input}, nil
	}
	return nil, fmt.Errorf("unsupported charset %q", label)
}

type latin1Reader struct {
	r   io.Reader
	buf []byte
}

func (l *latin1Reader) Read(p []byte) (int, error) {
	if len(l.buf) == 0 {
		// Each input byte expands to at most two UTF-8 bytes.
		src := make([]byte, len(p)/2+1)
		n, err := l.r.Read(src)
		if n == 0 {
			return 0, err
		}
		for _, b := range src[:n] {
			l.buf = append(l.buf, []byte(string(rune(b)))...)
		}
	}
	n := copy(p, l.buf)
	l.buf = l.buf[n:]
	return n, nil
}
