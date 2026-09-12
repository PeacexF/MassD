// Package htmlx pulls generic metadata out of HTML: title, description,
// language, canonical URL, links and visible text. It tokenises as it reads, so
// a large document never has to be held in memory at once.
//
// This is deliberately not a semantic parser. Site-specific extraction belongs
// in the source adapter that needs it.
package htmlx

import (
	"io"
	"net/url"
	"strings"
	"unicode"

	"golang.org/x/net/html"
)

type Link struct {
	Href string
	Rel  string
	Text string
}

type Doc struct {
	Title       string
	Description string
	Language    string
	Canonical   string
	Links       []Link
	Text        string
	TextLength  int
}

type Options struct {
	// MaxTextBytes caps retained text. Text is still counted past the cap.
	MaxTextBytes int
	CollectLinks bool
	MaxLinks     int
	// Base resolves relative links; nil leaves hrefs untouched.
	Base *url.URL
}

// maxTitleBytes bounds an unterminated <title>.
const maxTitleBytes = 4096

// skipText covers elements whose contents are never visible prose.
// <head> is absent here on purpose: a page with no </head> would otherwise
// suppress the entire body, and its only text-bearing children are skipped
// individually anyway.
var skipText = map[string]bool{
	"script": true, "style": true, "noscript": true,
	"template": true, "svg": true,
}

func Extract(r io.Reader, opts Options) (*Doc, error) {
	if opts.MaxLinks == 0 {
		opts.MaxLinks = 1000
	}

	doc := &Doc{}
	z := html.NewTokenizer(r)
	var (
		text     strings.Builder
		skip     []string
		inTitle  bool
		inAnchor bool
		anchor   Link
		anchorTx strings.Builder
	)

	for {
		switch z.Next() {
		case html.ErrorToken:
			err := z.Err()
			flushAnchor(doc, &inAnchor, &anchor, &anchorTx, opts.MaxLinks)
			doc.Title = collapse(doc.Title)
			doc.Text = strings.TrimSpace(text.String())
			if err == io.EOF {
				return doc, nil
			}
			// A truncated or malformed document still yields what was parsed.
			return doc, err

		case html.StartTagToken, html.SelfClosingTagToken:
			t := z.Token()
			name := t.Data
			switch name {
			case "html":
				if doc.Language == "" {
					doc.Language = attr(t, "lang")
				}
			case "title":
				inTitle = t.Type == html.StartTagToken
			case "meta":
				readMeta(doc, t)
			case "link":
				if strings.EqualFold(attr(t, "rel"), "canonical") {
					doc.Canonical = resolve(opts.Base, attr(t, "href"))
				}
			case "a":
				if opts.CollectLinks && t.Type == html.StartTagToken {
					// Unclosed anchors are common; keep the previous one.
					flushAnchor(doc, &inAnchor, &anchor, &anchorTx, opts.MaxLinks)
					inAnchor = true
					anchorTx.Reset()
					anchor = Link{Href: resolve(opts.Base, attr(t, "href")), Rel: attr(t, "rel")}
				}
			}
			if t.Type == html.StartTagToken && skipText[name] {
				skip = append(skip, name)
			}

		case html.EndTagToken:
			name := z.Token().Data
			switch name {
			case "title":
				inTitle = false
			case "a":
				flushAnchor(doc, &inAnchor, &anchor, &anchorTx, opts.MaxLinks)
			}
			// Unwind to the matching element rather than requiring exact
			// nesting, which malformed pages rarely provide.
			for i := len(skip) - 1; i >= 0; i-- {
				if skip[i] == name {
					skip = skip[:i]
					break
				}
			}

		case html.TextToken:
			if len(skip) > 0 && !inTitle {
				continue
			}
			raw := string(z.Text())
			if inTitle {
				// An unclosed <title> is RCDATA to the end of the document, so
				// the accumulator needs a ceiling.
				if room := maxTitleBytes - len(doc.Title); room > 0 {
					doc.Title += raw[:min(len(raw), room)]
				}
				continue
			}
			if inAnchor {
				anchorTx.WriteString(raw)
			}
			appendText(&text, doc, raw, opts.MaxTextBytes)
		}
	}
}

func flushAnchor(doc *Doc, inAnchor *bool, anchor *Link, text *strings.Builder, maxLinks int) {
	if !*inAnchor {
		return
	}
	*inAnchor = false
	if anchor.Href == "" || len(doc.Links) >= maxLinks {
		return
	}
	anchor.Text = collapse(text.String())
	doc.Links = append(doc.Links, *anchor)
}

// appendText collapses whitespace while writing, so the retained text is
// compact and the length count reflects real content.
func appendText(b *strings.Builder, doc *Doc, raw string, limit int) {
	fields := strings.FieldsFunc(raw, unicode.IsSpace)
	for _, f := range fields {
		doc.TextLength += len(f) + 1
		if limit > 0 && b.Len() >= limit {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(f)
	}
}

func readMeta(doc *Doc, t html.Token) {
	content := attr(t, "content")
	if content == "" {
		return
	}
	name := strings.ToLower(attr(t, "name"))
	property := strings.ToLower(attr(t, "property"))

	switch {
	case doc.Description == "" && (name == "description" || property == "og:description"):
		doc.Description = collapse(content)
	case doc.Title == "" && property == "og:title":
		doc.Title = collapse(content)
	case doc.Language == "" && (name == "language" || property == "og:locale"):
		doc.Language = collapse(content)
	}
}

func attr(t html.Token, key string) string {
	for _, a := range t.Attr {
		if strings.EqualFold(a.Key, key) {
			return strings.TrimSpace(a.Val)
		}
	}
	return ""
}

func resolve(base *url.URL, href string) string {
	href = strings.TrimSpace(href)
	if href == "" || strings.HasPrefix(href, "#") || strings.HasPrefix(href, "javascript:") {
		return ""
	}
	if base == nil {
		return href
	}
	u, err := base.Parse(href)
	if err != nil {
		return ""
	}
	u.Fragment = ""
	return u.String()
}

func collapse(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
