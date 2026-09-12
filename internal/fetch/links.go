package fetch

import "strings"

// ParseLinks reads an RFC 5988 Link header into a rel -> URL map, which is how
// GitHub and many other APIs express pagination.
func ParseLinks(header string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(header, ",") {
		segments := strings.Split(strings.TrimSpace(part), ";")
		if len(segments) < 2 {
			continue
		}
		link := strings.TrimSpace(segments[0])
		if !strings.HasPrefix(link, "<") || !strings.HasSuffix(link, ">") {
			continue
		}
		link = link[1 : len(link)-1]

		for _, attr := range segments[1:] {
			key, val, ok := strings.Cut(strings.TrimSpace(attr), "=")
			if !ok || strings.TrimSpace(key) != "rel" {
				continue
			}
			rel := strings.Trim(strings.TrimSpace(val), `"'`)
			if rel != "" {
				out[rel] = link
			}
		}
	}
	return out
}
