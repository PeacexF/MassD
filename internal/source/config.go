package source

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is one source's section of the config file. Values arrive untyped from
// YAML, so access goes through typed accessors with defaults.
type Config map[string]any

func (c Config) Has(key string) bool { _, ok := c[key]; return ok }

func (c Config) String(key, def string) string {
	v, ok := c[key]
	if !ok || v == nil {
		return def
	}
	s := fmt.Sprint(v)
	// Secrets stay out of the file: ${VAR} reads the environment.
	if strings.HasPrefix(s, "${") && strings.HasSuffix(s, "}") {
		if env := os.Getenv(strings.TrimSuffix(strings.TrimPrefix(s, "${"), "}")); env != "" {
			return env
		}
		return def
	}
	return s
}

func (c Config) Int(key string, def int) int {
	v, ok := c[key]
	if !ok || v == nil {
		return def
	}
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	if n, err := strconv.Atoi(strings.TrimSpace(fmt.Sprint(v))); err == nil {
		return n
	}
	return def
}

func (c Config) Bool(key string, def bool) bool {
	v, ok := c[key]
	if !ok || v == nil {
		return def
	}
	if b, isBool := v.(bool); isBool {
		return b
	}
	b, err := strconv.ParseBool(fmt.Sprint(v))
	if err != nil {
		return def
	}
	return b
}

func (c Config) Duration(key string, def time.Duration) time.Duration {
	s := c.String(key, "")
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return def
	}
	return d
}

func (c Config) Strings(key string) []string {
	v, ok := c[key]
	if !ok || v == nil {
		return nil
	}
	switch list := v.(type) {
	case []string:
		return list
	case []any:
		out := make([]string, 0, len(list))
		for _, item := range list {
			if s := strings.TrimSpace(fmt.Sprint(item)); s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		s := strings.TrimSpace(fmt.Sprint(v))
		if s == "" {
			return nil
		}
		return []string{s}
	}
}
