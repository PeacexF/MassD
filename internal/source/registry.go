package source

import (
	"fmt"
	"sort"
	"sync"
)

var (
	mu       sync.RWMutex
	registry = map[string]Source{}
)

// Register adds a source adapter, normally from an init function.
func Register(s Source) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := registry[s.Name()]; dup {
		panic(fmt.Sprintf("source %q registered twice", s.Name()))
	}
	registry[s.Name()] = s
}

func Get(name string) (Source, bool) {
	mu.RLock()
	defer mu.RUnlock()
	s, ok := registry[name]
	return s, ok
}

func Names() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(registry))
	for n := range registry {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func All() []Source {
	names := Names()
	out := make([]Source, 0, len(names))
	for _, n := range names {
		s, _ := Get(n)
		out = append(out, s)
	}
	return out
}

func Describe(s Source) string {
	if d, ok := s.(Describer); ok {
		return d.Describe()
	}
	return ""
}
