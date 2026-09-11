// Package config loads the optional YAML configuration file. Every value has a
// working default, so the collector runs with no config file at all.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type SQLite struct {
	CacheSizeKB int    `yaml:"cache_size_kb"`
	MMapSizeMB  int    `yaml:"mmap_size_mb"`
	BusyTimeout string `yaml:"busy_timeout"`
	MaxConns    int    `yaml:"max_conns"`
}

type Config struct {
	Database   string                    `yaml:"database"`
	Workers    int                       `yaml:"workers"`
	BatchSize  int                       `yaml:"batch_size"`
	Timeout    string                    `yaml:"timeout"`
	UserAgent  string                    `yaml:"user_agent"`
	RateLimit  float64                   `yaml:"rate_limit"`
	MaxRetries int                       `yaml:"max_retries"`
	SQLite     SQLite                    `yaml:"sqlite"`
	Sources    map[string]map[string]any `yaml:"sources"`
	loadedFrom string
}

func Default() *Config {
	return &Config{
		Database:   "./data/massive.db",
		Workers:    8,
		BatchSize:  10000,
		Timeout:    "60s",
		RateLimit:  0,
		MaxRetries: 4,
		SQLite:     SQLite{CacheSizeKB: 65536, MMapSizeMB: 256, BusyTimeout: "30s", MaxConns: 4},
		Sources:    map[string]map[string]any{},
	}
}

// DefaultPaths are searched in order when --config is not given.
var DefaultPaths = []string{"config/config.yaml", "config.yaml", "config/config.yml"}

func Load(path string) (*Config, error) {
	cfg := Default()

	explicit := path != ""
	if !explicit {
		for _, p := range DefaultPaths {
			if _, err := os.Stat(p); err == nil {
				path = p
				break
			}
		}
		if path == "" {
			return cfg, nil
		}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if !explicit && os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.Unmarshal([]byte(os.ExpandEnv(string(data))), cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if cfg.Sources == nil {
		cfg.Sources = map[string]map[string]any{}
	}
	cfg.loadedFrom = path
	return cfg, nil
}

func (c *Config) LoadedFrom() string { return c.loadedFrom }

func (c *Config) Source(name string) map[string]any {
	if s, ok := c.Sources[name]; ok && s != nil {
		return s
	}
	return map[string]any{}
}

// Enabled reports whether a source participates in `collector run all`.
func (c *Config) Enabled(name string) bool {
	s, ok := c.Sources[name]
	if !ok || s == nil {
		return false
	}
	v, ok := s["enabled"]
	if !ok {
		return true
	}
	b, _ := v.(bool)
	return b
}

func (c *Config) ParseTimeout() time.Duration {
	d, err := time.ParseDuration(c.Timeout)
	if err != nil || d <= 0 {
		return 60 * time.Second
	}
	return d
}

func (c *Config) ParseBusyTimeout() time.Duration {
	d, err := time.ParseDuration(c.SQLite.BusyTimeout)
	if err != nil || d <= 0 {
		return 30 * time.Second
	}
	return d
}
