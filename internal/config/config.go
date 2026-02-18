package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Port         string
	DatabaseURL  string
	RedisURL     string
	SentryDSN    string
	APIKeySecret string
	RateLimitRPS int
	CacheTTLHrs  int
	LogLevel     string
	Environment  string
}

func Load() (*Config, error) {
	cfg := &Config{
		Port:         envOr("PORT", "8080"),
		DatabaseURL:  os.Getenv("DATABASE_URL"),
		RedisURL:     os.Getenv("REDIS_URL"),
		SentryDSN:    os.Getenv("SENTRY_DSN"),
		APIKeySecret: os.Getenv("API_KEY_SECRET"),
		RateLimitRPS: envOrInt("RATE_LIMIT_RPS", 10),
		CacheTTLHrs:  envOrInt("CACHE_TTL_HOURS", 24),
		LogLevel:     envOr("LOG_LEVEL", "info"),
		Environment:  envOr("ENVIRONMENT", "production"),
	}

	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.RedisURL == "" {
		return nil, fmt.Errorf("REDIS_URL is required")
	}
	if cfg.APIKeySecret == "" {
		return nil, fmt.Errorf("API_KEY_SECRET is required")
	}

	return cfg, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}