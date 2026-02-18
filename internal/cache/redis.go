package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Cache struct {
	client *redis.Client
	ttl    time.Duration
}

func New(redisURL string, ttlHours int) (*Cache, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parsing redis URL: %w", err)
	}

	client := redis.NewClient(opts)
	return &Cache{
		client: client,
		ttl:    time.Duration(ttlHours) * time.Hour,
	}, nil
}

func (c *Cache) Close() error {
	return c.client.Close()
}

func (c *Cache) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

func (c *Cache) Get(ctx context.Context, zipCode string, dest any) error {
	val, err := c.client.Get(ctx, keyForZIP(zipCode)).Result()
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(val), dest)
}

func (c *Cache) Set(ctx context.Context, zipCode string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshaling cache value: %w", err)
	}
	return c.client.Set(ctx, keyForZIP(zipCode), data, c.ttl).Err()
}

func keyForZIP(zip string) string {
	return "tax:zip:" + zip
}
