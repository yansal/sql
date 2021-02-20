package preload

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-redis/redis/v8"
)

type Cache interface {
	Del(ctx context.Context, key string, fields ...string) error
	Get(ctx context.Context, key string, fields ...string) ([]string, error)
	Set(ctx context.Context, key string, values map[string]string) error
}

func NewCache() Cache {
	return &cache{
		m: make(map[string]map[string]string),
	}
}

type cache struct {
	mu sync.Mutex
	m  map[string]map[string]string
}

func (c *cache) Del(ctx context.Context, key string, fields ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := c.m[key]
	if m == nil {
		return nil
	}
	for i := range fields {
		delete(m, fields[i])
	}
	return nil
}

func (c *cache) Get(ctx context.Context, key string, fields ...string) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := c.m[key]
	values := make([]string, len(fields))
	for i := range fields {
		values[i] = m[fields[i]]
	}
	return values, nil
}

func (c *cache) Set(ctx context.Context, key string, values map[string]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := c.m[key]
	if m == nil {
		m = make(map[string]string)
		c.m[key] = m
	}
	for k, v := range values {
		m[k] = v
	}
	return nil
}

func NewRedisCache(redisclient redis.UniversalClient) Cache {
	return &rediscache{
		redisclient: redisclient,
	}
}

type rediscache struct {
	redisclient redis.UniversalClient
}

func (c *rediscache) Del(ctx context.Context, key string, fields ...string) error {
	return c.redisclient.HDel(ctx, key, fields...).Err()
}

func (c *rediscache) Get(ctx context.Context, key string, fields ...string) ([]string, error) {
	result, err := c.redisclient.HMGet(ctx, key, fields...).Result()
	if err != nil {
		return nil, err
	}
	values := make([]string, len(result))
	for i := range result {
		switch v := result[i].(type) {
		case string:
			values[i] = v
		case nil:
			values[i] = ""
		default:
			panic(fmt.Sprintf("unknown result type %T", v))
		}
	}
	return values, nil
}

func (c *rediscache) Set(ctx context.Context, key string, values map[string]string) error {
	hsetvalues := make(map[string]interface{})
	for k, v := range values {
		hsetvalues[k] = v
	}
	return c.redisclient.HSet(ctx, key, hsetvalues).Err()
}
