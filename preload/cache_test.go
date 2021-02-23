package preload

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/go-redis/redis/v8"
)

type Cache interface {
	Del(ctx context.Context, key string, fields ...string) error
	Get(ctx context.Context, key string, fields ...string) ([]string, error)
	Set(ctx context.Context, key string, values map[string]string) error
}

func NewMapCache() Cache {
	return &mapcache{
		m: make(map[string]map[string]string),
	}
}

type mapcache struct {
	mu sync.Mutex
	m  map[string]map[string]string
}

func (c *mapcache) Del(ctx context.Context, key string, fields ...string) error {
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

func (c *mapcache) Get(ctx context.Context, key string, fields ...string) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := c.m[key]
	values := make([]string, len(fields))
	for i := range fields {
		values[i] = m[fields[i]]
	}
	return values, nil
}

func (c *mapcache) Set(ctx context.Context, key string, values map[string]string) error {
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
	if len(values) == 0 {
		return nil
	}
	hsetvalues := make(map[string]interface{})
	for k, v := range values {
		hsetvalues[k] = v
	}
	return c.redisclient.HSet(ctx, key, hsetvalues).Err()
}

func TestMemoryCache(t *testing.T) {
	testcache(t, NewMapCache())
}

func TestRedisCache(t *testing.T) {
	redisclient := redis.NewClient(&redis.Options{})
	if err := redisclient.FlushAll(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	testcache(t, NewRedisCache(redisclient))
}

func testcache(t *testing.T, c Cache) {
	ctx := context.Background()

	key := "key"
	fields := []string{"foo", "bar", "baz"}
	values, err := c.Get(ctx, key, fields...)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%q", values)

	if err := c.Set(ctx, key, map[string]string{
		"foo": "foo value",
		"baz": "baz value",
	}); err != nil {
		t.Fatal(err)
	}

	values, err = c.Get(ctx, key, fields...)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%q", values)

	if err := c.Del(ctx, key, "foo"); err != nil {
		t.Fatal(err)
	}

	values, err = c.Get(ctx, key, fields...)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%q", values)
}

func newredishook(t *testing.T) *redishook {
	return &redishook{t: t}
}

type redishook struct{ t *testing.T }

func (h *redishook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	return ctx, nil
}

func (h *redishook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	h.t.Log(cmd)
	return nil
}

func (h *redishook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	return ctx, nil
}
func (h *redishook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error { return nil }
