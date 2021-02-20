package preload

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
)

func TestCache(t *testing.T) {
	testcache(t, NewCache())
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
