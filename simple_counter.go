package red

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// SimpleCounter is the interface that wraps an increase-only counter(based on redis HyperLogLog)
type SimpleCounter interface {
	Get(ctx context.Context, key ...string) (int, error)
	Append(ctx context.Context, key string, element ...interface{}) (bool, error)
	Del(ctx context.Context, key ...string) error
}

type simpleCounter struct {
	redisClient redis.UniversalClient
}

// NewSimpleCounter create a new SimpleCounter.
func NewSimpleCounter(redisClient redis.UniversalClient) SimpleCounter {
	return &simpleCounter{
		redisClient: redisClient,
	}
}

// Get return count. if multiple key was provided, the result is union count of the keys
func (p *simpleCounter) Get(ctx context.Context, key ...string) (int, error) {
	n, err := p.redisClient.PFCount(ctx, key...).Result()
	return int(n), err
}

// Append items to counter, the count will be increased only if brand-new items have been appended.
// if this happens, it will return true, otherwise return false
func (p *simpleCounter) Append(ctx context.Context, key string, element ...interface{}) (bool, error) {
	i, err := p.redisClient.PFAdd(ctx, key, element...).Result()
	if err != nil {
		return false, err
	}
	return i > 0, nil
}

// Del deletes the keys , reset counter to 0
func (p *simpleCounter) Del(ctx context.Context, key ...string) error {
	return p.redisClient.Del(ctx, key...).Err()
}
