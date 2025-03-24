package red

import (
	"context"
	"strconv"

	"github.com/go-redis/redis/v8"
)

// HashCounter represents a counter stored in a Redis hash.
type HashCounter struct {
	name   string                // The name of the hash in Redis.
	client redis.UniversalClient // The Redis client used to interact with the Redis server.
}

// NewHashCounter creates a new instance of HashCounter.
// name is the name of the hash in Redis.
// redisClient is the Redis client used to interact with the Redis server.
// Returns a new instance of HashCounter.
func NewHashCounter(name string, client redis.UniversalClient) *HashCounter {
	return &HashCounter{
		name:   name,
		client: client,
	}
}

// Inc increments the counter for a specific field by n.
// ctx is the context for the operation.
// field is the field in the hash to increment.
// n is the value to increment by.
// Returns the new value of the counter and any error encountered.
func (p *HashCounter) Inc(ctx context.Context, field string, n int) (int, error) {
	result, err := p.client.HIncrBy(ctx, p.name, field, int64(n)).Result()
	return int(result), err
}

// GetAll retrieves all fields and their values from the hash.
// ctx is the context for the operation.
// Returns a map of field names to their values and any error encountered.
func (p *HashCounter) GetAll(ctx context.Context) (map[string]int, error) {
	m, err := p.client.HGetAll(ctx, p.name).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	result := make(map[string]int)
	for k, v := range m {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, err
		}
		result[k] = n
	}
	return result, nil
}

// Get retrieves the value of a specific field from the hash.
// ctx is the context for the operation.
// field is the field in the hash to retrieve.
// Returns the value of the field and any error encountered.
func (p *HashCounter) Get(ctx context.Context, field string) (int, error) {
	return p.client.HGet(ctx, p.name, field).Int()
}

// Set sets the value of a specific field in the hash.
// ctx is the context for the operation.
// field is the field in the hash to set.
// value is the value to set the field to.
// Returns any error encountered.
func (p *HashCounter) Set(ctx context.Context, field string, value int) error {
	return p.client.HSet(ctx, p.name, field, value).Err()
}

// Clear deletes the hash from Redis.
// ctx is the context for the operation.
// Returns any error encountered.
func (p *HashCounter) Clear(ctx context.Context) error {
	return p.client.Del(ctx, p.name).Err()
}
