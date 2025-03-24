package red

import (
	"context"
	"strconv"

	"github.com/go-redis/redis/v8"
)

type HashCounter struct {
	name        string
	redisClient redis.UniversalClient
}

// NewHashCounter creates a new instance of HashCounter.
// name is the name of the hash in Redis.
// redisClient is the Redis client used to interact with the Redis server.
// Returns a new instance of HashCounter.
func NewHashCounter(name string, redisClient redis.UniversalClient) *HashCounter {
	return &HashCounter{
		name:        name,
		redisClient: redisClient,
	}
}

func (p *HashCounter) Inc(ctx context.Context, field string, n int) (int, error) {
	result, err := p.redisClient.HIncrBy(ctx, p.name, field, int64(n)).Result()
	return int(result), err
}

func (p *HashCounter) GetAll(ctx context.Context) (map[string]int, error) {
	m, err := p.redisClient.HGetAll(ctx, p.name).Result()
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

func (p *HashCounter) Get(ctx context.Context, field string) (int, error) {
	return p.redisClient.HGet(ctx, p.name, field).Int()
}

func (p *HashCounter) Set(ctx context.Context, field string, value int) error {
	return p.redisClient.HSet(ctx, p.name, field, value).Err()
}

func (p *HashCounter) Clear(ctx context.Context) error {
	return p.redisClient.Del(ctx, p.name).Err()
}
