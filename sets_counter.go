package red

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

// SetsCounter unique value counter based on redis sets
type SetsCounter struct {
	name        string
	expire      int // in second
	redisClient redis.UniversalClient
}

// NewSetsCounter create a new SetsCounter.
//  name: name of the counter
//  expire: expire time in second
func NewSetsCounter(name string, expire int, redisClient redis.UniversalClient) *SetsCounter {
	return &SetsCounter{
		name:        name,
		expire:      expire,
		redisClient: redisClient,
	}
}

// Append add value to SetsCounter,  if there's something new, increase the counter
func (p *SetsCounter) Append(ctx context.Context, key string, uniqVal interface{}) error {
	// append value to sets
	subKey := fmt.Sprintf("%s__%v", p.name, key)

	n, err := p.redisClient.SAdd(ctx, subKey, uniqVal).Result()
	if err != nil {
		return err
	}

	// set expire for sets key, if needed
	if err := p.setExpire(ctx, subKey); err != nil {
		return err
	}

	if n == 0 {
		return nil // nothing changed
	}

	if err := p.redisClient.HIncrBy(ctx, p.name, key, n).Err(); err != nil {
		return err
	}

	// set expire for hash key, if needed
	return p.setExpire(ctx, p.name)
}

func (p *SetsCounter) setExpire(ctx context.Context, key string) error {
	ttl, err := p.redisClient.TTL(ctx, key).Result()
	if err != nil {
		return err
	}
	if ttl < 0 {
		if err := p.redisClient.Expire(ctx, key, time.Duration(p.expire)*time.Second).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (p *SetsCounter) Get(ctx context.Context, key string) (int, error) {

	i, err := p.redisClient.HGet(ctx, p.name, key).Int()
	if err == redis.Nil {
		// expire
		return 0, nil
	}
	return i, err
}

func (p *SetsCounter) GetAll(ctx context.Context) (map[string]int, error) {
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
