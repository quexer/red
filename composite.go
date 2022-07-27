package red

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

// CompositeQ composite queue implementation, using redis
//  support multi queue & queue element expire
type CompositeQ struct {
	name        string
	redisClient redis.UniversalClient
}

func NewCompositeQ(name string, redisClient redis.UniversalClient) *CompositeQ {
	return &CompositeQ{
		name:        name,
		redisClient: redisClient,
	}
}

func (p *CompositeQ) compositeQname(uid interface{}) string {
	return fmt.Sprintf("q-%s-%v", p.name, uid)
}

func (p *CompositeQ) Len(ctx context.Context, uid interface{}) (int, error) {
	name := p.compositeQname(uid)

	i, err := p.redisClient.LLen(ctx, name).Result()
	if err == redis.Nil {
		return 0, nil
	}
	return int(i), err
}

func (p *CompositeQ) Enq(ctx context.Context, uid interface{}, data []byte, ttl ...uint32) error {
	id, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	k := fmt.Sprintf("mk%v", id)

	var expire time.Duration
	if len(ttl) > 0 && ttl[0] > 0 {
		expire = time.Duration(ttl[0]) * time.Second
	}

	if err := p.redisClient.Set(ctx, k, data, expire).Err(); err != nil {
		return err
	}

	return p.redisClient.RPush(ctx, p.compositeQname(uid), k).Err()
}

func (p *CompositeQ) Deq(ctx context.Context, uid interface{}) ([]byte, error) {
	for {
		name := p.compositeQname(uid)
		k, err := p.redisClient.LPop(ctx, name).Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}

		if k == "" {
			break
		}

		b, err := p.redisClient.Get(ctx, k).Bytes()
		if err != nil && err != redis.Nil {
			return nil, err
		}

		if b != nil {
			go func() {
				// clean
				if err := p.redisClient.Del(ctx, k).Err(); err != nil {
					log.Println("[Q Deq] err in clean", err)
				}
			}()
			return b, nil
		}
	}
	return nil, nil
}
