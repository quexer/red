package red

import (
	"context"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/quexer/utee"
)

func qname(name string) string {
	return "q" + name
}

type Queue struct {
	name        string
	redisClient redis.UniversalClient
	buffer      utee.MemQueue
	batch       int
}

// NewQueue ,  create a redis queue with optional input memory buffer
//   redisClient: redis UniversalClient
//   name: queue name in redis
//   enqBatch: batch enqueue number, must >=1
//   buffer: memory buffer capacity, must >= 0
//   concurrent (optional)  : concurrent enqueue count. default is 1
func NewQueue(redisClient redis.UniversalClient, name string, enqBatch, buffer int, concurrent ...int) *Queue {
	if enqBatch < 1 {
		log.Fatal("batch must >= 1")
	}

	if buffer < 0 {
		log.Fatal("buffer must >= 0")
	}

	q := &Queue{
		name:        qname(name),
		redisClient: redisClient,
		buffer:      utee.NewMemQueue(buffer),
		batch:       enqBatch,
	}

	n := 1 // default 1
	if len(concurrent) > 0 {
		n = concurrent[0]
		if n < 1 {
			log.Fatal("concurrent must >= 1")
		}
	}

	for i := 0; i < n; i++ {
		go q.enqLoop()
	}
	return q
}

func (p *Queue) enqLoop() {
	for {
		l := p.buffer.DeqN(p.batch)
		if len(l) > 0 {
			ctx := context.Background()
			if err := p.redisClient.RPush(ctx, p.name, l...).Err(); err != nil {
				log.Println(err, "[BufferedInputQueue enqLoop] err ")
			}
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Len , return queue length
func (p *Queue) Len(ctx context.Context) (int, error) {
	n, err := p.redisClient.LLen(ctx, p.name).Result()
	if err == redis.Nil {
		// expire
		return 0, nil
	}
	return int(n), err
}

// EnqBlocking .  enqueue, block if buffer is full
func (p *Queue) EnqBlocking(ctx context.Context, data []byte) {
	p.buffer.EnqBlocking(data)
}

// Enq .  enqueue, return error if buffer is full
func (p *Queue) Enq(ctx context.Context, data []byte) error {
	return p.buffer.Enq(data)
}

func (p *Queue) Deq(ctx context.Context) ([]byte, error) {
	b, err := p.redisClient.RPop(ctx, p.name).Bytes()
	if err == redis.Nil {
		// expire
		return nil, nil
	}
	return b, err
}

func (p *Queue) BufferLen(ctx context.Context) int {
	return p.buffer.Len()
}

func (p *Queue) BufferCap(ctx context.Context) int {
	return p.buffer.Cap()
}
