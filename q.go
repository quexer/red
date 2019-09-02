package red

import (
	"github.com/garyburd/redigo/redis"
	"github.com/quexer/utee"
	"log"
	"time"
)

func qname(name string) string {
	return "q" + name
}

type Queue struct {
	name   string
	pool   *redis.Pool
	buffer utee.MemQueue
	batch  int
	do     DoFunc
}

//NewQueue,  create a redis queue with optional input memory buffer
//pool: redis connection pool
//name: queue name in redis
//enqBatch: batch enqueue number, must >=1
//buffer: memory buffer capacity, must >= 0
//concurrent (optional)  : concurrent enqueue count. default is half of pool.MaxActive
func NewQueue(pool *redis.Pool, name string, enqBatch, buffer int, concurrent ...int) *Queue {
	if enqBatch < 1 {
		log.Fatal("batch must >= 1")
	}

	if buffer < 0 {
		log.Fatal("buffer must >= 0")
	}

	q := &Queue{
		name:   qname(name),
		pool:   pool,
		do:     BuildDoFunc(pool),
		buffer: utee.NewMemQueue(buffer),
		batch:  enqBatch,
	}

	n := pool.MaxActive / 2 //default half of MaxActive
	if len(concurrent) > 0 {
		n = concurrent[0]
	}

	if n < 1 {
		log.Fatal("concurrent must >= 1")
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
			err := p.enqBatch(l)
			utee.Log(err, "[BufferedInputQueue enqLoop] err ")
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (p *Queue) enqBatch(l []interface{}) error {
	c := p.pool.Get()
	defer c.Close()
	for _, data := range l {
		err := c.Send("RPUSH", p.name, data)
		utee.Log(err, "[BufferedInputQueue enqBatch] err :")
	}
	return c.Flush()
}

//Len, return queue length
func (p *Queue) Len() (int, error) {
	i, err := redis.Int(p.do("LLEN", p.name))

	if err != nil && err.Error() == "redigo: nil returned" {
		//expire
		return 0, nil
	}
	return i, err
}

//EnqBlocking.  enqueue, block if buffer is full
func (p *Queue) EnqBlocking(data interface{}) {
	p.buffer.EnqBlocking(data)
}

//Enq.  enqueue, return error if buffer is full
func (p *Queue) Enq(data interface{}) error {
	return p.buffer.Enq(data)
}

func (p *Queue) Deq() (interface{}, error) {
	return p.do("LPOP", p.name)
}

func (p *Queue) BufferLen() int {
	return p.buffer.Len()
}

func (p *Queue) BufferCap() int {
	return p.buffer.Cap()
}
