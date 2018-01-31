/**
 *  composite queue implementation, using redis
 *  support multi queue & queue element expire
 */
package red

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/pborman/uuid"
	"log"
)

type CompositeQ struct {
	name string
	do   DoFunc
}

func NewCompositeQ(name string, f DoFunc) *CompositeQ {
	return &CompositeQ{
		name: name,
		do:   f,
	}
}

func (p *CompositeQ) compositeQname(uid interface{}) string {
	return fmt.Sprintf("q-%s-%v", p.name, uid)
}

func (p *CompositeQ) Len(uid interface{}) (int, error) {
	name := p.compositeQname(uid)

	i, err := redis.Int(p.do("LLEN", name))

	if err != nil && err == redis.ErrNil {
		//expire
		return 0, nil
	}

	return i, err
}

func (p *CompositeQ) Enq(uid interface{}, data []byte, ttl ...uint32) error {
	k := fmt.Sprintf("mk%v", uuid.NewUUID())
	if len(ttl) > 0 && ttl[0] > 0 {
		if _, err := p.do("SETEX", k, ttl[0], data); err != nil {
			return err
		}
	} else {
		if _, err := p.do("SET", k, data); err != nil {
			return err
		}
	}
	name := p.compositeQname(uid)
	if _, err := p.do("RPUSH", name, k); err != nil {
		return err
	}
	return nil
}

func (p *CompositeQ) Deq(uid interface{}) ([]byte, error) {
	for {
		name := p.compositeQname(uid)
		k, err := redis.String(p.do("LPOP", name))
		if err != nil && err != redis.ErrNil {
			return nil, err
		}

		if k == "" {
			break
		}

		b, err := redis.Bytes(p.do("GET", k))
		if err != nil && err != redis.ErrNil {
			return nil, err
		}

		if b != nil {
			go func() {
				//clean
				if _, err := p.do("DEL", k); err != nil {
					log.Println("[Q Deq] err in clean", err)
				}
			}()
		}
		return b, nil
	}
	return nil, nil
}
