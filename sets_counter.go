package red

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
)

// unique value counter based on redis sets
type SetsCounter struct {
	name   string
	expire int // in second
	do     DoFunc
}

func NewSetsCounter(name string, expire int, f DoFunc) *SetsCounter {
	return &SetsCounter{
		name:   name,
		expire: expire,
		do:     f,
	}
}

// add value to sets,  if there're something new, increase the counter
func (p *SetsCounter) Append(key, uniqVal interface{}) error {
	// append value to sets
	subKey := fmt.Sprintf("%s__%v", p.name, key)
	n, err := redis.Int(p.do("SADD", subKey, uniqVal))
	if err != nil {
		return err
	}

	// set expire for sets key, if needed
	if err := p.setExpire(subKey); err != nil {
		return err
	}

	if n == 0 {
		return nil // nothing changed
	}
	_, err = p.do("HINCRBY", p.name, key, n)
	if err != nil {
		return err
	}

	// set expire for hash key, if needed
	return p.setExpire(p.name)
}

func (p *SetsCounter) setExpire(key string) error {
	ttl, err := redis.Int(p.do("TTL", key))
	if err != nil {
		return err
	}
	if ttl < 0 {
		_, err := p.do("EXPIRE", key, p.expire)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *SetsCounter) Get(key interface{}) (int, error) {
	i, err := redis.Int(p.do("HGET", p.name, key))
	if err == redis.ErrNil {
		// expire
		return 0, nil
	}
	return i, err
}

func (p *SetsCounter) GetAll() (map[string]int, error) {
	result, err := redis.IntMap(redis.Values(p.do("HGETALL", p.name)))
	if err == redis.ErrNil {
		// expire
		return nil, nil
	}
	return result, err
}
