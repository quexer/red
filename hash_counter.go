package red

import (
	"github.com/gomodule/redigo/redis"
)

type HashCounter struct {
	name string
	do   DoFunc
}

func NewHashCounter(name string, f DoFunc) *HashCounter {
	return &HashCounter{
		name: name,
		do:   f,
	}
}

func (p *HashCounter) Inc(field string, n int) (int, error) {
	return redis.Int(p.do("HINCRBY", p.name, field, n))
}

func (p *HashCounter) GetAll() (map[string]int, error) {
	return redis.IntMap(p.do("HGETALL", p.name))
}

func (p *HashCounter) Get(field string) (int, error) {
	return redis.Int(p.do("HGET", p.name, field))
}

func (p *HashCounter) Set(field string, value int) error {
	_, err := redis.Int(p.do("HSET", p.name, field, value))
	return err
}

func (p *HashCounter) Clear() error {
	_, err := redis.Int(p.do("DEL", p.name))
	return err
}
