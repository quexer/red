package red

import (
	"github.com/garyburd/redigo/redis"
	"github.com/quexer/utee"
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

func (p *HashCounter) Inc(field string, n int) int {
	i, err := redis.Int(p.do("HINCRBY", p.name, field, n))
	utee.Log(err, "HashCounter Inc err")
	return i
}

func (p *HashCounter) GetAll() map[string]int {
	m, err := redis.IntMap(p.do("HGETALL", p.name))
	utee.Log(err, "HashCounter GetAll err")
	if err != nil {
		return map[string]int{}
	}
	return m
}

func (p *HashCounter) Get(field string) int {
	i, err := redis.Int(p.do("HGET", p.name, field))
	utee.Log(err, "HashCounter Get err")
	return i
}

func (p *HashCounter) Set(field string, value int) {
	_, err := redis.Int(p.do("HSET", p.name, field, value))
	utee.Log(err, "HashCounter Set err")
}

func (p *HashCounter) Clear() {
	_, err := redis.Int(p.do("DEL", p.name))
	utee.Log(err, "HashCounter DelAll err")
}
