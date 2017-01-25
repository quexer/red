package utee

import (
	"github.com/garyburd/redigo/redis"
	"github.com/quexer/utee"
)

type HashCounter struct {
	do DoFunc
}

func (p *HashCounter) Inc(key, field string, n int) int {
	i, err := redis.Int(p.do("HINCRBY", key, field, n))
	utee.Log(err, "HashCounter Inc err")
	return i
}

func (p *HashCounter) GetAll(key string) map[string]int {
	m, err := redis.IntMap(p.do("HGETALL", key))
	utee.Log(err, "HashCounter GetAll err")
	if err != nil {
		return map[string]int{}
	}
	return m
}

func (p *HashCounter) Get(key, field string) int {
	i, err := redis.Int(p.do("HGET", key, field))
	utee.Log(err, "HashCounter Get err")
	return i
}

func (p *HashCounter) Set(key string, field int) {
	_, err := redis.Int(p.do("HSET", key, field))
	utee.Log(err, "HashCounter Set err")
}

func (p *HashCounter) DelAll(key ...string) {
	l := []interface{}{}
	for _, k := range key {
		l = append(l, k)
	}

	_, err := redis.Int(p.do("DEL", l...))
	utee.Log(err, "HashCounter DelAll err")
}
