package red

import (
	"log"

	
	"github.com/gomodule/redigo/redis"
)

// SimpleCounter is the interface that wraps a increase-only counter(based on redis HyperLogLog)
//
// Get return count. if multiple key was provided, the result is union count of the keys
//
// Append append items to counter, the count will be increased only if brand new items have been append.
// if this happen, Append() will return true, otherwise return false
//
// Del delete the keys , reset counter to 0

type SimpleCounter interface {
	Get(key ...string) int
	Append(key string, element ...interface{}) bool
	Del(key ...string)
}

type simpleCounter struct {
	do   DoFunc
	name string
}

func NewSimpleCounter(f DoFunc) SimpleCounter {
	return &simpleCounter{
		do: f,
	}
}

func (p *simpleCounter) Get(key ...string) int {
	l := []interface{}{}
	for _, k := range key {
		l = append(l, k)
	}

	i, err := redis.Int(p.do("PFCOUNT", l...))
	log.Println(err)
	return i
}

func (p *simpleCounter) Append(key string, element ...interface{}) bool {
	l := []interface{}{key}
	for _, v := range element {
		l = append(l, v)
	}
	i, err := redis.Int(p.do("PFADD", l...))
	log.Println(err)
	return i == 1
}

func (p *simpleCounter) Del(key ...string) {
	l := []interface{}{}
	for _, k := range key {
		l = append(l, k)
	}

	_, err := redis.Int(p.do("DEL", l...))
	log.Println(err)
}
