package utee

import (
	"github.com/garyburd/redigo/redis"
	"github.com/quexer/utee"
)

// SimpleCounter is the interface that wraps a increase-only counter(based on redis HyperLogLog)
//
// Get return count. if multiple key was provided, the result is union count of the keys
//
// Append append items to counter, the count will be increased only if brand new items have been append.
// if this happen, Append() will return true, otherwise return false
//
// Reset reset counter to 0

type SimpleCounter interface {
	Get(key ...string) int
	Append(key string, element ...interface{}) bool
	Reset(key ...string)
}

type simpleCounter struct {
	f    DoFunc
	name string
}

func NewSimpleCounter(f DoFunc) SimpleCounter {
	return &simpleCounter{
		f: f,
	}
}

func (p *simpleCounter) Get(key ...string) int {
	l := []interface{}{}
	for _, k := range key {
		l = append(l, k)
	}

	i, err := redis.Int(p.f("PFCOUNT", l...))
	utee.Log(err)
	return i
}

func (p *simpleCounter) Append(key string, element ...interface{}) bool {
	l := []interface{}{key}
	for _, v := range element {
		l = append(l, v)
	}
	i, err := redis.Int(p.f("PFADD", l...))
	utee.Log(err)
	return i == 1
}

func (p *simpleCounter) Reset(key ...string) {
	l := []interface{}{}
	for _, k := range key {
		l = append(l, k)
	}

	_, err := redis.Int(p.f("DEL", l...))
	utee.Log(err)
}
