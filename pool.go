package red

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

type Opt struct {
	Server   string
	Auth     string
	Db       int
	PoolSize int
}

// CreatePoolWithOpt create a *redis.Pool with Opt
func CreatePoolWithOpt(opt Opt) *redis.Pool {
	return CreatePool(opt.PoolSize, opt.Server, opt.Auth, opt.Db)
}

// CreatePool create a *redis.Pool
// db is optional redis db number. the default value is 0
func CreatePool(size int, server, auth string, db ...int) *redis.Pool {
	pool := &redis.Pool{
		MaxIdle:     size,
		MaxActive:   size,
		Wait:        true,
		IdleTimeout: 4 * time.Minute,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if auth != "" {
				if _, err := c.Do("AUTH", auth); err != nil {
					c.Close()
					return nil, err
				}
			}
			if len(db) > 0 && db[0] > 0 {
				if _, err := c.Do("SELECT", db[0]); err != nil {
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			// test only if the connection is not used within last 10 seconds
			if time.Now().After(t.Add(10 * time.Second)) {
				_, err := c.Do("PING")
				return err
			} else {
				return nil
			}
		},
	}
	return pool
}

// DoFunc is a general redis template function, which can execute any redis command
type DoFunc func(string, ...interface{}) (interface{}, error)

// BuildExeFunc build a ExeFunc controlled by pool
func BuildDoFunc(pool *redis.Pool) DoFunc {
	return func(cmd string, args ...interface{}) (interface{}, error) {
		c := pool.Get()
		defer c.Close()
		return c.Do(cmd, args...)
	}
}
