package red

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
)

// NewMutex creates a new Mutex
func NewMutex(client redis.UniversalClient) *Mutex {
	return &Mutex{
		rs: redsync.New(goredis.NewPool(client)),
	}
}

// Mutex distributed mutex based on redis.
// It's a simple wrapper of redsync. see redsync for more details
type Mutex struct {
	rs *redsync.Redsync
}

// UnlockFunc call this function to manually release a lock.
type UnlockFunc func() error

// Lock locks given name, return immediately regardless of result.
// return (UnlockFunc, true, nil) if successfully locked.
// return (nil, false, nil) if name is locked by others.
func (p *Mutex) Lock(ctx context.Context, name string, expire time.Duration) (UnlockFunc, bool, error) {
	if name == "" {
		return nil, false, errors.New("name is empty")
	}
	if expire < 10*time.Millisecond {
		return nil, false, errors.New("expire time must be greater than 10ms")
	}

	m := p.rs.NewMutex(name,
		redsync.WithExpiry(expire),
		redsync.WithTries(1), // no retry
	)

	err := m.LockContext(ctx)

	if err == redsync.ErrFailed {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	fn := func() error {
		_, err := m.Unlock()
		return err
	}
	return fn, true, nil
}
