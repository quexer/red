package red_test

import (
	"time"

	"github.com/alicebob/miniredis/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redis/go-redis/v9"

	"github.com/quexer/red"
)

var _ = Describe("Mutex", func() {
	var mutex *red.Mutex

	var redisSrv *miniredis.Miniredis
	BeforeEach(func() {
		var err error
		redisSrv, err = miniredis.Run()
		Ω(err).To(Succeed())

		client := redis.NewClient(&redis.Options{
			Addr: redisSrv.Addr(),
		})

		mutex = red.NewMutex(client)
	})
	AfterEach(func() {
		redisSrv.Close()
	})

	const (
		lockName = "foo"
		expire   = time.Minute
	)

	It("repeat lock", func() {
		fn, ok, err := mutex.Lock(ctx, lockName, expire)
		Ω(err).To(Succeed())
		Ω(ok).To(BeTrue())
		Ω(fn).NotTo(BeNil())

		fn, ok, err = mutex.Lock(ctx, lockName, expire)
		Ω(err).To(Succeed())
		Ω(ok).To(BeFalse()) // fail
		Ω(fn).To(BeNil())
	})
	It("lock again after manually unlock", func() {
		fn, ok, err := mutex.Lock(ctx, lockName, expire)
		Ω(err).To(Succeed())
		Ω(ok).To(BeTrue())
		Ω(fn).NotTo(BeNil())

		err = fn() // unlock
		Ω(err).To(Succeed())

		fn, ok, err = mutex.Lock(ctx, lockName, expire)
		Ω(err).To(Succeed())
		Ω(ok).To(BeTrue()) // success
		Ω(fn).NotTo(BeNil())
	})
	It("lock again after expire", func() {
		fn, ok, err := mutex.Lock(ctx, lockName, expire)
		Ω(err).To(Succeed())
		Ω(ok).To(BeTrue())
		Ω(fn).NotTo(BeNil())

		redisSrv.FastForward(time.Hour) // expire quickly

		fn, ok, err = mutex.Lock(ctx, lockName, expire)
		Ω(err).To(Succeed())
		Ω(ok).To(BeTrue()) // success
		Ω(fn).NotTo(BeNil())
	})
	It("don't care unlock function", func() {
		_, ok, err := mutex.Lock(ctx, lockName, expire)

		Ω(err).To(Succeed())
		Ω(ok).To(BeTrue())
	})
})
