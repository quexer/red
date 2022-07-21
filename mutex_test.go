package red_test

import (
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/quexer/red"
)

var _ = Describe("Mutex", func() {
	var lock *red.Mutex

	var redisSrv *miniredis.Miniredis
	BeforeEach(func() {
		var err error
		redisSrv, err = miniredis.Run()
		Ω(err).To(Succeed())

		client := redis.NewClient(&redis.Options{
			Addr: redisSrv.Addr(),
		})

		lock = red.NewMutex(client)
	})
	AfterEach(func() {
		redisSrv.Close()
	})

	const (
		lockName = "foo"
		expire   = time.Minute
	)

	It("repeat lock", func() {
		unlock, ok, err := lock.Lock(ctx, lockName, expire)
		Ω(err).To(Succeed())
		Ω(ok).To(BeTrue())
		Ω(unlock).NotTo(BeNil())

		unlock, ok, err = lock.Lock(ctx, lockName, expire)
		Ω(err).To(Succeed())
		Ω(ok).To(BeFalse()) // fail
		Ω(unlock).To(BeNil())
	})
	It("lock again after manually unlock", func() {
		unlock, ok, err := lock.Lock(ctx, lockName, expire)
		Ω(err).To(Succeed())
		Ω(ok).To(BeTrue())
		Ω(unlock).NotTo(BeNil())

		err = unlock() // unlock
		Ω(err).To(Succeed())

		unlock, ok, err = lock.Lock(ctx, lockName, expire)
		Ω(err).To(Succeed())
		Ω(ok).To(BeTrue()) // success
		Ω(unlock).NotTo(BeNil())
	})
	It("lock again after expire", func() {
		unlock, ok, err := lock.Lock(ctx, lockName, expire)
		Ω(err).To(Succeed())
		Ω(ok).To(BeTrue())
		Ω(unlock).NotTo(BeNil())

		redisSrv.FastForward(time.Hour) //

		unlock, ok, err = lock.Lock(ctx, lockName, expire)
		Ω(err).To(Succeed())
		Ω(ok).To(BeTrue()) // success
		Ω(unlock).NotTo(BeNil())
	})
})
