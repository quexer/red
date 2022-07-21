package red_test

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestRed(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Red Suite")
}

var (
	ctx context.Context
)
var _ = BeforeSuite(func() {
	ctx = context.Background()
})
