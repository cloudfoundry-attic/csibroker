package csibroker_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestCsibroker(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Csibroker Suite")
}
