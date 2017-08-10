package csibroker_test

import (
	"context"

	"code.cloudfoundry.org/csibroker/csibroker"
	"code.cloudfoundry.org/goshims/osshim/os_fake"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagertest"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-cf/brokerapi"
)

var _ = Describe("Broker", func() {
	var (
		broker *csibroker.Broker
		fakeOs *os_fake.FakeOs
		logger lager.Logger
		ctx    context.Context
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test-broker")
		ctx = context.TODO()
		fakeOs = &os_fake.FakeOs{}
	})

	Context("when creating first time", func() {
		BeforeEach(func() {
			broker = csibroker.New(
				logger,
				"service-name",
				"service-id",
				fakeOs,
				nil,
			)
		})

		Context(".Services", func() {
			It("returns the service catalog as appropriate", func() {
				result := broker.Services(ctx)[0]
				Expect(result.ID).To(Equal("service-id"))
				Expect(result.Name).To(Equal("service-name"))
				Expect(result.Description).To(Equal("Existing CSI volumes"))
				Expect(result.Bindable).To(Equal(true))
				Expect(result.PlanUpdatable).To(Equal(false))
				Expect(result.Tags).To(ContainElement("csi"))
				Expect(result.Requires).To(ContainElement(brokerapi.RequiredPermission("volume_mount")))

				Expect(result.Plans[0].Name).To(Equal("Existing"))
				Expect(result.Plans[0].ID).To(Equal("CSI-Existing"))
				Expect(result.Plans[0].Description).To(Equal("A preexisting filesystem"))
			})
		})
	})

})
