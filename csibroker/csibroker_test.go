package csibroker_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"

	"code.cloudfoundry.org/csibroker/csibroker"
	"code.cloudfoundry.org/csibroker/csibrokerfakes"
	"code.cloudfoundry.org/goshims/osshim/os_fake"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagertest"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/paulcwarren/spec"
	"github.com/paulcwarren/spec/csishim/csi_fake"
	"github.com/pivotal-cf/brokerapi"
	"google.golang.org/grpc"
	"bytes"
)

var _ = Describe("Broker", func() {
	var (
		broker         *csibroker.Broker
		fakeOs         *os_fake.FakeOs
		specFilepath   string
		pwd            string
		logger         lager.Logger
		ctx            context.Context
		fakeStore      *csibrokerfakes.FakeStore
		fakeCsi        *csi_fake.FakeCsi
		conn           *grpc.ClientConn
		fakeController *csi_fake.FakeControllerClient
		err            error
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test-broker")
		ctx = context.TODO()
		fakeOs = &os_fake.FakeOs{}
		fakeStore = &csibrokerfakes.FakeStore{}
		fakeCsi = &csi_fake.FakeCsi{}
		fakeController = &csi_fake.FakeControllerClient{}
		fakeCsi.NewControllerClientReturns(fakeController)
		listenAddr := "0.0.0.0:" + strconv.Itoa(8999+GinkgoParallelNode())
		conn, err = grpc.Dial(listenAddr, grpc.WithInsecure())
	})

	Context("when creating first time", func() {
		BeforeEach(func() {
			pwd, err = os.Getwd()
			Expect(err).ToNot(HaveOccurred())
			specFilepath = filepath.Join(pwd, "..", "fixtures", "service_spec.json")

			broker, err = csibroker.New(
				logger,
				specFilepath,
				fakeOs,
				nil,
				fakeStore,
				fakeCsi,
				conn,
			)
		})

		Context(".Services", func() {
			Context("when the specfile is valid", func() {
				It("returns the service catalog as appropriate", func() {
					result := broker.Services(ctx)[0]
					Expect(result.ID).To(Equal("Service.ID"))
					Expect(result.Name).To(Equal("Service.Name"))
					Expect(result.Description).To(Equal("Service.Description"))
					Expect(result.Bindable).To(Equal(true))
					Expect(result.PlanUpdatable).To(Equal(false))
					Expect(result.Tags).To(ContainElement("Service.Tag1"))
					Expect(result.Tags).To(ContainElement("Service.Tag2"))
					Expect(result.Requires).To(ContainElement(brokerapi.RequiredPermission("Service.Requires")))

					Expect(result.Plans[0].Name).To(Equal("Service.Plans.Name"))
					Expect(result.Plans[0].ID).To(Equal("Service.Plans.ID"))
					Expect(result.Plans[0].Description).To(Equal("Service.Plans.Description"))
				})
			})

			Context("when the specfile is not valid", func() {
				BeforeEach(func() {
					specFilepath = filepath.Join(pwd, "..", "fixtures", "broken_service_spec.json")

					broker, err = csibroker.New(
						logger,
						specFilepath,
						fakeOs,
						nil,
						fakeStore,
						fakeCsi,
						conn,
					)
				})

				It("returns an error", func() {
					Expect(err).To(Equal(errors.New("Not an valid specfile")))
				})
			})

		})

		Context(".Provision", func() {
			var (
				instanceID       string
				provisionDetails brokerapi.ProvisionDetails
				asyncAllowed     bool

				configuration string
				spec          brokerapi.ProvisionedServiceSpec
				err           error
			)

			BeforeEach(func() {
				instanceID = "some-instance-id"
				configuration = `
        {
           "name":"csi-storage",
           "capacity_range":{
              "requiredBytes":"2",
              "limitBytes":"3"
           },
           "volume_capabilities":[
              {
                 "mount":{
                    "fsType":"fsType",
                    "mountFlags":[
                       "-o something",
                       "-t anotherthing"
                    ]
                 }
              }
           ],
           "parameters":{
              "a":"b"
           }
        }
        `
				provisionDetails = brokerapi.ProvisionDetails{PlanID: "CSI-Existing", RawParameters: json.RawMessage(configuration)}
				asyncAllowed = false
				fakeStore.RetrieveInstanceDetailsReturns(csibroker.ServiceInstance{}, errors.New("not found"))
			})

			JustBeforeEach(func() {
				spec, err = broker.Provision(ctx, instanceID, provisionDetails, asyncAllowed)
			})

			It("should not error", func() {
				Expect(err).NotTo(HaveOccurred())
			})

			It("should write state", func() {
				Expect(fakeStore.SaveCallCount()).Should(BeNumerically(">", 0))
			})

			It("should send the request to the controller client", func() {
				expectedRequest := &csi.CreateVolumeRequest{
					Version: csibroker.CSIversion,
					Name:    "csi-storage",
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: 2,
						LimitBytes:    3,
					},
					VolumeCapabilities: []*csi.VolumeCapability{{
						Value: &csi.VolumeCapability_Mount{
							Mount: &csi.VolumeCapability_MountVolume{
								FsType:     "fsType",
								MountFlags: []string{"-o something", "-t anotherthing"},
							},
						},
					}},
					Parameters: map[string]string{"a": "b"},
				}
				Expect(fakeController.CreateVolumeCallCount()).To(Equal(1))
				_, request, _ := fakeController.CreateVolumeArgsForCall(0)
				Expect(request).To(Equal(expectedRequest))
			})

			Context("when the client returns an error", func() {
				BeforeEach(func() {
					fakeController.CreateVolumeReturns(&csi.CreateVolumeResponse{Reply: &csi.CreateVolumeResponse_Error{}}, nil)
				})

				It("should error", func() {
					Expect(err).To(HaveOccurred())
				})
			})

			Context("create-service was given invalid JSON", func() {
				BeforeEach(func() {
					badJson := []byte("{this is not json")
					provisionDetails = brokerapi.ProvisionDetails{PlanID: "CSI-Existing", RawParameters: json.RawMessage(badJson)}
				})

				It("errors", func() {
					Expect(err).To(Equal(brokerapi.ErrRawParamsInvalid))
				})

			})
			Context("create-service was given valid JSON but no 'name'", func() {
				BeforeEach(func() {
					configuration := `
					{
            "volume_capabilities":[
               {
                  "mount":{
                     "fsType":"fsType",
                     "mountFlags":[
                        "-o something",
                        "-t anotherthing"
                     ]
                  }
               }
            ]
          }`
					provisionDetails = brokerapi.ProvisionDetails{PlanID: "CSI-Existing", RawParameters: json.RawMessage(configuration)}
				})

				It("errors", func() {
					Expect(err).To(Equal(errors.New("config requires a \"name\"")))
				})
			})

			Context("create-service was given valid JSON but no 'volume_capabilities'", func() {
				BeforeEach(func() {
					configuration := `
				  {
				     "name":"csi-storage"
				  }
				  `
					provisionDetails = brokerapi.ProvisionDetails{PlanID: "CSI-Existing", RawParameters: json.RawMessage(configuration)}
				})

				It("errors", func() {
					Expect(err).To(Equal(errors.New("config requires \"volume_capabilities\"")))
				})
			})

			Context("when the service instance already exists with the same details", func() {
				BeforeEach(func() {
					fakeStore.IsInstanceConflictReturns(false)
				})

				It("should not error", func() {
					Expect(err).NotTo(HaveOccurred())
				})
			})

			Context("when the service instance already exists with different details", func() {
				BeforeEach(func() {
					fakeStore.IsInstanceConflictReturns(true)
				})

				It("should error", func() {
					Expect(err).To(Equal(brokerapi.ErrInstanceAlreadyExists))
				})
			})

			Context("when the service instance creation fails", func() {
				BeforeEach(func() {
					fakeStore.CreateInstanceDetailsReturns(errors.New("badness"))
				})

				It("should error", func() {
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when the save fails", func() {
				BeforeEach(func() {
					fakeStore.SaveReturns(errors.New("badness"))
				})

				It("should error", func() {
					Expect(err).To(HaveOccurred())
				})
			})
		})

		Context(".Deprovision", func() {
			var (
				instanceID       string
				asyncAllowed     bool
				deprovisionDetails brokerapi.DeprovisionDetails
				err           error
			)

			BeforeEach(func() {
				instanceID = "some-instance-id"
				deprovisionDetails = brokerapi.DeprovisionDetails{PlanID: "Existing", ServiceID: "some-service-id"}
				asyncAllowed = true
			})

			JustBeforeEach(func() {
				_, err = broker.Deprovision(ctx, instanceID, deprovisionDetails, asyncAllowed)
			})

			Context("when the instance does not exist", func() {
				BeforeEach(func() {
					instanceID = "does-not-exist"
					fakeStore.RetrieveInstanceDetailsReturns(csibroker.ServiceInstance{}, brokerapi.ErrInstanceDoesNotExist)
				})

				It("should fail", func() {
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})

			Context("given an existing instance", func() {
				var (
					previousSaveCallCount int
				)

				BeforeEach(func() {
					configuration := map[string]interface{}{"share": "server:/some-share"}
					buf := &bytes.Buffer{}
					_ = json.NewEncoder(buf).Encode(configuration)
					asyncAllowed = false
					fakeStore.RetrieveInstanceDetailsReturns(csibroker.ServiceInstance{ServiceID: "some-service-id"}, nil)
					previousSaveCallCount = fakeStore.SaveCallCount()
				})

				It("should succeed", func() {
					Expect(err).NotTo(HaveOccurred())
				})

				It("save state", func() {
					Expect(fakeStore.SaveCallCount()).To(Equal(previousSaveCallCount + 1))
				})

				It("should send the request to the controller client", func() {
					expectedRequest := &csi.DeleteVolumeRequest{
						Version: csibroker.CSIversion,
						VolumeId:    &csi.VolumeID{
							Values: map[string]string{"volume_name": instanceID},
						},
						VolumeMetadata: &csi.VolumeMetadata{
							Values: map[string]string{"plan_id": "Existing", "service_id": "some-service-id"},
						},
					}
					Expect(fakeController.DeleteVolumeCallCount()).To(Equal(1))
					_, request, _ := fakeController.DeleteVolumeArgsForCall(0)
					Expect(request).To(Equal(expectedRequest))
				})

				Context("when the client returns an error", func() {
					BeforeEach(func() {
						fakeController.DeleteVolumeReturns(&csi.DeleteVolumeResponse{Reply: &csi.DeleteVolumeResponse_Error{}}, nil)
					})

					It("should error", func() {
						Expect(err).To(HaveOccurred())
					})
				})

				Context("when deletion of the instance fails", func() {
					BeforeEach(func() {
						fakeStore.DeleteInstanceDetailsReturns(errors.New("badness"))
					})

					It("should error", func() {
						Expect(err).To(HaveOccurred())
					})
				})

				Context("when the save fails", func() {
					BeforeEach(func() {
						fakeStore.SaveReturns(errors.New("badness"))
					})

					It("should error", func() {
						Expect(err).To(HaveOccurred())
					})
				})

				Context("delete-service was given no 'service_id'", func() {
					BeforeEach(func() {
						deprovisionDetails = brokerapi.DeprovisionDetails{PlanID: "Existing"}
					})

					It("errors", func() {
						Expect(err).To(Equal(errors.New("volume deletion requires \"service_id\"")))
					})
				})

				Context("delete-service was given no 'plan_id'", func() {
					BeforeEach(func() {
						deprovisionDetails = brokerapi.DeprovisionDetails{ServiceID: "some-service-id"}
					})

					It("errors", func() {
						Expect(err).To(Equal(errors.New("volume deletion requires \"plan_id\"")))
					})
				})

				Context("delete-service was given no instance id", func() {
					BeforeEach(func() {
						instanceID = ""
					})

					It("errors", func() {
						Expect(err).To(Equal(errors.New("volume deletion requires instance ID")))
					})
				})

				Context("when the service instance already exists with the same details", func() {
					BeforeEach(func() {
						fakeStore.IsInstanceConflictReturns(false)
					})

					It("should not error", func() {
						Expect(err).NotTo(HaveOccurred())
					})
				})
			})
		})
	})
})
