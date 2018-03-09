package csibroker_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"

	"code.cloudfoundry.org/csibroker/csibroker"
	"code.cloudfoundry.org/csishim/csi_fake"
	"code.cloudfoundry.org/goshims/osshim/os_fake"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagertest"
	"code.cloudfoundry.org/service-broker-store/brokerstore"
	"code.cloudfoundry.org/service-broker-store/brokerstore/brokerstorefakes"
	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-cf/brokerapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var _ = Describe("Broker", func() {
	var (
		broker                 *csibroker.Broker
		fakeOs                 *os_fake.FakeOs
		specFilepath           string
		pwd                    string
		logger                 lager.Logger
		ctx                    context.Context
		fakeStore              *brokerstorefakes.FakeStore
		fakeCsi                *csi_fake.FakeCsi
		conn                   *grpc.ClientConn
		fakeController         *csi_fake.FakeControllerClient
		fakeIdentityController *csi_fake.FakeIdentityClient
		err                    error
		driverName             string
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test-broker")
		ctx = context.TODO()
		fakeOs = &os_fake.FakeOs{}
		fakeStore = &brokerstorefakes.FakeStore{}
		fakeCsi = &csi_fake.FakeCsi{}
		fakeController = &csi_fake.FakeControllerClient{}
		fakeIdentityController = &csi_fake.FakeIdentityClient{}

		fakeCsi.NewControllerClientReturns(fakeController)
		fakeCsi.NewIdentityClientReturns(fakeIdentityController)

		listenAddr := "0.0.0.0:" + strconv.Itoa(8999+GinkgoParallelNode())
		conn, err = grpc.Dial(listenAddr, grpc.WithInsecure())
		driverName = "some-csi-driver"

	})

	Context("when creating first time", func() {
		BeforeEach(func() {
			pwd, err = os.Getwd()
			Expect(err).ToNot(HaveOccurred())
			specFilepath = filepath.Join(pwd, "..", "fixtures", "service_spec.json")

			Expect(err).NotTo(HaveOccurred())

			broker, err = csibroker.New(
				logger,
				specFilepath,
				fakeOs,
				nil,
				fakeStore,
				fakeCsi,
				conn,
				driverName,
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
						driverName,
					)
				})

				It("returns an error", func() {
					Expect(err).To(Equal(errors.New("Not a valid specfile")))
				})
			})

		})

		Context(".Provision", func() {
			var (
				instanceID       string
				provisionDetails brokerapi.ProvisionDetails
				asyncAllowed     bool

				configuration string
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
				fakeStore.RetrieveInstanceDetailsReturns(brokerstore.ServiceInstance{}, errors.New("not found"))
			})

			JustBeforeEach(func() {
				_, err = broker.Provision(ctx, instanceID, provisionDetails, asyncAllowed)
			})

			Context("if the controller has not been probed yet", func() {
				It("probes the controller", func() {
					Expect(fakeIdentityController.ProbeCallCount()).To(Equal(1))
				})

				Context("if the probe fails", func() {
					BeforeEach(func() {
						fakeIdentityController.ProbeReturns(&csi.ProbeResponse{}, grpc.Errorf(codes.Unknown, "probe badness"))
					})

					It("should error", func() {
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).To(Equal("rpc error: code = Unknown desc = probe badness"))
					})
				})
			})

			Context("if the controller has been probed already", func() {
				JustBeforeEach(func() {
					Expect(fakeIdentityController.ProbeCallCount()).To(Equal(1))
					fakeIdentityController.ProbeReturns(&csi.ProbeResponse{}, nil)
				})

				It("does not probe the controller again for any future calls", func() {
					_, _ = broker.Provision(ctx, instanceID, provisionDetails, asyncAllowed)
					Expect(fakeIdentityController.ProbeCallCount()).To(Equal(1))
				})
			})

			It("should not error", func() {
				Expect(err).NotTo(HaveOccurred())
			})

			It("should write state", func() {
				Expect(fakeStore.SaveCallCount()).Should(BeNumerically(">", 0))
			})

			It("should send the request to the controller client", func() {
				expectedRequest := &csi.CreateVolumeRequest{
					Name: "csi-storage",
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: 2,
						LimitBytes:    3,
					},
					VolumeCapabilities: []*csi.VolumeCapability{{
						AccessType: &csi.VolumeCapability_Mount{
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

			Context("when creating volume returns volume info", func() {
				var volInfo *csi.Volume

				BeforeEach(func() {
					volInfo = &csi.Volume{
						CapacityBytes: int64(20),
						Id:            "some-volume-id",
					}
					fakeController.CreateVolumeReturns(&csi.CreateVolumeResponse{Volume: volInfo}, nil)
				})

				It("should save it", func() {
					Expect(fakeController.CreateVolumeCallCount()).To(Equal(1))

					fingerprint := csibroker.ServiceFingerPrint{
						Name:   "csi-storage",
						Volume: volInfo,
					}

					expectedServiceInstance := brokerstore.ServiceInstance{
						PlanID:             "CSI-Existing",
						ServiceFingerPrint: fingerprint,
					}
					Expect(fakeStore.CreateInstanceDetailsCallCount()).To(Equal(1))
					fakeInstanceID, fakeServiceInstance := fakeStore.CreateInstanceDetailsArgsForCall(0)
					Expect(fakeInstanceID).To(Equal(instanceID))
					Expect(fakeServiceInstance).To(Equal(expectedServiceInstance))
					Expect(fakeStore.SaveCallCount()).Should(BeNumerically(">", 0))
				})
			})
			Context("when the client returns an error", func() {
				BeforeEach(func() {
					fakeController.CreateVolumeReturns(&csi.CreateVolumeResponse{}, grpc.Errorf(codes.Unknown, "badness"))
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
				instanceID         string
				asyncAllowed       bool
				deprovisionDetails brokerapi.DeprovisionDetails
				err                error
			)

			BeforeEach(func() {
				instanceID = "some-instance-id"
				deprovisionDetails = brokerapi.DeprovisionDetails{PlanID: "Existing", ServiceID: "some-service-id"}
				asyncAllowed = true
			})

			JustBeforeEach(func() {
				_, err = broker.Deprovision(ctx, instanceID, deprovisionDetails, asyncAllowed)
			})

			Context("when the probe fails", func() {
				BeforeEach(func() {
					fakeIdentityController.ProbeReturns(&csi.ProbeResponse{}, grpc.Errorf(codes.Unknown, "probe badness"))
				})

				It("should error", func() {
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("rpc error: code = Unknown desc = probe badness"))
				})
			})

			Context("when the instance does not exist", func() {
				BeforeEach(func() {
					instanceID = "does-not-exist"
					fakeStore.RetrieveInstanceDetailsReturns(brokerstore.ServiceInstance{}, brokerapi.ErrInstanceDoesNotExist)
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
					asyncAllowed = false

					fingerprint := csibroker.ServiceFingerPrint{
						Name:   "some-csi-storage",
						Volume: &csi.Volume{Id: "some-volume-id"},
					}

					// simulate untyped data loaded from a data file
					jsonFingerprint := &map[string]interface{}{}
					raw, err := json.Marshal(fingerprint)
					Expect(err).ToNot(HaveOccurred())
					err = json.Unmarshal(raw, jsonFingerprint)
					Expect(err).ToNot(HaveOccurred())

					fakeStore.RetrieveInstanceDetailsReturns(brokerstore.ServiceInstance{
						ServiceID:          "some-service-id",
						ServiceFingerPrint: jsonFingerprint,
					}, nil)
					previousSaveCallCount = fakeStore.SaveCallCount()
				})

				Context("if the controller has been probed already", func() {
					JustBeforeEach(func() {
						Expect(fakeIdentityController.ProbeCallCount()).To(Equal(1))
						fakeIdentityController.ProbeReturns(&csi.ProbeResponse{}, nil)
					})

					It("does not probe the controller again for any future calls", func() {
						_, err = broker.Deprovision(ctx, instanceID, deprovisionDetails, asyncAllowed)
						Expect(fakeIdentityController.ProbeCallCount()).To(Equal(1))
					})
				})

				It("probes the controller", func() {
					_, request, _ := fakeIdentityController.ProbeArgsForCall(0)
					Expect(request).To(Equal(&csi.ProbeRequest{}))
					Expect(fakeIdentityController.ProbeCallCount()).To(Equal(1))
				})

				It("should succeed", func() {
					Expect(err).NotTo(HaveOccurred())
				})

				It("save state", func() {
					Expect(fakeStore.SaveCallCount()).To(Equal(previousSaveCallCount + 1))
				})

				It("should send the request to the controller client", func() {
					expectedRequest := &csi.DeleteVolumeRequest{
						VolumeId:                "some-volume-id",
						ControllerDeleteSecrets: map[string]string{},
					}
					Expect(fakeController.DeleteVolumeCallCount()).To(Equal(1))
					_, request, _ := fakeController.DeleteVolumeArgsForCall(0)
					Expect(request).To(Equal(expectedRequest))
				})

				Context("when the client returns an error", func() {
					BeforeEach(func() {
						fakeController.DeleteVolumeReturns(&csi.DeleteVolumeResponse{}, grpc.Errorf(codes.Unknown, "badness"))
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

		Context(".Bind", func() {
			var (
				instanceID    string
				serviceID     string
				bindDetails   brokerapi.BindDetails
				rawParameters json.RawMessage
				params        map[string]interface{}
			)

			BeforeEach(func() {
				instanceID = "some-instance-id"
				serviceID = "some-service-id"
				params = make(map[string]interface{})
				params["key"] = "value"
				rawParameters, err = json.Marshal(params)

				fingerprint := csibroker.ServiceFingerPrint{
					Name: "some-csi-storage",
					Volume: &csi.Volume{
						Id:         instanceID,
						Attributes: map[string]string{"foo": "bar"},
					},
				}

				// simulate untyped data loaded from a data file
				jsonFingerprint := &map[string]interface{}{}
				raw, err := json.Marshal(fingerprint)
				Expect(err).ToNot(HaveOccurred())
				err = json.Unmarshal(raw, jsonFingerprint)
				Expect(err).ToNot(HaveOccurred())

				fakeStore.RetrieveInstanceDetailsReturns(brokerstore.ServiceInstance{
					ServiceID:          serviceID,
					ServiceFingerPrint: jsonFingerprint,
				}, nil)

				bindDetails = brokerapi.BindDetails{
					AppGUID:       "guid",
					ServiceID:     serviceID,
					RawParameters: rawParameters,
				}
			})

			Context("if the controller has not been probed yet", func() {
				It("probes the controller", func() {
					_, _ = broker.Bind(ctx, instanceID, "binding-id", bindDetails)
					_, request, _ := fakeIdentityController.ProbeArgsForCall(0)
					Expect(request).To(Equal(&csi.ProbeRequest{}))
					Expect(fakeIdentityController.ProbeCallCount()).To(Equal(1))
				})

				Context("if the probe fails", func() {
					BeforeEach(func() {
						fakeIdentityController.ProbeReturns(&csi.ProbeResponse{}, grpc.Errorf(codes.Unknown, "probe badness"))
					})

					It("should error", func() {
						_, err = broker.Bind(ctx, instanceID, "binding-id", bindDetails)
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).To(Equal("rpc error: code = Unknown desc = probe badness"))
					})
				})
			})

			Context("if the controller has been probed already", func() {
				JustBeforeEach(func() {
					fakeIdentityController.ProbeReturns(&csi.ProbeResponse{}, nil)
					_, err = broker.Bind(ctx, instanceID, "binding-id", bindDetails)
					Expect(err).NotTo(HaveOccurred())
					Expect(fakeIdentityController.ProbeCallCount()).To(Equal(1))
				})

				It("does not probe the controller again for any future calls", func() {
					_, _ = broker.Bind(ctx, instanceID, "binding-id", bindDetails)
					Expect(fakeIdentityController.ProbeCallCount()).To(Equal(1))
				})
			})

			It("includes empty credentials to prevent CAPI crash", func() {
				binding, err := broker.Bind(ctx, instanceID, "binding-id", bindDetails)
				Expect(err).NotTo(HaveOccurred())

				Expect(binding.Credentials).NotTo(BeNil())
			})

			It("includes csi volume info in the service binding", func() {
				binding, err := broker.Bind(ctx, instanceID, "binding-id", bindDetails)
				Expect(err).NotTo(HaveOccurred())

				Expect(binding.VolumeMounts).NotTo(BeEmpty())
				Expect(binding.VolumeMounts[0].Device.MountConfig).NotTo(BeEmpty())
				Expect(binding.VolumeMounts[0].Device.MountConfig["id"]).To(Equal(instanceID))
				Expect(binding.VolumeMounts[0].Device.MountConfig["attributes"]).ToNot(BeEmpty())
				attr, _ := binding.VolumeMounts[0].Device.MountConfig["attributes"].(map[string]string)
				Expect(attr).ToNot(BeNil())
				Expect(attr["foo"]).To(Equal("bar"))
			})

			It("uses the instance id in the default container path", func() {
				binding, err := broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)
				Expect(err).NotTo(HaveOccurred())
				Expect(binding.VolumeMounts[0].ContainerDir).To(Equal("/var/vcap/data/some-instance-id"))
			})

			It("flows container path through", func() {
				params["mount"] = "/var/vcap/otherdir/something"
				bindDetails.RawParameters, err = json.Marshal(params)
				Expect(err).NotTo(HaveOccurred())
				binding, err := broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)
				Expect(err).NotTo(HaveOccurred())
				Expect(binding.VolumeMounts[0].ContainerDir).To(Equal("/var/vcap/otherdir/something"))
			})

			It("uses rw as its default mode", func() {
				binding, err := broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)
				Expect(err).NotTo(HaveOccurred())
				Expect(binding.VolumeMounts[0].Mode).To(Equal("rw"))
			})

			It("should write state", func() {
				previousSaveCallCount := fakeStore.SaveCallCount()
				_, err := broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)
				Expect(err).NotTo(HaveOccurred())
				Expect(fakeStore.SaveCallCount()).To(Equal(previousSaveCallCount + 1))
			})

			It("errors if mode is not a boolean", func() {
				params["readonly"] = ""
				bindDetails.RawParameters, err = json.Marshal(params)
				Expect(err).NotTo(HaveOccurred())
				_, err := broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)
				Expect(err).To(Equal(brokerapi.ErrRawParamsInvalid))
			})

			It("fills in the driver name", func() {
				binding, err := broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)
				Expect(err).NotTo(HaveOccurred())

				Expect(binding.VolumeMounts[0].Driver).To(Equal(driverName))
			})

			It("fills in the volume id", func() {
				binding, err := broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)
				Expect(err).NotTo(HaveOccurred())

				Expect(binding.VolumeMounts[0].Device.VolumeId).To(ContainSubstring("some-instance-id"))
			})

			Context("when the binding already exists", func() {

				It("doesn't error when binding the same details", func() {
					fakeStore.IsBindingConflictReturns(false)
					_, err := broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)
					Expect(err).NotTo(HaveOccurred())
				})

				It("errors when binding different details", func() {
					fakeStore.IsBindingConflictReturns(true)
					_, err := broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)
					Expect(err).To(Equal(brokerapi.ErrBindingAlreadyExists))
				})
			})

			Context("when the details are not provided", func() {
				BeforeEach(func() {
					bindDetails.RawParameters = nil
				})
				It("succeeds", func() {
					_, err := broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)
					Expect(err).NotTo(HaveOccurred())
				})
			})

			Context("when the binding cannot be stored", func() {
				var (
					err error
				)

				BeforeEach(func() {
					fakeStore.CreateBindingDetailsReturns(errors.New("badness"))
					_, err = broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)

				})

				It("should error", func() {
					Expect(err).To(HaveOccurred())
				})

			})

			Context("when the save fails", func() {
				var (
					err error
				)
				BeforeEach(func() {
					fakeStore.SaveReturns(errors.New("badness"))
					_, err = broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)
				})

				It("should error", func() {
					Expect(err).To(HaveOccurred())
				})
			})

			It("errors when the service instance does not exist", func() {
				fakeStore.RetrieveInstanceDetailsReturns(brokerstore.ServiceInstance{}, errors.New("Awesome!"))
				_, err := broker.Bind(ctx, "nonexistent-instance-id", "binding-id", brokerapi.BindDetails{AppGUID: "guid"})
				Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
			})

			It("errors when the app guid is not provided", func() {
				_, err := broker.Bind(ctx, "some-instance-id", "binding-id", brokerapi.BindDetails{})
				Expect(err).To(Equal(brokerapi.ErrAppGuidNotProvided))
			})
		})

		Context(".Unbind", func() {
			var (
				instanceID    string
				bindDetails   brokerapi.BindDetails
				params        map[string]interface{}
				rawParameters json.RawMessage
				err           error
			)
			BeforeEach(func() {
				instanceID = "some-instance-id"

				params = make(map[string]interface{})
				params["key"] = "value"
				rawParameters, err = json.Marshal(params)
				Expect(err).NotTo(HaveOccurred())

				bindDetails = brokerapi.BindDetails{AppGUID: "guid", RawParameters: rawParameters}

				fakeStore.RetrieveBindingDetailsReturns(bindDetails, nil)
			})

			Context("if the controller has not been probed yet", func() {
				It("probes the controller", func() {
					err := broker.Unbind(ctx, instanceID, "binding-id", brokerapi.UnbindDetails{})
					Expect(err).NotTo(HaveOccurred())

					_, request, _ := fakeIdentityController.ProbeArgsForCall(0)
					Expect(request).To(Equal(&csi.ProbeRequest{}))
					Expect(fakeIdentityController.ProbeCallCount()).To(Equal(1))
				})

				Context("if the probe fails", func() {
					BeforeEach(func() {
						fakeIdentityController.ProbeReturns(&csi.ProbeResponse{}, grpc.Errorf(codes.Unknown, "probe badness"))
					})

					It("should error", func() {
						err = broker.Unbind(ctx, instanceID, "binding-id", brokerapi.UnbindDetails{})
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).To(Equal("rpc error: code = Unknown desc = probe badness"))
					})
				})
			})

			Context("if the controller has been probed already", func() {
				JustBeforeEach(func() {
					fakeIdentityController.ProbeReturns(&csi.ProbeResponse{}, nil)
					err = broker.Unbind(ctx, instanceID, "binding-id", brokerapi.UnbindDetails{})
					Expect(err).NotTo(HaveOccurred())
					Expect(fakeIdentityController.ProbeCallCount()).To(Equal(1))
				})

				It("does not probe the controller again for any future calls", func() {
					err := broker.Unbind(ctx, instanceID, "binding-id", brokerapi.UnbindDetails{})
					Expect(err).NotTo(HaveOccurred())
					Expect(fakeIdentityController.ProbeCallCount()).To(Equal(1))
				})
			})

			It("unbinds a bound service instance from an app", func() {
				err := broker.Unbind(ctx, "some-instance-id", "binding-id", brokerapi.UnbindDetails{})
				Expect(err).NotTo(HaveOccurred())
			})

			It("fails when trying to unbind a instance that has not been provisioned", func() {
				fakeStore.RetrieveInstanceDetailsReturns(brokerstore.ServiceInstance{}, errors.New("Shazaam!"))
				err := broker.Unbind(ctx, "some-other-instance-id", "binding-id", brokerapi.UnbindDetails{})
				Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
			})

			It("fails when trying to unbind a binding that has not been bound", func() {
				fakeStore.RetrieveBindingDetailsReturns(brokerapi.BindDetails{}, errors.New("Hooray!"))
				err := broker.Unbind(ctx, "some-instance-id", "some-other-binding-id", brokerapi.UnbindDetails{})
				Expect(err).To(Equal(brokerapi.ErrBindingDoesNotExist))
			})

			It("should write state", func() {
				previousCallCount := fakeStore.SaveCallCount()
				err := broker.Unbind(ctx, "some-instance-id", "binding-id", brokerapi.UnbindDetails{})
				Expect(err).NotTo(HaveOccurred())
				Expect(fakeStore.SaveCallCount()).To(Equal(previousCallCount + 1))
			})

			Context("when the save fails", func() {
				BeforeEach(func() {
					fakeStore.SaveReturns(errors.New("badness"))
				})

				It("should error", func() {
					err := broker.Unbind(ctx, "some-instance-id", "binding-id", brokerapi.UnbindDetails{})
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when deletion of the binding details fails", func() {
				BeforeEach(func() {
					fakeStore.DeleteBindingDetailsReturns(errors.New("badness"))
				})

				It("should error", func() {
					err := broker.Unbind(ctx, "some-instance-id", "binding-id", brokerapi.UnbindDetails{})
					Expect(err).To(HaveOccurred())
				})
			})
		})
	})
})
