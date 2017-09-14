package csibroker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"sync"

	"path"

	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/csishim"
	"code.cloudfoundry.org/goshims/osshim"
	"code.cloudfoundry.org/lager"
	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/protobuf/jsonpb"
	"github.com/pivotal-cf/brokerapi"
	"google.golang.org/grpc"
)

const (
	PermissionVolumeMount = brokerapi.RequiredPermission("volume_mount")
	DefaultContainerPath  = "/var/vcap/data"
)

var CSIversion = &csi.Version{
	Major: 0,
	Minor: 0,
	Patch: 1,
}

type ServiceInstance struct {
	ServiceID        string `json:"service_id"`
	PlanID           string `json:"plan_id"`
	OrganizationGUID string `json:"organization_guid"`
	SpaceGUID        string `json:"space_guid"`
	Name             string
	VolumeInfo       *csi.VolumeInfo
}

type lock interface {
	Lock()
	Unlock()
}

type Broker struct {
	logger           lager.Logger
	os               osshim.Os
	mutex            lock
	clock            clock.Clock
	service          *brokerapi.Service
	store            Store
	csi              csishim.Csi
	controllerClient csi.ControllerClient
	driverName       string
}

func New(
	logger lager.Logger,
	serviceSpecPath string,
	os osshim.Os,
	clock clock.Clock,
	store Store,
	csi csishim.Csi,
	conn *grpc.ClientConn,
	driverName string,
) (*Broker, error) {

	logger = logger.Session("new-csi-broker")
	logger.Info("start")
	defer logger.Info("end")

	serviceSpec, err := ioutil.ReadFile(serviceSpecPath)

	if err != nil {
		logger.Error("failed-to-read-service-spec", err, lager.Data{"fileName": serviceSpecPath})
		return &Broker{}, err
	}

	brokerService := &brokerapi.Service{}

	err = json.Unmarshal(serviceSpec, brokerService)
	if err != nil {
		logger.Error("failed-to-unmarshall-spec from spec-file", err, lager.Data{"fileName": serviceSpecPath})
		return &Broker{}, err
	}
	logger.Info("spec-loaded", lager.Data{"fileName": serviceSpecPath})

	if brokerService.ID == "" || brokerService.Name == "" || brokerService.Description == "" || brokerService.Plans == nil {
		err = errors.New("Not an valid specfile")
		logger.Error("invalid-service-spec-file", err, lager.Data{"fileName": serviceSpecPath, "brokerService": brokerService})
		return &Broker{}, err
	}

	theBroker := Broker{
		logger:           logger,
		os:               os,
		mutex:            &sync.Mutex{},
		clock:            clock,
		store:            store,
		csi:              csi,
		controllerClient: csi.NewControllerClient(conn),
		service:          brokerService,
		driverName:       driverName,
	}

	return &theBroker, nil
}

func (b *Broker) Services(_ context.Context) []brokerapi.Service {
	logger := b.logger.Session("services")
	logger.Info("start")
	defer logger.Info("end")

	return []brokerapi.Service{*b.service}
}

func (b *Broker) Provision(context context.Context, instanceID string, details brokerapi.ProvisionDetails, asyncAllowed bool) (_ brokerapi.ProvisionedServiceSpec, e error) {
	logger := b.logger.Session("provision").WithData(lager.Data{"instanceID": instanceID, "details": details})
	logger.Info("start")
	defer logger.Info("end")

	var configuration csi.CreateVolumeRequest

	logger.Debug("provision-raw-parameters", lager.Data{"RawParameters": details.RawParameters})
	err := jsonpb.UnmarshalString(string(details.RawParameters), &configuration)
	if err != nil {
		logger.Error("provision-raw-parameters-decode-error", err)
		return brokerapi.ProvisionedServiceSpec{}, brokerapi.ErrRawParamsInvalid
	}
	configuration.Version = CSIversion
	if configuration.Name == "" {
		return brokerapi.ProvisionedServiceSpec{}, errors.New("config requires a \"name\"")
	}

	if len(configuration.GetVolumeCapabilities()) == 0 {
		return brokerapi.ProvisionedServiceSpec{}, errors.New("config requires \"volume_capabilities\"")
	}

	response, err := b.controllerClient.CreateVolume(context, &configuration)
	volumeResponseError, ok := response.GetReply().(*csi.CreateVolumeResponse_Error)
	if ok {
		return brokerapi.ProvisionedServiceSpec{}, errors.New(volumeResponseError.Error.String())
	}

	volInfo := response.GetResult().GetVolumeInfo()

	b.mutex.Lock()
	defer b.mutex.Unlock()
	defer func() {
		out := b.store.Save(logger)
		if e == nil {
			e = out
		}
	}()
	instanceDetails := ServiceInstance{
		details.ServiceID,
		details.PlanID,
		details.OrganizationGUID,
		details.SpaceGUID,
		configuration.Name,
		volInfo,
	}

	if b.instanceConflicts(instanceDetails, instanceID) {
		return brokerapi.ProvisionedServiceSpec{}, brokerapi.ErrInstanceAlreadyExists
	}
	err = b.store.CreateInstanceDetails(instanceID, instanceDetails)
	if err != nil {
		return brokerapi.ProvisionedServiceSpec{}, fmt.Errorf("failed to store instance details %s", instanceID)
	}
	logger.Info("service-instance-created", lager.Data{"instanceDetails": instanceDetails})

	return brokerapi.ProvisionedServiceSpec{IsAsync: false}, nil
}

func (b *Broker) Deprovision(context context.Context, instanceID string, details brokerapi.DeprovisionDetails, asyncAllowed bool) (_ brokerapi.DeprovisionServiceSpec, e error) {
	logger := b.logger.Session("deprovision")
	logger.Info("start")
	defer logger.Info("end")

	var configuration csi.DeleteVolumeRequest
	configuration.Version = CSIversion

	if instanceID == "" {
		return brokerapi.DeprovisionServiceSpec{}, errors.New("volume deletion requires instance ID")
	}
	if details.PlanID == "" {
		return brokerapi.DeprovisionServiceSpec{}, errors.New("volume deletion requires \"plan_id\"")
	}
	if details.ServiceID == "" {
		return brokerapi.DeprovisionServiceSpec{}, errors.New("volume deletion requires \"service_id\"")
	}

	instanceDetails, err := b.store.RetrieveInstanceDetails(instanceID)
	if err != nil {
		return brokerapi.DeprovisionServiceSpec{}, brokerapi.ErrInstanceDoesNotExist
	}

	configuration.UserCredentials = &csi.Credentials{}
	configuration.VolumeHandle = &csi.VolumeHandle{
		Id:       instanceDetails.VolumeInfo.Handle.Id,
		Metadata: map[string]string{},
	}

	response, _ := b.controllerClient.DeleteVolume(context, &configuration)
	volumeResponseError, ok := response.GetReply().(*csi.DeleteVolumeResponse_Error)
	if ok {
		return brokerapi.DeprovisionServiceSpec{}, errors.New(volumeResponseError.Error.String())
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()
	defer func() {
		out := b.store.Save(logger)
		if e == nil {
			e = out
		}
	}()

	err = b.store.DeleteInstanceDetails(instanceID)
	if err != nil {
		return brokerapi.DeprovisionServiceSpec{}, err
	}

	return brokerapi.DeprovisionServiceSpec{IsAsync: false, OperationData: "deprovision"}, nil
}

func (b *Broker) Bind(context context.Context, instanceID string, bindingID string, bindDetails brokerapi.BindDetails) (_ brokerapi.Binding, e error) {
	logger := b.logger.Session("bind")
	logger.Info("start", lager.Data{"bindingID": bindingID, "details": bindDetails})
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()
	defer func() {
		out := b.store.Save(logger)
		if e == nil {
			e = out
		}
	}()

	logger.Info("starting-csibroker-bind")
	instanceDetails, err := b.store.RetrieveInstanceDetails(instanceID)
	if err != nil {
		return brokerapi.Binding{}, brokerapi.ErrInstanceDoesNotExist
	}

	if bindDetails.AppGUID == "" {
		return brokerapi.Binding{}, brokerapi.ErrAppGuidNotProvided
	}

	params := make(map[string]interface{})

	logger.Debug(fmt.Sprintf("bindDetails: %#v", bindDetails.RawParameters))

	if bindDetails.RawParameters != nil {
		err = json.Unmarshal(bindDetails.RawParameters, &params)

		if err != nil {
			return brokerapi.Binding{}, err
		}
	}
	mode, err := evaluateMode(params)
	if err != nil {
		return brokerapi.Binding{}, err
	}

	if b.bindingConflicts(bindingID, bindDetails) {
		return brokerapi.Binding{}, brokerapi.ErrBindingAlreadyExists
	}

	logger.Info("retrieved-instance-details", lager.Data{"instanceDetails": instanceDetails})

	err = b.store.CreateBindingDetails(bindingID, bindDetails)
	if err != nil {
		return brokerapi.Binding{}, err
	}

	logger.Info("volume-service-binding", lager.Data{"Driver": "nfsv3driver"})

	volumeId := fmt.Sprintf("%s-volume", instanceID)

	ret := brokerapi.Binding{
		Credentials: struct{}{}, // if nil, cloud controller chokes on response
		VolumeMounts: []brokerapi.VolumeMount{{
			ContainerDir: evaluateContainerPath(params, instanceID),
			Mode:         mode,
			Driver:       b.driverName,
			DeviceType:   "shared",
			Device: brokerapi.SharedDevice{
				VolumeId: volumeId,
			},
		}},
	}
	return ret, nil
}

func (b *Broker) Unbind(context context.Context, instanceID string, bindingID string, details brokerapi.UnbindDetails) (e error) {
	logger := b.logger.Session("unbind")
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()
	defer func() {
		out := b.store.Save(logger)
		if e == nil {
			e = out
		}
	}()

	if _, err := b.store.RetrieveInstanceDetails(instanceID); err != nil {
		return brokerapi.ErrInstanceDoesNotExist
	}

	if _, err := b.store.RetrieveBindingDetails(bindingID); err != nil {
		return brokerapi.ErrBindingDoesNotExist
	}

	if err := b.store.DeleteBindingDetails(bindingID); err != nil {
		return err
	}
	return nil
}

func (b *Broker) Update(context context.Context, instanceID string, details brokerapi.UpdateDetails, asyncAllowed bool) (brokerapi.UpdateServiceSpec, error) {
	panic("not implemented")
}

func (b *Broker) LastOperation(_ context.Context, instanceID string, operationData string) (brokerapi.LastOperation, error) {
	return brokerapi.LastOperation{}, nil
}

func (b *Broker) instanceConflicts(details ServiceInstance, instanceID string) bool {
	return b.store.IsInstanceConflict(instanceID, ServiceInstance(details))
}

func (b *Broker) bindingConflicts(bindingID string, details brokerapi.BindDetails) bool {
	return b.store.IsBindingConflict(bindingID, details)
}
func evaluateContainerPath(parameters map[string]interface{}, volId string) string {
	if containerPath, ok := parameters["mount"]; ok && containerPath != "" {
		return containerPath.(string)
	}

	return path.Join(DefaultContainerPath, volId)
}

func evaluateMode(parameters map[string]interface{}) (string, error) {

	if ro, ok := parameters["readonly"]; ok {
		switch ro := ro.(type) {
		case bool:
			return readOnlyToMode(ro), nil
		default:
			return "", brokerapi.ErrRawParamsInvalid
		}
	}
	return "rw", nil
}

func readOnlyToMode(ro bool) string {
	if ro {
		return "r"
	}
	return "rw"
}
