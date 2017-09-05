package csibroker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"sync"

	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/goshims/osshim"
	"code.cloudfoundry.org/lager"
	"github.com/golang/protobuf/jsonpb"
	csi "github.com/paulcwarren/spec"
	"github.com/paulcwarren/spec/csishim"
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
}

func New(
	logger lager.Logger,
	serviceSpecPath string,
	os osshim.Os,
	clock clock.Clock,
	store Store,
	csi csishim.Csi,
	conn *grpc.ClientConn,
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
		configuration.Name}

	if b.instanceConflicts(instanceDetails, instanceID) {
		return brokerapi.ProvisionedServiceSpec{}, brokerapi.ErrInstanceAlreadyExists
	}
	err = b.store.CreateInstanceDetails(instanceID, instanceDetails)
	if err != nil {
		return brokerapi.ProvisionedServiceSpec{}, fmt.Errorf("failed to store instance details %s", instanceID)
	}
	logger.Info("service-instance-created", lager.Data{"instanceDetails": instanceDetails})

	b.controllerClient.CreateVolume(context, &configuration)

	return brokerapi.ProvisionedServiceSpec{IsAsync: false}, nil
}

func (b *Broker) Deprovision(context context.Context, instanceID string, details brokerapi.DeprovisionDetails, asyncAllowed bool) (_ brokerapi.DeprovisionServiceSpec, e error) {
	return brokerapi.DeprovisionServiceSpec{}, nil
}
func (b *Broker) Bind(context context.Context, instanceID string, bindingID string, bindDetails brokerapi.BindDetails) (_ brokerapi.Binding, e error) {
	return brokerapi.Binding{}, nil
}

func (b *Broker) Unbind(context context.Context, instanceID string, bindingID string, details brokerapi.UnbindDetails) (e error) {
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
