package csibroker

import (
	"context"
	"sync"

	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/goshims/osshim"
	"code.cloudfoundry.org/lager"
	"github.com/pivotal-cf/brokerapi"
)

const (
	PermissionVolumeMount = brokerapi.RequiredPermission("volume_mount")
	DefaultContainerPath  = "/var/vcap/data"
)

type staticState struct {
	ServiceName string `json:"ServiceName"`
	ServiceId   string `json:"ServiceId"`
}

type ServiceInstance struct {
	ServiceID        string `json:"service_id"`
	PlanID           string `json:"plan_id"`
	OrganizationGUID string `json:"organization_guid"`
	SpaceGUID        string `json:"space_guid"`
	Share            string
}

type lock interface {
	Lock()
	Unlock()
}

type Broker struct {
	logger lager.Logger
	os     osshim.Os
	mutex  lock
	clock  clock.Clock
	static staticState
}

func New(
	logger lager.Logger,
	serviceName, serviceId string,
	os osshim.Os,
	clock clock.Clock,
) *Broker {

	theBroker := Broker{
		logger: logger,
		os:     os,
		mutex:  &sync.Mutex{},
		clock:  clock,
		static: staticState{
			ServiceName: serviceName,
			ServiceId:   serviceId,
		},
	}

	return &theBroker
}

func (b *Broker) Services(_ context.Context) []brokerapi.Service {
	logger := b.logger.Session("services")
	logger.Info("start")
	defer logger.Info("end")

	return []brokerapi.Service{{
		ID:            b.static.ServiceId,
		Name:          b.static.ServiceName,
		Description:   "Existing CSI volumes",
		Bindable:      true,
		PlanUpdatable: false,
		Tags:          []string{"csi"},
		Requires:      []brokerapi.RequiredPermission{PermissionVolumeMount},

		Plans: []brokerapi.ServicePlan{
			{
				Name:        "Existing",
				ID:          "Existing",
				Description: "A preexisting filesystem",
			},
		},
	}}
}

func (b *Broker) Provision(context context.Context, instanceID string, details brokerapi.ProvisionDetails, asyncAllowed bool) (_ brokerapi.ProvisionedServiceSpec, e error) {
	return brokerapi.ProvisionedServiceSpec{}, nil
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
