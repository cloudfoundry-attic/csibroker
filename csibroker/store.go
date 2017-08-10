package csibroker

import (
	"code.cloudfoundry.org/lager"
	"github.com/pivotal-cf/brokerapi"
)

//go:generate counterfeiter -o ../nfsbrokerfakes/fake_store.go . Store
type Store interface {
	RetrieveInstanceDetails(id string) (ServiceInstance, error)
	RetrieveBindingDetails(id string) (brokerapi.BindDetails, error)

	CreateInstanceDetails(id string, details ServiceInstance) error
	CreateBindingDetails(id string, details brokerapi.BindDetails) error

	DeleteInstanceDetails(id string) error
	DeleteBindingDetails(id string) error

	IsInstanceConflict(id string, details ServiceInstance) bool
	IsBindingConflict(id string, details brokerapi.BindDetails) bool

	Restore(logger lager.Logger) error
	Save(logger lager.Logger) error
	Cleanup() error
}
