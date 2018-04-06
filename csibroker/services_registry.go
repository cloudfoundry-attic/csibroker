package csibroker

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"code.cloudfoundry.org/csishim"
	"code.cloudfoundry.org/goshims/grpcshim"
	"code.cloudfoundry.org/lager"
	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/pivotal-cf/brokerapi"
	"google.golang.org/grpc"
)

type ErrServiceNotFound struct {
	ID string
}

func (e ErrServiceNotFound) Error() string {
	return fmt.Sprintf("Service with ID %s not found", e.ID)
}

type ServicesRegistry struct {
	csiShim           csishim.Csi
	grpcShim          grpcshim.Grpc
	services          []Service
	identityClients   map[string]csi.IdentityClient
	controllerClients map[string]csi.ControllerClient
}

func NewServicesRegistry(
	csiShim csishim.Csi,
	grpcShim grpcshim.Grpc,
	serviceSpecPath string,
	logger lager.Logger,
) (*ServicesRegistry, error) {
	serviceSpec, err := ioutil.ReadFile(serviceSpecPath)

	if err != nil {
		logger.Error("failed-to-read-service-spec", err, lager.Data{"fileName": serviceSpecPath})
		return nil, err
	}

	var services []Service

	err = json.Unmarshal(serviceSpec, &services)
	if err != nil {
		logger.Error("failed-to-unmarshall-spec from spec-file", err, lager.Data{"fileName": serviceSpecPath})
		return nil, ErrInvalidSpecFile{err}
	}
	logger.Info("spec-loaded", lager.Data{"fileName": serviceSpecPath})

	if len(services) < 1 {
		logger.Error("invalid-service-spec-file", ErrEmptySpecFile, lager.Data{"fileName": serviceSpecPath})
		return nil, ErrEmptySpecFile
	}

	for i, service := range services {
		if service.ID == "" || service.Name == "" || service.Description == "" || service.Plans == nil {
			err = ErrInvalidService{Index: i}
			logger.Error("invalid-service-spec-file", err, lager.Data{"fileName": serviceSpecPath, "index": i, "service": service})
			return nil, err
		}
	}

	return &ServicesRegistry{
		csiShim:           csiShim,
		grpcShim:          grpcShim,
		services:          services,
		identityClients:   map[string]csi.IdentityClient{},
		controllerClients: map[string]csi.ControllerClient{},
	}, nil
}

func (r *ServicesRegistry) IdentityClient(serviceID string) (csi.IdentityClient, error) {
	service, found := r.findServiceByID(serviceID)
	if !found {
		return nil, ErrServiceNotFound{ID: serviceID}
	}

	if service.ConnAddr == "" {
		return new(NoopIdentityClient), nil
	}

	conn, err := r.grpcShim.Dial(service.ConnAddr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	return r.csiShim.NewIdentityClient(conn), nil
}

func (r *ServicesRegistry) ControllerClient(serviceID string) (csi.ControllerClient, error) {
	service, found := r.findServiceByID(serviceID)
	if !found {
		return nil, ErrServiceNotFound{ID: serviceID}
	}

	if service.ConnAddr == "" {
		return new(NoopControllerClient), nil
	}

	conn, err := r.grpcShim.Dial(service.ConnAddr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	return r.csiShim.NewControllerClient(conn), nil
}

func (r *ServicesRegistry) BrokerServices() []brokerapi.Service {
	var brokerServices []brokerapi.Service
	for _, s := range r.services {
		brokerServices = append(brokerServices, s.Service)
	}

	return brokerServices
}

func (r *ServicesRegistry) DriverName(serviceID string) (string, error) {
	service, found := r.findServiceByID(serviceID)
	if !found {
		return "", ErrServiceNotFound{ID: serviceID}
	}

	return service.DriverName, nil
}

func (r *ServicesRegistry) findServiceByID(serviceID string) (Service, bool) {
	for _, service := range r.services {
		if service.ID == serviceID {
			return service, true
		}
	}

	return Service{}, false
}
