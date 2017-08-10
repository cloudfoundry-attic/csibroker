package main

import (
	"flag"
	"os"

	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/csibroker/csibroker"
	"code.cloudfoundry.org/csibroker/utils"
	"code.cloudfoundry.org/debugserver"
	"code.cloudfoundry.org/goshims/osshim"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagerflags"

	"fmt"
	"path/filepath"

	"code.cloudfoundry.org/goshims/ioutilshim"
	"github.com/paulcwarren/spec/csishim"
	"github.com/pivotal-cf/brokerapi"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grouper"
	"github.com/tedsuo/ifrit/http_server"
)

var dataDir = flag.String(
	"dataDir",
	"",
	"[REQUIRED] - Broker's state will be stored here to persist across reboots",
)

var atAddress = flag.String(
	"listenAddr",
	"0.0.0.0:8999",
	"host:port to serve service broker API",
)

var serviceName = flag.String(
	"serviceName",
	"brokervolume",
	"name of the service to register with cloud controller",
)
var serviceId = flag.String(
	"serviceId",
	"service-guid",
	"ID of the service to register with cloud controller",
)

var (
	username string
	password string
)

func main() {
	parseCommandLine()
	parseEnvironment()
	checkParams()

	sink, err := lager.NewRedactingWriterSink(os.Stdout, lager.DEBUG, nil, nil)
	if err != nil {
		panic(err)
	}
	logger, logSink := lagerflags.NewFromSink("csibroker", sink)
	logger.Info("starting")
	defer logger.Info("ends")

	server := createServer(logger)

	if dbgAddr := debugserver.DebugAddress(flag.CommandLine); dbgAddr != "" {
		server = utils.ProcessRunnerFor(grouper.Members{
			{"debug-server", debugserver.Runner(dbgAddr, logSink)},
			{"broker-api", server},
		})
	}

	process := ifrit.Invoke(server)
	logger.Info("started")
	utils.UntilTerminated(logger, process)
}

func parseCommandLine() {
	lagerflags.AddFlags(flag.CommandLine)
	debugserver.AddFlags(flag.CommandLine)
	flag.Parse()
}

func parseEnvironment() {
	username, _ = os.LookupEnv("USERNAME")
	password, _ = os.LookupEnv("PASSWORD")
}

func checkParams() {
	if *dataDir == "" {
		fmt.Fprint(os.Stderr, "\nERROR:dataDir must be provided.\n\n")
		flag.Usage()
		os.Exit(1)
	}
}

func createServer(logger lager.Logger) ifrit.Runner {
	fileName := filepath.Join(*dataDir, fmt.Sprintf("%s-services.json", *serviceName))

	store := csibroker.NewFileStore(fileName, &ioutilshim.IoutilShim{})

	serviceBroker := csibroker.New(logger,
		*serviceName, *serviceId,
		&osshim.OsShim{}, clock.NewClock(), store, &csishim.CsiShim{})

	logger.Info("listenAddr: " + *atAddress + ", serviceName: " + *serviceName + ", serviceId: " + *serviceId)

	credentials := brokerapi.BrokerCredentials{Username: username, Password: password}
	handler := brokerapi.New(serviceBroker, logger.Session("broker-api"), credentials)

	return http_server.New(*atAddress, handler)
}
