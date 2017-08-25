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
	"google.golang.org/grpc"
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

var username = flag.String(
	"username",
	"admin",
	"basic auth username to verify on incoming requests",
)

var password = flag.String(
	"password",
	"admin",
	"basic auth password to verify on incoming requests",
)

var planName = flag.String(
	"planName",
	"free",
	"name of the service plan to register with cloud controller",
)
var planId = flag.String(
	"planId",
	"free-plan-guid",
	"ID of the service plan to register with cloud controller",
)
var planDesc = flag.String(
	"planDesc",
	"free csi local filesystem",
	"description of the service plan to register with cloud controller",
)

var csiConAddr = flag.String(
	"csiConAddr",
	"",
	"[REQUIRED] - address por CSI controller is listen to",
)

func main() {
	parseCommandLine()
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
			{Name: "debug-server", Runner: debugserver.Runner(dbgAddr, logSink)},
			{Name: "broker-api", Runner: server},
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

func checkParams() {
	if *dataDir == "" {
		fmt.Fprint(os.Stderr, "\nERROR:dataDir must be provided.\n\n")
		flag.Usage()
		os.Exit(1)
	}
	if *csiConAddr == "" {
		fmt.Fprint(os.Stderr, "\nERROR:csiConAddr must be provided.\n\n")
		flag.Usage()
		os.Exit(1)
	}
}

func createServer(logger lager.Logger) ifrit.Runner {
	fileName := filepath.Join(*dataDir, fmt.Sprintf("%s-services.json", *serviceName))

	logger.Debug("csiConAddress: " + *csiConAddr)
	store := csibroker.NewFileStore(fileName, &ioutilshim.IoutilShim{})
	conn, err := grpc.Dial(*csiConAddr, grpc.WithInsecure())

	if err != nil {
		logger.Error("Cannot reach csi plugin", err)
		os.Exit(1)
	}

	serviceBroker := csibroker.New(logger,
		*serviceName, *serviceId, *planName, *planId, *planDesc,
		&osshim.OsShim{}, clock.NewClock(), store, &csishim.CsiShim{}, conn)
	logger.Info("listenAddr: " + *atAddress + ", serviceName: " + *serviceName + ", serviceId: " + *serviceId + ", planName: " + *planName + ", planId: " + *planId + ", planDesc: " + *planDesc)

	credentials := brokerapi.BrokerCredentials{Username: *username, Password: *password}
	handler := brokerapi.New(serviceBroker, logger.Session("broker-api"), credentials)

	return http_server.New(*atAddress, handler)
}
