package main

import (
	"encoding/json"
	"errors"
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

	"code.cloudfoundry.org/csishim"
	"code.cloudfoundry.org/service-broker-store/brokerstore"
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

var serviceSpec = flag.String(
	"serviceSpec",
	"",
	"[REQUIRED] - the file path of the specfile which defines the service",
)

var csiConAddr = flag.String(
	"csiConAddr",
	"",
	"[REQUIRED] - address por CSI controller is listen to",
)

var driverName = flag.String(
	"driverName",
	"",
	"[REQUIRED] - driver name of CSI plugin",
)

var dbDriver = flag.String(
	"dbDriver",
	"",
	"(optional) database driver name when using SQL to store broker state",
)

var dbHostname = flag.String(
	"dbHostname",
	"",
	"(optional) database hostname when using SQL to store broker state",
)
var dbPort = flag.String(
	"dbPort",
	"",
	"(optional) database port when using SQL to store broker state",
)

var dbName = flag.String(
	"dbName",
	"",
	"(optional) database name when using SQL to store broker state",
)

var dbCACert = flag.String(
	"dbCACert",
	"",
	"(optional) CA Cert to verify SSL connection",
)

var cfServiceName = flag.String(
	"cfServiceName",
	"",
	"(optional) For CF pushed apps, the service name in VCAP_SERVICES where we should find database credentials.  dbDriver must be defined if this option is set, but all other db parameters will be extracted from the service binding.",
)

var (
	dbUsername string
	dbPassword string
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
	if *dataDir == "" && *dbDriver == "" {
		fmt.Fprint(os.Stderr, "\nERROR: Either dataDir or db parameters must be provided.\n\n")
		flag.Usage()
		os.Exit(1)
	}

	if *csiConAddr == "" {
		fmt.Fprint(os.Stderr, "\nERROR:csiConAddr must be provided.\n\n")
		flag.Usage()
		os.Exit(1)
	}
	if *serviceSpec == "" {
		fmt.Fprint(os.Stderr, "\nERROR:serviceSpec must be provided.\n\n")
		flag.Usage()
		os.Exit(1)
	}
	if *driverName == "" {
		fmt.Fprint(os.Stderr, "\nERROR:driverName must be provided.\n\n")
		flag.Usage()
		os.Exit(1)
	}

}

func parseVcapServices(logger lager.Logger, os osshim.Os) {
	if *dbDriver == "" {
		logger.Fatal("missing-db-driver-parameter", errors.New("dbDriver parameter is required for cf deployed broker"))
	}

	// populate db parameters from VCAP_SERVICES and pitch a fit if there isn't one.
	services, hasValue := os.LookupEnv("VCAP_SERVICES")
	if !hasValue {
		logger.Fatal("missing-vcap-services-environment", errors.New("missing VCAP_SERVICES environment"))
	}

	stuff := map[string][]interface{}{}
	err := json.Unmarshal([]byte(services), &stuff)
	if err != nil {
		logger.Fatal("json-unmarshal-error", err)
	}

	stuff2, ok := stuff[*cfServiceName]
	if !ok {
		logger.Fatal("missing-service-binding", errors.New("VCAP_SERVICES missing specified db service"), lager.Data{"stuff": stuff})
	}

	stuff3 := stuff2[0].(map[string]interface{})

	credentials := stuff3["credentials"].(map[string]interface{})
	logger.Debug("credentials-parsed", lager.Data{"credentials": credentials})

	dbUsername = credentials["username"].(string)
	dbPassword = credentials["password"].(string)
	*dbHostname = credentials["hostname"].(string)
	if *dbPort, ok = credentials["port"].(string); !ok {
		*dbPort = fmt.Sprintf("%.0f", credentials["port"].(float64))
	}
	*dbName = credentials["name"].(string)
}

func parseEnvironment() {
	dbUsername, _ = os.LookupEnv("DB_USERNAME")
	dbPassword, _ = os.LookupEnv("DB_PASSWORD")
}

func createServer(logger lager.Logger) ifrit.Runner {
	fileName := filepath.Join(*dataDir, "csi-general-services.json")

	logger.Debug("csiConAddress: " + *csiConAddr)

	// if we are CF pushed
	if *cfServiceName != "" {
		parseVcapServices(logger, &osshim.OsShim{})
	}

	store := brokerstore.NewStore(logger, *dbDriver, dbUsername, dbPassword, *dbHostname, *dbPort, *dbName, *dbCACert, fileName)
	conn, err := grpc.Dial(*csiConAddr, grpc.WithInsecure())

	if err != nil {
		logger.Error("cannot-reach-csi-plugin", err)
		os.Exit(1)
	}

	serviceBroker, err := csibroker.New(logger,
		*serviceSpec,
		&osshim.OsShim{}, clock.NewClock(), store, &csishim.CsiShim{}, conn, *driverName)
	logger.Info("listenAddr: " + *atAddress + ", serviceSpec: " + *serviceSpec)

	if err != nil {
		logger.Error("csibroker-initialize-error", err)
		os.Exit(1)
	}

	credentials := brokerapi.BrokerCredentials{Username: *username, Password: *password}
	handler := brokerapi.New(serviceBroker, logger.Session("broker-api"), credentials)

	return http_server.New(*atAddress, handler)
}
