// test package for the application
package test

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/Jeffail/benthos/v3/lib/service"
	"github.com/cucumber/godog"
	"github.com/cucumber/godog/colors"
	_ "github.com/mfamador/datawebhooks/processor"
	"github.com/ory/dockertest/v3"
	"github.com/rs/zerolog/log"
)

const (
	kafkatype       = "kafka"
	zookeepertype   = "zookeeper"
	subscribersType = "subscribers"

	trueconst = "true"
)

var (
	opts      = godog.Options{Output: colors.Colored(os.Stdout)}
	serverURL *string
	w         *World
	tt        testing.T
)

func init() {
	serverURL = flag.String("server-url", "http://localhost:44195", "Benthos HTTP Input")
	godog.BindFlags("godog.", flag.CommandLine, &opts)
}

func Test_ATtest(t *testing.T) {
	tt = *t //nolint
}

func TestMain(m *testing.M) {
	flag.Parse()
	opts.Paths = flag.Args()
	os.Args = []string{""}

	if run := os.Getenv("GO_TEST_RUN_INTEGRATION_TESTS"); run == trueconst {
		status := godog.TestSuite{
			Name:                 "data-webhooks",
			TestSuiteInitializer: InitializeTestSuite,
			ScenarioInitializer:  InitializeScenario,
			Options:              &opts,
		}.Run()
		if st := m.Run(); st > status {
			status = st
		}
		os.Exit(status)
	}
}

func overrideConf(c *config.Type) {
	_, err := config.Read("../config/pipeline.yaml", true, c)
	if err != nil {
		log.Error().Msg(fmt.Sprintf("error loading conf file: %v", err))
	}
}

func runBenthos(containers map[string]struct { //nolint:gocyclo
	Resource *dockertest.Resource
	Port     *int
}) {
	if err := os.Setenv("SUBSCRIBERS_SERVICE_ADDRESS", fmt.Sprintf("localhost:%d", *containers[subscribersType].Port)); err != nil {
		log.Error().Msg(fmt.Sprintf("error setting env var: %v", err))
	}
	if err := os.Setenv("KAFKA_ADDRESS", fmt.Sprintf("localhost:%d", *containers[kafkatype].Port)); err != nil {
		log.Error().Msg(fmt.Sprintf("error setting env var: %v", err))
	}
	if err := os.Setenv("KAFKA_RETRIES_ADDRESS", fmt.Sprintf("localhost:%d", *containers[kafkatype].Port)); err != nil {
		log.Error().Msg(fmt.Sprintf("error setting env var: %v", err))
	}
	if err := os.Setenv("KAFKA_PASSWORD", ""); err != nil {
		log.Error().Msg(fmt.Sprintf("error setting env var: %v", err))
	}
	if err := os.Setenv("KAFKA_TLS_ENABLED", "false"); err != nil {
		log.Error().Msg(fmt.Sprintf("error setting env var: %v", err))
	}
	if err := os.Setenv("KAFKA_SASL_MECHANISM", ""); err != nil {
		log.Error().Msg(fmt.Sprintf("error setting env var: %v", err))
	}
	if err := os.Setenv("HTTP_ADDRESS", "0.0.0.0:44195"); err != nil {
		log.Error().Msg(fmt.Sprintf("error setting env var: %v", err))
	}
	if err := os.Setenv("JSON_SCHEMA_PATH", "../config/json-schemas/readings/readings.json"); err != nil {
		log.Error().Msg(fmt.Sprintf("error setting env var: %v", err))
	}
	if err := os.Setenv("AZURE_STORAGE_CONNECTION_STRING", os.Getenv("INTEGRATION_TEST_STORAGECONNECTIONSTRING")); err != nil {
		log.Error().Msg(fmt.Sprintf("error setting env var: %v", err))
	}
	if err := os.Setenv("MAX_RETRIES", "2"); err != nil {
		log.Error().Msg(fmt.Sprintf("error setting env var: %v", err))
	}
	if err := os.Setenv("RETRY_INTERVAL", "0"); err != nil {
		log.Error().Msg(fmt.Sprintf("error setting env var: %v", err))
	}
	var d *string
	service.RunWithOpts(service.OptOverrideConfigDefaults(overrideConf),
		service.OptAddStringFlag("-test.timeout", "-test.timeout", []string{"test.timeout"}, "10", d))
}

func startDockerContainers(pool *dockertest.Pool) (map[string]struct {
	Resource *dockertest.Resource
	Port     *int
}, error) {
	dt := dockertestdata.NewDockerTestContainer(pool)
	dt.Expire = 600

	deviceIdentity, deviceIdentityPort, err := dt.RunDeviceIdentityMock(pool)
	if err != nil {
		log.Error().Msgf("Could not start device identity mock: %s", err)
		return nil, err
	}

	variables, variablesPort, err := dt.RunVariablesMock(pool)
	if err != nil {
		log.Error().Msgf("Could not start variables mock: %s", err)
		return nil, err
	}

	redis, redisPort, err := dt.RunRedis(pool)
	if err != nil {
		log.Error().Msgf("Could not start redis: %s", err)
		return nil, err
	}

	zookeeper, za, err := dt.RunZookeeper(pool)
	if err != nil {
		log.Error().Msgf("Could not start zookeeper: %s", err)
		return nil, err
	}

	kafka, kafkaPort, err := dt.RunKafka(pool, za)
	if err != nil {
		log.Error().Msgf("Could not start kafka: %s", err)
		return nil, err
	}
	time.Sleep(5 * time.Second)

	identity, identityPort, err := dt.RunIdentityMock(pool)
	if err != nil {
		log.Error().Msgf("Could not start identity mock: %s", err)
		return nil, err
	}
	time.Sleep(time.Second)
	containers := map[string]struct {
		Resource *dockertest.Resource
		Port     *int
	}{
		redistype:          {Resource: redis, Port: redisPort},
		kafkatype:          {Resource: kafka, Port: kafkaPort},
		identitytype:       {Resource: identity, Port: identityPort},
		subscribersType:    {Resource: variables, Port: variablesPort},
		deviceidentitytype: {Resource: deviceIdentity, Port: deviceIdentityPort},
		zookeepertype:      {Resource: zookeeper, Port: nil},
	}
	return containers, err
}

func startBenthos(containers map[string]struct {
	Resource *dockertest.Resource
	Port     *int
}, ready, cancel chan int) {
	go runBenthos(containers)
	c := NewClient(*serverURL)
	for times := 0; times < 20; times++ {
		c.Get("/ready")
		err := c.Err
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	ready <- 0
	<-cancel
}

func purgeDockerContainers(pool *dockertest.Pool, containers map[string]struct {
	Resource *dockertest.Resource
	Port     *int
}) {
	for _, resource := range containers {
		if perr := pool.Purge(resource.Resource); perr != nil {
			log.Error().Msg("Could not purge zookeeper")
		}
	}
	time.Sleep(time.Second)
}

func InitializeTestSuite(ctx *godog.TestSuiteContext) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Error().Msgf("Could not connect to docker: %s", err)
		return
	}

	// start mock containers for integration tests
	containers, err := startDockerContainers(pool)
	if err != nil {
		log.Error().Msgf("Could not start containers: %s", err)
		return
	}
	if containers == nil {
		log.Error().Msg("Could not start containers!!")
		return
	}
	// start Benthos with Readings Digestor pipeline
	ready, cancel := make(chan int), make(chan int)
	go startBenthos(containers, ready, cancel)
	<-ready
	close(ready)

	// create World object to keep integration tests state
	w = NewWorld(&tt, *serverURL, *containers["kafka"].Port, os.Getenv("INTEGRATION_TEST_STORAGECONNECTIONSTRING"))

	ctx.BeforeSuite(func() {
		err := w.TheServiceIsUp()
		if err != nil {
			log.Error().AnErr("Error before scenario:", err)
		}
	})

	ctx.AfterSuite(func() {
		// remove the mock docker containers
		purgeDockerContainers(pool, containers)

		if run := os.Getenv("KEEP_STORAGE_CONTAINERS"); run != trueconst {
			// delete the storage containers
			w.DeleteStorageContainers()
			w.DeleteStorageTables()
		}
	})
}

func InitializeScenario(ctx *godog.ScenarioContext) {
	ctx.BeforeScenario(func(s *godog.Scenario) {
		// clean integration tests data before each scenario
		w.NewData()
	})

	ctx.AfterScenario(func(s *godog.Scenario, err error) {
	})

	ctx.BeforeStep(func(s *godog.Step) {
	})

	ctx.AfterStep(func(s *godog.Step, err error) {
	})

	ctx.Step(`^I query the health endpoint$`, w.IQueryTheHealthEndpoint)
	ctx.Step(`^I query the readiness endpoint$`, w.IQueryTheReadinessEndpoint)
	ctx.Step(`^I should have status code (\d+)$`, w.IShouldHaveStatusCode)

	ctx.Step(`^(\d+) (valid|invalid) (retry|message)(s|) with (nonexistent |)deviceID (\d+) and (\d+) sample(s|)(| each)$`, w.MessagesWithDeviceAndSamples) //nolint
	ctx.Step(`^The message(s|) (is|are) sent to (.*) topic$`, w.MessagesSentToTopic)
	ctx.Step(`^I should have (\d+) message(s|) in (.*) topic$`, w.IShouldHaveMessagesInTheTopic)
	ctx.Step(`^I should have (\d+) record(s|) in (.*) table`, w.IShouldHaveRecordsInTheTable)
	ctx.Step(`^I should have (\d+) message(s|) in (.*) queue$`, w.IShouldHaveMessagesInTheQueue)
	ctx.Step(`^The (.*) service is down$`, w.TheServiceIsDown)
	ctx.Step(`^The message is retried for the last time`, w.TheMessageIsRetriedLastTime)
}
