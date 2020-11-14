// Package dockertestdata for the application
package dockertestdata

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"

	"github.com/Shopify/sarama"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/rs/zerolog/log"
)

type dockerTestContainer struct {
	pool    *dockertest.Pool
	Expire  uint
	MaxWait time.Duration
}

// NewDockerTestContainer builds a new dockerTestContainer
func NewDockerTestContainer(pool *dockertest.Pool) *dockerTestContainer { // nolint
	return &dockerTestContainer{
		pool,
		900,
		60,
	}
}

func (d *dockerTestContainer) RunDeviceIdentityMock(pool *dockertest.Pool) (*dockertest.Resource, *int, error) {
	args := []string{"/protos/core/devices.proto", "/protos/core/entities/entities.proto", "/protos/core/channels.proto"}
	deviceIdentityMock, deviceIdentityPort, err := d.RunGRPCMock(pool, "deviceidentityv1", args)
	if err != nil {
		return nil, nil, err
	}

	return deviceIdentityMock, deviceIdentityPort, err
}

func (d *dockerTestContainer) RunVariablesMock(pool *dockertest.Pool) (*dockertest.Resource, *int, error) {
	args := []string{"/protos/core/variables.proto"}
	variablesMock, variablesPort, err := d.RunGRPCMock(pool, "variablesv1", args)
	if err != nil {
		return nil, nil, err
	}

	return variablesMock, variablesPort, err
}

// RunGRPCMock starts a gRPC mock container
func (d *dockerTestContainer) RunGRPCMock(pool *dockertest.Pool, mockPackage string, args []string) (*dockertest.Resource, *int, error) {
	grpcPort, err := getFreePort()
	if err != nil {
		return nil, nil, err
	}

	stubPort, err := getFreePort()
	if err != nil {
		return nil, nil, err
	}
	// This doesnt work with azure auth because it uses a token instead of user:password
	// auth, err := docker.NewAuthConfigurationsFromDockerCfg()
	// if err != nil {
	// 	return nil, err
	// }
	auth := docker.AuthConfiguration{
		Email:         "",
		Username:      "stagmapleleafeun",
		Password:      os.Getenv("ACR_TOKEN"),
		ServerAddress: "sandmapleleafeun.azurecr.io",
	}
	grpcPortStr := strconv.Itoa(grpcPort)
	stubPortStr := strconv.Itoa(stubPort)

	grpcMock, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "stagmapleleafeun.azurecr.io/mock-grpc",
		Tag:          "0.1.0",
		ExposedPorts: []string{grpcPortStr + "/tcp", stubPortStr + "/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			docker.Port(grpcPortStr + "/tcp"): {{HostIP: "", HostPort: grpcPortStr}},
			docker.Port(stubPortStr + "/tcp"): {{HostIP: "", HostPort: stubPortStr}},
		},
		Cmd: append([]string{
			"gripmock",
			"--grpc-port",
			grpcPortStr,
			"--admin-port",
			stubPortStr,
			"--package",
			mockPackage,
			"--stub",
			"/stubs",
		}, args...),
		Auth: auth,
	})
	if err != nil {
		return nil, nil, err
	}
	if eerr := grpcMock.Expire(d.Expire); eerr != nil {
		return nil, nil, eerr
	}
	if eerr := pool.Retry(func() error {
		resp, rerr := http.Get("http://localhost:" + stubPortStr)
		if resp != nil {
			resp.Body.Close() //nolint
		}
		return rerr
	}); eerr != nil {
		log.Error().AnErr("Could not connect to docker: %s", eerr)
		return nil, nil, eerr
	}

	return grpcMock, &grpcPort, nil
}

func (d *dockerTestContainer) RunIdentityMock(pool *dockertest.Pool) (*dockertest.Resource, *int, error) {
	identityPort, err := getFreePort()
	if err != nil {
		return nil, nil, err
	}
	identityPortStr := strconv.Itoa(identityPort)
	auth := docker.AuthConfiguration{
		Email:         "",
		Username:      "stagmapleleafeun",
		Password:      os.Getenv("ACR_TOKEN"),
		ServerAddress: "sandmapleleafeun.azurecr.io",
	}

	identityMock, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "stagmapleleafeun.azurecr.io/mock-identity",
		Tag:          "0.1.0",
		ExposedPorts: []string{"80/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"80/tcp": {{HostIP: "", HostPort: identityPortStr}},
		},
		Env: []string{
			"CLIENTS_CONFIGURATION_PATH=/config/clients_config.json",
			"API_SCOPES_PATH=/config/api_scopes.json",
		},
		Auth: auth,
	})

	if err != nil {
		return nil, nil, err
	}
	if eerr := identityMock.Expire(d.Expire); eerr != nil {
		return nil, nil, eerr
	}
	if eerr := pool.Retry(func() error {
		resp, rerr := http.Get("http://localhost:" + identityPortStr)
		if resp != nil {
			resp.Body.Close() //nolint
		}
		return rerr
	}); eerr != nil {
		log.Error().Msgf("Could not connect to docker: %s", eerr)
		return nil, nil, eerr
	}

	return identityMock, &identityPort, nil
}

// RunRedis starts a redis container
func (d *dockerTestContainer) RunRedis(pool *dockertest.Pool) (*dockertest.Resource, *int, error) {
	redisPort, err := getFreePort()
	if err != nil {
		return nil, nil, err
	}
	redisPortStr := strconv.Itoa(redisPort)

	redisC, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "redis",
		Tag:          "latest",
		ExposedPorts: []string{"6379/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"6379/tcp": {{HostIP: "", HostPort: redisPortStr}},
		},
	})

	if err != nil {
		return nil, nil, err
	}
	if eerr := redisC.Expire(d.Expire); eerr != nil {
		return nil, nil, eerr
	}
	var db *redis.Client
	if eerr := pool.Retry(func() error {
		db = redis.NewClient(&redis.Options{
			Addr: fmt.Sprintf("localhost:%s", redisPortStr),
		})
		return db.Ping(context.Background()).Err()
	}); eerr != nil {
		log.Error().Msgf("Could not connect to docker: %s", eerr)
		return nil, nil, eerr
	}

	return redisC, &redisPort, nil
}

// RunAzurite starts an azurite container
func (d *dockerTestContainer) RunAzurite(pool *dockertest.Pool) (*dockertest.Resource, error) {
	opts := dockertest.RunOptions{
		Repository:   "mcr.microsoft.com/azure-storage/azurite",
		Tag:          "latest",
		ExposedPorts: []string{"10000"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"10000": {{HostIP: "0.0.0.0", HostPort: "10000"}},
		},
	}
	azurite, err := pool.RunWithOptions(&opts)
	if err != nil {
		log.Error().AnErr("Could not start azurite: %s", err)
	}
	if eerr := azurite.Expire(d.Expire); eerr != nil {
		return nil, eerr
	}
	pool.MaxWait = d.MaxWait * time.Second
	rerr := pool.Retry(func() error {
		client, eerr := storage.NewEmulatorClient()
		if eerr != nil {
			return eerr
		}
		s := client.GetBlobService()
		c := s.GetContainerReference("cont")
		if _, err = c.Exists(); err != nil {
			return err
		}
		return nil
	})
	return azurite, rerr
}

// RunAzuriteTag starts an azurite container
func (d *dockerTestContainer) RunAzuriteV2(pool *dockertest.Pool) (*dockertest.Resource, error) {
	opts := dockertest.RunOptions{
		Repository:   "marcoamador/azurite",
		Tag:          "v2",
		ExposedPorts: []string{"10002"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"10002": {{HostIP: "0.0.0.0", HostPort: "10002"}},
		},
	}
	azurite, err := pool.RunWithOptions(&opts)
	if err != nil {
		log.Error().AnErr("Could not start azurite: %s", err)
	}
	if eerr := azurite.Expire(d.Expire); eerr != nil {
		return nil, eerr
	}
	pool.MaxWait = d.MaxWait * time.Second
	rerr := pool.Retry(func() error {
		client, eerr := storage.NewEmulatorClient()
		if eerr != nil {
			return eerr
		}
		s := client.GetTableService()
		c := s.GetTableReference("doesnotexist")
		const timeout = 30
		if err = c.Get(timeout, storage.FullMetadata); err != nil {
			if cerr, ok := err.(storage.AzureStorageServiceError); ok {
				if cerr.StatusCode == 404 || cerr.Code == "TableNotFound" {
					return nil
				}
			}
		}
		return nil
	})
	const secs = 5
	time.Sleep(secs * time.Second)
	return azurite, rerr
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer listener.Close() //nolint
	return listener.Addr().(*net.TCPAddr).Port, nil
}

// RunKafka starts a kafka container
func (d *dockerTestContainer) RunKafka(pool *dockertest.Pool, zkAddr string) (*dockertest.Resource, *int, error) {
	hostIP := d.getHostIP(pool)
	kafkaPort, err := getFreePort()
	if err != nil {
		return nil, nil, err
	}
	kafkaPortStr := strconv.Itoa(kafkaPort)
	env := []string{
		"KAFKA_ADVERTISED_HOST_NAME=" + hostIP,
		"KAFKA_BROKER_ID=1",
		"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=OUTSIDE:PLAINTEXT,INSIDE:PLAINTEXT",
		"KAFKA_LISTENERS=OUTSIDE://:" + kafkaPortStr + ",INSIDE://:9092",
		"KAFKA_ADVERTISED_LISTENERS=OUTSIDE://localhost:" + kafkaPortStr + ",INSIDE://" + hostIP + ":9092",
		"KAFKA_INTER_BROKER_LISTENER_NAME=INSIDE",
		"KAFKA_ZOOKEEPER_CONNECT=" + zkAddr,
	}
	kafka, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "wurstmeister/kafka",
		Tag:          "latest",
		ExposedPorts: []string{kafkaPortStr + "/tcp", "9092/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			docker.Port(kafkaPortStr + "/tcp"): {{HostIP: "", HostPort: kafkaPortStr}},
			"9092/tcp":                         {{HostIP: "", HostPort: "9092"}},
		},
		Env: env,
	})
	if err != nil {
		return nil, nil, err
	}
	if eerr := kafka.Expire(d.Expire); eerr != nil {
		return nil, nil, eerr
	}
	pool.MaxWait = 10 * time.Second
	err = pool.Retry(func() error {
		broker := sarama.NewBroker("localhost:" + kafkaPortStr)
		kafkaErr := broker.Open(nil)
		if kafkaErr != nil {
			return kafkaErr
		}
		producer, perr := sarama.NewSyncProducer([]string{"localhost:" + kafkaPortStr}, nil)
		if perr != nil {
			return perr
		}
		producer.Close() //nolint
		broker.Close()   //nolint
		return nil
	})
	return kafka, &kafkaPort, err
}

// RunZookeeper starts a zookeeper container
func (d *dockerTestContainer) RunZookeeper(pool *dockertest.Pool) (*dockertest.Resource, string, error) {
	zookeeper, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "wurstmeister/zookeeper",
		Tag:        "latest",
	})
	if err != nil {
		log.Error().AnErr("Could not get current path: %s", err)
		return nil, "", err
	}
	if err := zookeeper.Expire(d.Expire); err != nil {
		return nil, "", err
	}
	zkAddr := fmt.Sprintf("%v:2181", zookeeper.Container.NetworkSettings.IPAddress)
	return zookeeper, zkAddr, nil
}

// RunBenthos starts a benthos container
func (d *dockerTestContainer) RunBenthos(pool *dockertest.Pool,
	additionalEnv []string) (*dockertest.Resource, error) {
	const port = 9092
	hostIP := d.getHostIP(pool)
	path, err := os.Getwd()
	if err != nil {
		log.Error().AnErr("Could not get current path: %s", err)
		return nil, err
	}
	env := []string{
		"LOGLEVEL=DEBUG",
		"HTTP_ADDRESS=0.0.0.0:4195",
		"KAFKA_TLS_ENABLED=false",
		"KAFKA_USER=false",
		fmt.Sprintf("KAFKA_ADDRESS=%s:%d", hostIP, port),
		"KAFKA_USER=na",
		"KAFKA_PASSWORD=na",
		"AZURE_STORAGE_CONNECTION_STRING=UseDevelopmentStorage=true;", // nolint:lll
	}
	if len(additionalEnv) > 0 {
		env = append(env, additionalEnv...)
	}

	bropts := dockertest.RunOptions{
		Repository: "jeffail/benthos",
		Tag:        "latest",
		Env:        env,
		Cmd: []string{
			"-c", "benthos.yaml",
		},
		Mounts: []string{
			fmt.Sprintf("%s/../config/pipeline.yaml:/benthos.yaml", path),
			fmt.Sprintf("%s/../config/json-schemas/dolv3/:/json-schemas/", path)},
		ExposedPorts: []string{"4195"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"4195": {
				{HostIP: "0.0.0.0", HostPort: "44195"},
			},
		},
	}
	benthos, err := pool.RunWithOptions(&bropts)
	if err != nil {
		return nil, err
	}
	if eerr := benthos.Expire(d.Expire); eerr != nil {
		return nil, eerr
	}
	if eerr := pool.Retry(func() error {
		resp, rerr := http.Get("http://localhost:44195/ping")
		if resp != nil {
			resp.Body.Close() //nolint
		}
		return rerr
	}); eerr != nil {
		log.Error().AnErr("Could not connect to docker: %s", eerr)
		return nil, eerr
	}

	return benthos, err
}

func (d *dockerTestContainer) getHostIP(pool *dockertest.Pool) string {
	networks, _ := pool.Client.ListNetworks()
	hostIP := ""
	for i := range networks {
		network := &networks[i]
		if network.Name == "bridge" {
			hostIP = network.IPAM.Config[0].Gateway
		}
	}
	return hostIP
}
