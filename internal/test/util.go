// Package test for the application
package test

import (
	"errors"
	"fmt"
	"github.com/Jeffail/benthos/v3/lib/broker"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/rs/zerolog/log"
)

const invalid = "invalid_"

// ExpectThen returns the next func
func ExpectThen(msg string, next func() error) error {
	if msg != "" {
		return errors.New(msg)
	}
	return next()
}

// Expect returns the next func
func Expect(msg string) error {
	if msg != "" {
		return errors.New(msg)
	}
	return nil
}

// DoThen returns the next func
func DoThen(err error, next func() error) error {
	if err != nil {
		return err
	}
	return next()
}

// Message holds a message
type Message struct {
	id       string
	packetID string
	deviceID int
	valid    bool
	retry    bool
	samples  int
}

// ToJSON return a msg in JSON
func (p *Message) ToJSON() string {
	channels := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}
	rawValues := []float64{15.0, 23.0, 3.32, 4.0, 43.34, 3.34, 43.34, 3.2, 5.3, 45.4, 4.3, 2.3, 4.6}
	msg := fmt.Sprintf(`
{
  "packetId": "%s",
  "deviceId": %d%s,
  "samples": [ `+func() string {
		samples := ""
		for i := 0; i < p.samples; i++ {
			if i > 0 {
				samples += `,`
			}
			samples += `{
			    "timestamp": 1590702281,
				"rawValue": ` + fmt.Sprintf("%f", rawValues[i]) + `,
				"channelId": ` + strconv.Itoa(channels[i]) + `
		}`
		}
		return samples
	}()+`
  ]
}
`, p.packetID, p.deviceID, func(v bool) string {
		if !v {
			return invalid
		}
		return ""
	}(p.valid))

	if p.retry {
		msg = fmt.Sprintf(`
{ 
   "timestamp" : %d,
   "content" : %s,
   "num_retries": 2
}`, time.Now().Unix(), msg)
	}

	return msg
}

// ScenarioData holds data from scenario
type ScenarioData struct {
	Messages []*Message
	MsgCount int
	Subject  interface{}
}

// World holds global vars to be used in tests
type World struct {
	serverURL     string
	client        *Client
	kafkaClient   *broker.KafkaClient
	storageClient *storage.Client
	data          *ScenarioData
	containers    []string
	tables        []string
	t             *testing.T
}

// messages returns the msgs as strings
func (w *World) messages() []*string {
	msgs := make([]*string, len(w.data.Messages))
	for i := range w.data.Messages {
		msg := w.data.Messages[i]
		s := msg.ToJSON()
		msgs[i] = &s
	}
	return msgs
}

// NewWorld creates a new World structure
func NewWorld(t *testing.T, serverURL string, kafkaPort int, azureConnectString string) *World {
	storageClient, err := storage.NewClientFromConnectionString(azureConnectString)
	if err != nil {
		log.Error().Msgf("error establishing connection to azure storage: %v", err)
	}
	return &World{
		serverURL:     serverURL,
		client:        NewClient(serverURL),
		kafkaClient:   broker.NewKafkaClient(kafkaPort),
		storageClient: &storageClient,
		data:          &ScenarioData{},
		containers:    []string{"dead-letter-readings-digestor"},
		tables:        []string{"Readings", "LastValues", "BatteryData", "NetworkQuality", "VolumeReadings"},
		t:             t,
	}
}

func (w *World) findContainer(containerName string) bool {
	for _, container := range w.containers {
		if strings.Contains(containerName, container) {
			return true
		}
	}
	return false
}

// DeleteAllTopics deletes all the Kafka topics
func (w *World) DeleteAllTopics() {
	w.kafkaClient.DeleteAllTopics() //nolint
}

// DeleteBlobs deletes all the storage blobs
func (w *World) DeleteBlobs() {
	blobService := w.storageClient.GetBlobService()
	resp, _ := blobService.ListContainers(storage.ListContainersParameters{})
	for i := range resp.Containers {
		c := &resp.Containers[i]
		if found := w.findContainer(c.Name); found {
			bresp, _ := c.ListBlobs(storage.ListBlobsParameters{})
			for i := range bresp.Blobs {
				b := &bresp.Blobs[i]
				b.Delete(&storage.DeleteBlobOptions{}) //nolint
			}
		}
	}
}

// DeleteStorageTables deletes all the storage tables
func (w *World) DeleteStorageTables() {
	tableService := w.storageClient.GetTableService()
	for i := range w.tables {
		t := &w.tables[i]
		table := tableService.GetTableReference(*t)
		table.Delete(10, &storage.TableOptions{}) //nolint
	}
}

// DeleteTableRecords deletes all the records in storage tables
func (w *World) DeleteTableRecords() {
	const timeout = 10
	tableService := w.storageClient.GetTableService()
	for i := range w.tables {
		t := &w.tables[i]
		table := tableService.GetTableReference(*t)
		if table != nil {
			res, err := table.QueryEntities(timeout, storage.FullMetadata, &storage.QueryOptions{})
			if err != nil {
				return
			}
			for _, e := range res.Entities {
				e.Delete(true, &storage.EntityOptions{}) //nolint
			}
		}
	}
}

// DeleteStorageContainers deletes all the storage containers
func (w *World) DeleteStorageContainers() {
	blobService := w.storageClient.GetBlobService()
	resp, _ := blobService.ListContainers(storage.ListContainersParameters{})
	for i := range resp.Containers {
		c := &resp.Containers[i]
		if found := w.findContainer(c.Name); found {
			c.Delete(&storage.DeleteContainerOptions{}) // nolint
		}
	}
}

// NewData clears the Data
func (w *World) NewData() {
	w.DeleteTableRecords()
	w.DeleteBlobs()
	w.data = &ScenarioData{}
}
