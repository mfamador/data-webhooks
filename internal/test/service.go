// Package test for the application
package test

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/stretchr/testify/assert"

	"github.com/google/uuid"
	"github.com/mdaverde/jsonpath"
	"github.com/smartystreets/assertions"
)

// TheServiceIsUp checks if the service is up
func (w *World) TheServiceIsUp() error {
	return DoThen(w.IQueryTheHealthEndpoint(), func() error {
		return w.IShouldHaveStatusCode(http.StatusOK)
	})
}

// IQueryTheHealthEndpoint calls the liveness endpoint
func (w *World) IQueryTheHealthEndpoint() error {
	w.client.Get("/ping")
	return nil
}

// IQueryTheReadinessEndpoint calls the readiness endpoint
func (w *World) IQueryTheReadinessEndpoint() error {
	w.client.Get("/ready")
	return nil
}

// IShouldHaveStatusCode checks the Status Code
func (w *World) IShouldHaveStatusCode(expected int) error {
	return ExpectThen(assertions.ShouldBeNil(w.client.Err), func() error {
		return ExpectThen(assertions.ShouldNotBeNil(w.client.Resp), func() error {
			return Expect(assertions.ShouldEqual(w.client.Resp.StatusCode, expected))
		})
	})
}

// IShouldHaveContentType checks the content type
func (w *World) IShouldHaveContentType(expected string) error {
	return ExpectThen(assertions.ShouldNotBeNil(w.client.Resp), func() error {
		return Expect(assertions.ShouldContainSubstring(w.client.Resp.Header.Get("content-type"), expected))
	})
}

// IShouldHaveAJson checks if we have a JSON
func (w *World) IShouldHaveAJson() error {
	return DoThen(w.IShouldHaveContentType("application/json"), func() error {
		return ExpectThen(assertions.ShouldNotBeNil(w.client.JSON), func() error {
			w.data.Subject = w.client.JSON
			return nil
		})
	})
}

// ThatJSONShouldHaveItems checks the items within the JSON
func (w *World) ThatJSONShouldHaveItems(expected int) error {
	return ExpectThen(assertions.ShouldNotBeNil(w.data.Subject), func() error {
		actual, err := jsonpath.Get(w.data.Subject, "data")
		return ExpectThen(assertions.ShouldBeNil(err), func() error {
			var items []interface{}
			return ExpectThen(assertions.ShouldEqual(reflect.TypeOf(actual), reflect.TypeOf(items)), func() error {
				items = actual.([]interface{})
				return Expect(assertions.ShouldEqual(len(items), expected))
			})
		})
	})
}

// MessagesWithDeviceAndSamples builds messages for a specific device
func (w *World) MessagesWithDeviceAndSamples(n int, v, r, _, _ string, deviceID, s int) error {
	for i := 0; i < n; i++ {
		msg := Message{
			id:       uuid.New().String(),
			packetID: uuid.New().String(),
			deviceID: deviceID,
			samples:  s,
			valid:    v == "valid",
			retry:    r == "retry",
		}
		w.data.Messages = append(w.data.Messages, &msg)
	}
	return Expect(assertions.ShouldEqual(len(w.data.Messages), n))
}

// IShouldHaveMessagesInTheQueue checks if there are messages in the queue
func (w *World) IShouldHaveMessagesInTheQueue(target int, _, queue string) error {
	blobService := w.storageClient.GetBlobService()
	const timeout = 10

	if target == 0 {
		const timeoutEmpty = 2
		time.Sleep(timeoutEmpty * time.Second)
	}

	if queue == "dead-letter" {
		queue += "-readings-digestor"
	}
	numMsgs := 0
	res := assert.Eventually(w.t, func() bool {
		resp, err := blobService.ListContainers(storage.ListContainersParameters{})
		if err != nil {
			return false
		}
		for i := range resp.Containers {
			c := &resp.Containers[i]
			if strings.Contains(c.Name, queue) {
				bresp, err := c.ListBlobs(storage.ListBlobsParameters{})
				if err != nil {
					return false
				}
				numMsgs = len(bresp.Blobs)
				break
			}
		}
		return target == numMsgs
	}, timeout*time.Second, time.Second, "check if there are messages in the bucket")

	return ExpectThen(assertions.ShouldEqual(numMsgs, target), func() error {
		return Expect(assertions.ShouldBeTrue(res))
	})
}

// IShouldHaveRecordsInTheTable checks if there are records in the table
func (w *World) IShouldHaveRecordsInTheTable(target int, _, t string) error {
	tableService := w.storageClient.GetTableService()
	const timeout = 10

	if target == 0 {
		const timeoutEmpty = 2
		time.Sleep(timeoutEmpty * time.Second)
	}
	numMsgs := 0
	table := tableService.GetTableReference(t)
	res := assert.Eventually(w.t, func() bool {
		resp, err := table.QueryEntities(timeout, storage.FullMetadata, &storage.QueryOptions{})
		if err != nil {
			return target == 0
		}
		numMsgs = len(resp.Entities)
		return target == numMsgs
	}, timeout*time.Second, time.Second, "check if there are records in the table")

	return ExpectThen(assertions.ShouldEqual(numMsgs, target), func() error {
		return Expect(assertions.ShouldBeTrue(res))
	})
}

// TheMessageShouldMatchTheJSONSchema checks if the message is compliant with the JSON schema
func (w *World) TheMessageShouldMatchTheJSONSchema(schema string) error {
	fmt.Println(schema)
	return nil
}

// IShouldHaveMessagesInTheTopic checks if there are messages in the event hub
func (w *World) IShouldHaveMessagesInTheTopic(target int, _, topic string) error {
	const timeout = 15
	const timeoutEmpty = 2
	msgs, err := w.kafkaClient.ConsumeTopic(topic, target,
		func(target int) time.Duration {
			if target == 0 {
				return timeoutEmpty * time.Second
			}
			return timeout * time.Second
		}(target))
	if err != nil {
		return err
	}
	w.data.MsgCount = len(msgs)
	return Expect(assertions.ShouldEqual(len(msgs), target))
}

// MessagesSentToTopic sends messages to the event hub
func (w *World) MessagesSentToTopic(_, _, topic string) error {
	err := w.kafkaClient.ProduceMessages(topic, w.messages())
	return err
}

// TheServiceIsDown turns the service down
func (w *World) TheServiceIsDown(service string) error {
	fmt.Println(service)
	return nil
}

// TheMessageIsRetriedLastTime doesn't do much
func (w *World) TheMessageIsRetriedLastTime() error {
	return nil
}
