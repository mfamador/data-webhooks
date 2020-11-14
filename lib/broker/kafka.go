// Package broker for the broker utilities
package broker

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rs/zerolog/log"
)

// KafkaClient allow us to produce, consume and delete msgs in a Kafka cluster
type KafkaClient struct {
	brokers []string
}

// NewKafkaClient instantiates a new KafkaClient
func NewKafkaClient(kafkaPort int) *KafkaClient {
	return &KafkaClient{
		brokers: []string{fmt.Sprintf("localhost:%d", kafkaPort)},
	}
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready    chan bool
	messages []string
	target   int
	cancel   context.CancelFunc
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		session.MarkMessage(message, "")
		if string(message.Value) != "{\"keep\":\"alive\"}" {
			consumer.messages = append(consumer.messages, string(message.Value))
			if consumer.target != 0 && consumer.target == len(consumer.messages) {
				consumer.cancel()
			}
		}
	}
	return nil
}

// ConsumeTopic consumes messages from a topic for some time
func (k *KafkaClient) ConsumeTopic(topic string, target int, timeout time.Duration) ([]string, error) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	ctx, cancel := context.WithCancel(context.Background())
	consumer := Consumer{
		ready:  make(chan bool),
		target: target,
		cancel: cancel,
	}
	group := "test-cg"
	client, err := sarama.NewConsumerGroup(k.brokers, group, config)
	if err != nil {
		return nil, err
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, []string{topic}, &consumer); err != nil {
				log.Error().Msgf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()
	<-consumer.ready

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
	case <-sigterm:
	case <-time.After(timeout):
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Error().Msgf("Error closing client: %v", err)
	}
	return consumer.messages, nil
}

// ProduceMessages produces messages in a Kafka topic
func (k *KafkaClient) ProduceMessages(topic string, messages []*string) error {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewSyncProducer(k.brokers, config)
	if err != nil {
		return err
	}
	defer func() {
		producer.Close() //nolint
	}()

	for _, message := range messages {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(*message),
		}
		_, _, err = producer.SendMessage(msg)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteAllTopics deletes all topics in a Kafka cluster
func (k *KafkaClient) DeleteAllTopics() error {
	config := sarama.NewConfig()
	clusterAdmin, err := sarama.NewClusterAdmin(k.brokers, config)
	if err != nil {
		return err
	}
	defer func() {
		clusterAdmin.Close() //nolint
	}()
	cluster, err := sarama.NewConsumer(k.brokers, config)
	if err != nil {
		return err
	}
	defer func() {
		cluster.Close() //nolint
	}()

	topics, err := cluster.Topics()
	if err != nil {
		return err
	}
	for index := range topics {
		err = clusterAdmin.DeleteTopic(topics[index])
		if err != nil {
			log.Debug().Msgf("error deleting topic %s: %v", topics[index], err)
			return err
		}
	}

	return nil
}
