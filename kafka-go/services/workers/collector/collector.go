package controllers

import (
	"encoding/json"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/inconshreveable/log15"
)

// Collector defines a job collector structure
type Collector struct {
	consumer   *kafka.Consumer
	topic      string
	workerPool *WorkerPool
}

// NewCollector initializes a new Collector
func NewCollector(consumer *kafka.Consumer, topic string, workerPool *WorkerPool) *Collector {
	return &Collector{
		consumer:   consumer,
		topic:      topic,
		workerPool: workerPool,
	}
}

// Start begins consuming messages from Kafka and dispatching them to the worker pool
func (c *Collector) Start() {
	log15.Info("Starting Collector for topic", "topic", c.topic)

	go func() {
		err := c.consumer.SubscribeTopics([]string{c.topic}, nil)
		if err != nil {
			log15.Error("Failed to subscribe to topic", "topic", c.topic, "error", err)
			return
		}

		for {
			msg, err := c.consumer.ReadMessage(-1) // Blocks until a message is available
			if err != nil {
				log15.Error("Error reading message", "error", err)
				continue
			}

			var payload map[string]interface{}
			err = json.Unmarshal(msg.Value, &payload)
			if err != nil {
				log15.Error("Failed to deserialize message", "error", err, "message", string(msg.Value))
				continue
			}

			log15.Info("Message received", "topic", *msg.TopicPartition.Topic, "payload", payload)
			c.workerPool.AddJob(payload)
		}
	}()
}

// Stop gracefully stops the Collector
func (c *Collector) Stop() {
	log15.Info("Stopping Collector for topic", "topic", c.topic)
	c.consumer.Close()
}
