package services

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/inconshreveable/log15"
)

// Service handles application business logic directly with Kafka
type Service struct {
	kafkaProducer *kafka.Producer
	kafkaConsumer *kafka.Consumer
	topicGET      string
	topicPOST     string
	ctx           context.Context
	cancel        context.CancelFunc
}

// ServiceInterface defines service functions
type ServiceInterface interface {
	Start()
	Stop()
	AddCollectorGETJob(payload map[string][]string)
	AddCollectorPOSTJob(payload string)
}

// NewService initializes the Service instance
func NewService(kafkaBootstrapServers, kafkaTopicGET, kafkaTopicPOST string) (*Service, error) {
	// Create Kafka producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	// Create Kafka consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
		"group.id":          "analytic-collector-group",
		"auto.offset.reset": "latest",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	// Context for managing consumer loop
	ctx, cancel := context.WithCancel(context.Background())

	return &Service{
		kafkaProducer: producer,
		kafkaConsumer: consumer,
		topicGET:      kafkaTopicGET,
		topicPOST:     kafkaTopicPOST,
		ctx:           ctx,
		cancel:        cancel,
	}, nil
}

// AddCollectorGETJob sends a GET job to the Kafka topic
func (s *Service) AddCollectorGETJob(payload map[string][]string) {
	log15.Debug("Adding GET job to Kafka")
	// Serialize payload to JSON
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log15.Error("Failed to serialize GET payload", "error", err)
		return
	}

	// Send message to Kafka
	err = s.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &s.topicGET, Partition: kafka.PartitionAny},
		Value:          payloadBytes,
	}, nil)
	if err != nil {
		log15.Error("Failed to send GET job to Kafka", "error", err)
	} else {
		log15.Info("GET job successfully added to Kafka")
	}
}

// AddCollectorPOSTJob sends a POST job to the Kafka topic
func (s *Service) AddCollectorPOSTJob(payload string) {
	log15.Debug("Adding POST job to Kafka")
	// Send message to Kafka
	err := s.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &s.topicPOST, Partition: kafka.PartitionAny},
		Value:          []byte(payload),
	}, nil)
	if err != nil {
		log15.Error("Failed to send POST job to Kafka", "error", err)
	} else {
		log15.Info("POST job successfully added to Kafka")
	}
}

// Start begins the Kafka consumer loop
func (s *Service) Start() {
	log15.Info("Starting Kafka consumer")
	go func() {
		// Subscribe to Kafka topics
		err := s.kafkaConsumer.SubscribeTopics([]string{s.topicGET, s.topicPOST}, nil)
		if err != nil {
			log15.Error("Failed to subscribe to Kafka topics", "error", err)
			return
		}

		for {
			select {
			case <-s.ctx.Done():
				// Stop consuming if context is canceled
				log15.Info("Kafka consumer stopping")
				return
			default:
				// Poll for messages
				msg, err := s.kafkaConsumer.ReadMessage(-1)
				if err != nil {
					log15.Error("Error reading message from Kafka", "error", err)
					continue
				}

				// Process the message
				s.processMessage(msg)
			}
		}
	}()
}

// Stop stops the Kafka consumer and producer
func (s *Service) Stop() {
	log15.Info("Stopping Kafka producer and consumer")
	s.cancel() // Cancel the consumer context

	// Close Kafka consumer
	if err := s.kafkaConsumer.Close(); err != nil {
		log15.Error("Failed to close Kafka consumer", "error", err)
	}

	// Close Kafka producer
	s.kafkaProducer.Close()
}

// processMessage processes a single Kafka message based on its topic
func (s *Service) processMessage(msg *kafka.Message) {
	log15.Info("Processing message", "topic", *msg.TopicPartition.Topic, "value", string(msg.Value))

	switch *msg.TopicPartition.Topic {
	case s.topicGET:
		// Process GET job
		var payload map[string][]string
		if err := json.Unmarshal(msg.Value, &payload); err != nil {
			log15.Error("Failed to deserialize GET message", "error", err)
			return
		}
		log15.Info("Processed GET message", "payload", payload)

	case s.topicPOST:
		// Process POST job
		log15.Info("Processed POST message", "payload", string(msg.Value))

	default:
		log15.Warn("Received message for unknown topic", "topic", *msg.TopicPartition.Topic)
	}
}
