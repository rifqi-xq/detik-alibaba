package services

import (
	"analytic-collector/services/workers/collector"
	"context"
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/inconshreveable/log15"
)

// Service handles application business logic
type Service struct {
	collectorWorkerPool collector.WorkerPool
	kafkaProducer       *kafka.Producer
	kafkaConsumer       *kafka.Consumer
	topicGET            string
	topicPOST           string
}

// ServiceInterface defines service functions
type ServiceInterface interface {
	StartWorkers()
	StopWorkers()
	AddCollectorGETJob(payload map[string][]string)
	AddCollectorPOSTJob(payload string)
}

// NewService initializes the service
func NewService(ctx context.Context, maxCollectorWorker int, kafkaBootstrapServers, kafkaTopicGET, kafkaTopicPOST string) (*Service, error) {
	// Initialize Kafka producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	// Initialize Kafka consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
		"group.id":          "analytic-collector-group",
		"auto.offset.reset": "latest",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	// Create a worker pool for collectors
	collectorPool := collector.NewWorkerPool(maxCollectorWorker)

	return &Service{
		collectorWorkerPool: collectorPool,
		kafkaProducer:       producer,
		kafkaConsumer:       consumer,
		topicGET:            kafkaTopicGET,
		topicPOST:           kafkaTopicPOST,
	}, nil
}

// AddCollectorGETJob sends a GET job to the Kafka topic
func (s *Service) AddCollectorGETJob(payload map[string][]string) {
	log15.Debug("adding GET job to Kafka")
	// Serialize payload to JSON or a suitable format
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log15.Error("failed to serialize GET payload", "error", err)
		return
	}

	// Send message to Kafka
	err = s.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &s.topicGET, Partition: kafka.PartitionAny},
		Value:          payloadBytes,
	}, nil)
	if err != nil {
		log15.Error("failed to send GET job to Kafka", "error", err)
	} else {
		log15.Info("GET job added to Kafka")
	}
}

// AddCollectorPOSTJob sends a POST job to the Kafka topic
func (s *Service) AddCollectorPOSTJob(payload string) {
	log15.Debug("adding POST job to Kafka")
	// Send message to Kafka
	err := s.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &s.topicPOST, Partition: kafka.PartitionAny},
		Value:          []byte(payload),
	}, nil)
	if err != nil {
		log15.Error("failed to send POST job to Kafka", "error", err)
	} else {
		log15.Info("POST job added to Kafka")
	}
}

// StartWorkers starts all worker processes
func (s *Service) StartWorkers() {
	log15.Info("starting workers and Kafka consumer")
	s.collectorWorkerPool.StartWorkers()

	// Subscribe to Kafka topics
	err := s.kafkaConsumer.SubscribeTopics([]string{s.topicGET, s.topicPOST}, nil)
	if err != nil {
		log15.Error("failed to subscribe to Kafka topics", "error", err)
		return
	}

	// Start consuming messages
	go func() {
		for {
			msg, err := s.kafkaConsumer.ReadMessage(-1) // Block until a message is received
			if err != nil {
				log15.Error("error reading message from Kafka", "error", err)
				continue
			}

			// Determine the topic and forward the message to the worker pool
			switch *msg.TopicPartition.Topic {
			case s.topicGET:
				log15.Debug("received GET job from Kafka")
				s.collectorWorkerPool.AddGETJob(string(msg.Value))
			case s.topicPOST:
				log15.Debug("received POST job from Kafka")
				s.collectorWorkerPool.AddPOSTJob(string(msg.Value))
			default:
				log15.Warn("received message for unknown topic", "topic", *msg.TopicPartition.Topic)
			}
		}
	}()
}

// StopWorkers stops all worker processes and closes Kafka connections
func (s *Service) StopWorkers() {
	log15.Info("stopping workers and closing Kafka connections")
	s.collectorWorkerPool.StopWorkers()

	if err := s.kafkaConsumer.Close(); err != nil {
		log15.Error("failed to close Kafka consumer", "error", err)
	}

	s.kafkaProducer.Close()
}
