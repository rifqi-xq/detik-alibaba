package kafkaproducer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/inconshreveable/log15"
)

// KafkaProducer holds Kafka producer configuration and client
type KafkaProducer struct {
	producer  *kafka.Producer
	topicName string
}

// KafkaConfig holds the necessary configurations for Kafka
type KafkaConfig struct {
	BootstrapServers string
	SASLUsername     string
	SASLPassword     string
	CALocation       string
	TopicName        string
}

// NewKafkaProducer initializes a new Kafka producer for Alibaba Cloud Kafka
func NewKafkaProducer(ctx context.Context, config KafkaConfig) (*KafkaProducer, error) {
	if config.BootstrapServers == "" || config.SASLUsername == "" || config.SASLPassword == "" || config.TopicName == "" {
		log15.Error("Invalid Kafka configuration provided")
		return nil, errors.New("missing required Kafka configuration")
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     config.BootstrapServers,
		"sasl.mechanisms":                       "PLAIN",
		"security.protocol":                     "SASL_SSL",
		"ssl.ca.location":                       config.CALocation,
		"sasl.username":                         config.SASLUsername,
		"sasl.password":                         config.SASLPassword,
		"ssl.endpoint.identification.algorithm": "none", // Disable hostname verification for Alibaba Cloud Kafka
	})
	if err != nil {
		log15.Error("Failed to initialize Kafka producer: " + err.Error())
		return nil, err
	}

	kp := &KafkaProducer{
		producer:  producer,
		topicName: config.TopicName,
	}

	return kp, nil
}

// Close closes the Kafka producer connection
func (kp *KafkaProducer) Close() error {
	log15.Info("Closing Kafka producer...")
	kp.producer.Close()
	return nil
}

// Publish sends a message to the Kafka topic
func (kp *KafkaProducer) Publish(messageJSON []byte) error {
	deliveryChan := make(chan kafka.Event, 1)
	defer close(deliveryChan)

	err := kp.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kp.topicName, Partition: kafka.PartitionAny},
		Value:          messageJSON,
	}, deliveryChan)

	if err != nil {
		log15.Error("Failed to send message: " + err.Error())
		return err
	}

	e := <-deliveryChan
	msg := e.(*kafka.Message)
	if msg.TopicPartition.Error != nil {
		log15.Error("Delivery failed: " + msg.TopicPartition.Error.Error())
		return msg.TopicPartition.Error
	}

	log15.Info("Message delivered to topic: " + *msg.TopicPartition.Topic)
	return nil
}

// FetchDataFromAPI simulates fetching data from an API (similar to the Python version)
func FetchDataFromAPI(apiURL string) ([]byte, error) {
	// Placeholder: Use actual HTTP requests to fetch from an API endpoint.
	// Example: Use "net/http" to send a GET request to apiURL and return response body.
	log15.Info(fmt.Sprintf("Fetching data from API: %s", apiURL))

	// For demonstration, return dummy JSON data
	return []byte(`{"example": "data from API"}`), nil
}

// Example of usage (optional: simulate main)
func Example() {
	ctx := context.Background()
	config := KafkaConfig{
		BootstrapServers: "your-kafka-bootstrap-servers",
		SASLUsername:     "your-kafka-username",
		SASLPassword:     "your-kafka-password",
		CALocation:       "your-ca-location",
		TopicName:        "your-topic-name",
	}

	// Initialize Kafka producer
	kafkaProducer, err := NewKafkaProducer(ctx, config)
	if err != nil {
		log15.Crit("Failed to initialize Kafka producer: ", err)
		return
	}
	defer kafkaProducer.Close()

	// Simulate API fetching and publishing loop
	apiURL := "http://127.0.0.1:8000/stream-data"
	for {
		data, err := FetchDataFromAPI(apiURL)
		if err != nil {
			log15.Error("Failed to fetch data from API: " + err.Error())
			time.Sleep(5 * time.Second)
			continue
		}

		err = kafkaProducer.Publish(data)
		if err != nil {
			log15.Error("Failed to publish data to Kafka: " + err.Error())
		}

		time.Sleep(5 * time.Second) // Simulate periodic publishing
	}
}
