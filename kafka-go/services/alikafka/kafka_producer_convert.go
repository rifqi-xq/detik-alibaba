package kafkaproducer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Configuration structure
type Config struct {
	BootstrapServers string
	CALocation       string
	Username         string
	Password         string
	TopicName        string
}

// Load Kafka configuration
func loadConfig() Config {
	return Config{
		BootstrapServers: os.Getenv("BOOTSTRAP_SERVERS"),
		CALocation:       os.Getenv("CA_LOCATION"),
		Username:         os.Getenv("SASL_USERNAME"),
		Password:         os.Getenv("SASL_PASSWORD"),
		TopicName:        os.Getenv("TOPIC_NAME"),
	}
}

// Fetch data from API
func fetchDataFromAPI(apiURL string) (map[string]interface{}, error) {
	resp, err := http.Get(apiURL)
	if err != nil {
		return nil, fmt.Errorf("error fetching data from API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected response status: %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading API response: %w", err)
	}

	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("error parsing API response: %w", err)
	}

	return data, nil
}

// Send data to Kafka
func sendToKafka(producer *kafka.Producer, topic string, data map[string]interface{}) error {
	value, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("error marshalling data to JSON: %w", err)
	}

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          value,
	}

	err = producer.Produce(message, nil)
	if err != nil {
		return fmt.Errorf("error producing to Kafka: %w", err)
	}

	// Wait for delivery confirmation
	e := <-producer.Events()
	msg := e.(*kafka.Message)

	if msg.TopicPartition.Error != nil {
		return fmt.Errorf("delivery failed: %w", msg.TopicPartition.Error)
	}

	fmt.Printf("Sent to Kafka: %s\n", value)
	return nil
}

func main() {
	// Load configuration
	conf := loadConfig()

	// Create Kafka producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     conf.BootstrapServers,
		"security.protocol":                     "SASL_SSL",
		"sasl.mechanisms":                       "PLAIN",
		"sasl.username":                         conf.Username,
		"sasl.password":                         conf.Password,
		"ssl.ca.location":                       conf.CALocation,
		"ssl.endpoint.identification.algorithm": "none",
	})
	if err != nil {
		fmt.Printf("Failed to create Kafka producer: %v\n", err)
		return
	}
	defer producer.Close()

	apiURL := "http://127.0.0.1:8000/stream-data"

	fmt.Println("Starting Kafka Producer...")
	for {
		data, err := fetchDataFromAPI(apiURL)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			time.Sleep(1 * time.Second) // Retry after 1 second
			continue
		}

		err = sendToKafka(producer, conf.TopicName, data)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		}

		time.Sleep(5 * time.Second) // Poll API every 5 seconds
	}
}
