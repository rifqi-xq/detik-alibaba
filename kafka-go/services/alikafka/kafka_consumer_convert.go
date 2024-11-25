package kafkaconsumer

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Configuration structures
type KafkaConfig struct {
	BootstrapServers string
	CALocation       string
	Username         string
	Password         string
	GroupName        string
	TopicName        string
}

type OSSConfig struct {
	AccessKeyID     string
	AccessKeySecret string
	Endpoint        string
	BucketName      string
}

// Load Kafka and OSS configurations
func loadConfig() (KafkaConfig, OSSConfig) {
	return KafkaConfig{
			BootstrapServers: os.Getenv("BOOTSTRAP_SERVERS"),
			CALocation:       os.Getenv("CA_LOCATION"),
			Username:         os.Getenv("SASL_USERNAME"),
			Password:         os.Getenv("SASL_PASSWORD"),
			GroupName:        os.Getenv("GROUP_NAME"),
			TopicName:        os.Getenv("TOPIC_NAME"),
		}, OSSConfig{
			AccessKeyID:     os.Getenv("OSS_ACCESS_KEY_ID"),
			AccessKeySecret: os.Getenv("OSS_ACCESS_KEY_SECRET"),
			Endpoint:        os.Getenv("OSS_ENDPOINT"),
			BucketName:      os.Getenv("OSS_BUCKET_NAME"),
		}
}

// Initialize OSS connection
func initializeOSSConnection(conf OSSConfig) (*oss.Bucket, error) {
	client, err := oss.New(conf.Endpoint, conf.AccessKeyID, conf.AccessKeySecret)
	if err != nil {
		return nil, fmt.Errorf("failed to create OSS client: %w", err)
	}

	bucket, err := client.Bucket(conf.BucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to get OSS bucket: %w", err)
	}

	return bucket, nil
}

// Store messages in OSS
func storeBatchInOSS(bucket *oss.Bucket, topic string, messages []string) error {
	if len(messages) == 0 {
		return nil
	}

	currentTime := time.Now().Unix()
	filename := fmt.Sprintf("%s_batch_%d.txt", topic, currentTime)
	batchContent := strings.Join(messages, "\n")

	err := bucket.PutObject(filename, bytes.NewReader([]byte(batchContent)))
	if err != nil {
		return fmt.Errorf("failed to store batch in OSS: %w", err)
	}

	fmt.Printf("Batch stored in OSS as %s\n", filename)
	return nil
}

func main() {
	kafkaConf, ossConf := loadConfig()

	// Initialize Kafka consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":                     kafkaConf.BootstrapServers,
		"security.protocol":                     "SASL_SSL",
		"sasl.mechanisms":                       "PLAIN",
		"sasl.username":                         kafkaConf.Username,
		"sasl.password":                         kafkaConf.Password,
		"ssl.ca.location":                       kafkaConf.CALocation,
		"ssl.endpoint.identification.algorithm": "none",
		"group.id":                              kafkaConf.GroupName,
		"auto.offset.reset":                     "latest",
		"fetch.message.max.bytes":               1024 * 512,
	})
	if err != nil {
		fmt.Printf("Failed to create Kafka consumer: %v\n", err)
		return
	}
	defer consumer.Close()

	// Initialize OSS bucket
	bucket, err := initializeOSSConnection(ossConf)
	if err != nil {
		fmt.Printf("Failed to initialize OSS connection: %v\n", err)
		return
	}

	// Message buffer and sync primitives
	messageBuffer := make(map[string][]string)
	var bufferLock sync.Mutex

	// Periodically store messages in OSS
	batchInterval := time.Minute
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			bufferLock.Lock()
			for topic, messages := range messageBuffer {
				if len(messages) > 0 {
					if err := storeBatchInOSS(bucket, topic, messages); err != nil {
						fmt.Printf("Error storing batch for topic %s: %v\n", topic, err)
					}
					messageBuffer[topic] = nil // Clear the buffer for the topic
				}
			}
			bufferLock.Unlock()
		}
	}()

	// Subscribe to Kafka topic
	err = consumer.Subscribe(kafkaConf.TopicName, nil)
	if err != nil {
		fmt.Printf("Failed to subscribe to topic: %v\n", err)
		return
	}

	// Handle system interrupts gracefully
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	fmt.Println("Starting Kafka Consumer...")
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Stopping Kafka Consumer...")
			return
		default:
			msg, err := consumer.ReadMessage(time.Second)
			if err != nil {
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
					continue // Ignore timeout errors
				}
				fmt.Printf("Error reading message: %v\n", err)
				continue
			}

			// Decode and buffer the message
			messageStr := string(msg.Value)
			bufferLock.Lock()
			messageBuffer[*msg.TopicPartition.Topic] = append(messageBuffer[*msg.TopicPartition.Topic], messageStr)
			bufferLock.Unlock()
		}
	}
}
