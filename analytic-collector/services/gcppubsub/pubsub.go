package gcppubsub

import (
	"errors"
	"os"

	"cloud.google.com/go/pubsub"
	"github.com/inconshreveable/log15"
	"golang.org/x/net/context"
)

// GcpPubSub encapsulates the Google Cloud Pub/Sub client and related topics.
// This struct is used to manage Pub/Sub interactions for specific topics.
type GcpPubSub struct {
	ctx       context.Context // Context for managing request lifetime
	client    *pubsub.Client  // Pub/Sub client for interacting with GCP
	topicGET  *pubsub.Topic   // Pub/Sub topic for GET operations
	topicPOST *pubsub.Topic   // Pub/Sub topic for POST operations
}

// NewGcpPubSub initializes a new instance of GcpPubSub with the given configuration.
// Params:
// - ctx: The context to use for managing request deadlines and cancellations.
// - projectID: The GCP project ID where the Pub/Sub topics reside.
// - topicGET: The name of the Pub/Sub topic for GET operations.
// - topicPOST: The name of the Pub/Sub topic for POST operations.
// Returns:
// - A pointer to the GcpPubSub instance or an error if initialization fails.
func NewGcpPubSub(ctx context.Context, projectID, topicGET, topicPOST string) (*GcpPubSub, error) {
	// Ensure the GOOGLE_APPLICATION_CREDENTIALS environment variable is set.
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		log15.Error("GOOGLE_APPLICATION_CREDENTIALS environment variable not found")
		return nil, errors.New("GOOGLE_APPLICATION_CREDENTIALS environment variable not found")
	}

	// Attempt to create a Pub/Sub client using the provided context and project ID.
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log15.Error("Failed to connect to GCP: " + err.Error())
		return nil, err
	}

	// Create and return the GcpPubSub instance.
	gcp := &GcpPubSub{
		ctx:       ctx,
		client:    client,
		topicGET:  client.Topic(topicGET),
		topicPOST: client.Topic(topicPOST),
	}

	return gcp, nil
}

// Close releases any resources held by the GcpPubSub client.
// This should be called when the GcpPubSub instance is no longer needed.
func (g *GcpPubSub) Close() error {
	return g.client.Close()
}

// PublishGET publishes a message to the GET Pub/Sub topic.
// Params:
// - messageJSON: The message data to publish, encoded as a JSON byte slice.
// Returns:
// - The server-generated ID of the published message or an error if the publish fails.
func (g *GcpPubSub) PublishGET(messageJSON []byte) (*string, error) {
	// Publish the message to the GET topic.
	result := g.topicGET.Publish(g.ctx, &pubsub.Message{
		Data: messageJSON,
	})

	// Wait for the publish result and handle any errors.
	serverID, err := result.Get(g.ctx)
	if err != nil {
		log15.Error(err.Error())
		return nil, err
	}

	return &serverID, nil
}

// PublishPOST publishes a message to the POST Pub/Sub topic.
// Params:
// - messageJSON: The message data to publish, encoded as a JSON byte slice.
// Returns:
// - The server-generated ID of the published message or an error if the publish fails.
func (g *GcpPubSub) PublishPOST(messageJSON []byte) (*string, error) {
	// Publish the message to the POST topic.
	result := g.topicPOST.Publish(g.ctx, &pubsub.Message{
		Data: messageJSON,
	})

	// Wait for the publish result and handle any errors.
	serverID, err := result.Get(g.ctx)
	if err != nil {
		log15.Error(err.Error())
		return nil, err
	}

	return &serverID, nil
}
