// package gcppubsub

// import (
// 	"errors"
// 	"os"

// 	"cloud.google.com/go/pubsub"
// 	"github.com/inconshreveable/log15"
// 	"golang.org/x/net/context"
// )

// // GcpPubSub holds GCP PubSub struct
// // example: https://github.com/GoogleCloudPlatform/golang-samples/blob/master/pubsub/topics/main.go
// type GcpPubSub struct {
// 	ctx       context.Context
// 	client    *pubsub.Client
// 	topicGET  *pubsub.Topic
// 	topicPOST *pubsub.Topic
// }

// // NewGcpPubSub initiate new GCP PubSub
// func NewGcpPubSub(ctx context.Context, projectID, topicGET, topicPOST string) (*GcpPubSub, error) {
// 	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
// 		log15.Error("GOOGLE_APPLICATION_CREDENTIALS environment variable not found")
// 		return nil, errors.New("GOOGLE_APPLICATION_CREDENTIALS environment variable not found")
// 	}

// 	client, err := pubsub.NewClient(ctx, projectID)
// 	if err != nil {
// 		log15.Error("Failed to connect to GCP " + err.Error())
// 		return nil, err
// 	}

// 	gcp := &GcpPubSub{
// 		ctx:       ctx,
// 		client:    client,
// 		topicGET:  client.Topic(topicGET),
// 		topicPOST: client.Topic(topicPOST),
// 	}

// 	return gcp, nil
// }

// // Close connection
// func (g *GcpPubSub) Close() error {
// 	return g.client.Close()
// }

// // PublishGET is function to publish message from GET to PubSub
// func (g *GcpPubSub) PublishGET(messageJSON []byte) (*string, error) {
// 	result := g.topicGET.Publish(g.ctx, &pubsub.Message{
// 		Data: messageJSON,
// 	})

// 	serverID, err := result.Get(g.ctx)
// 	if err != nil {
// 		log15.Error(err.Error())
// 		return nil, err
// 	}

// 	return &serverID, nil
// }

// // PublishPOST is function to publish message from POST to PubSub
// func (g *GcpPubSub) PublishPOST(messageJSON []byte) (*string, error) {
// 	result := g.topicPOST.Publish(g.ctx, &pubsub.Message{
// 		Data: messageJSON,
// 	})

// 	serverID, err := result.Get(g.ctx)
// 	if err != nil {
// 		log15.Error(err.Error())
// 		return nil, err
// 	}

// 	return &serverID, nil
// }
