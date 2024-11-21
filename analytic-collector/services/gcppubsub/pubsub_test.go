package gcppubsub

import (
	"os"
	"testing"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
)

// import (
// 	"os"
// 	"reflect"
// 	"testing"
// 	"time"

// 	"golang.org/x/net/context"

// 	"github.com/inconshreveable/log15"

// 	"cloud.google.com/go/pubsub"
// )

// var gcp *GcpPubSub

// func init() {
// 	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "../../files/detikcom-test-account.json")
// 	var err error
// 	gcp, err = NewGcpPubSub(context.Background(), "detikcom-179007", "test")
// 	if err != nil {
// 		log15.Crit(err.Error())
// 	}

// }

// func TestNewGcpPubSub(t *testing.T) {
// 	ctx := context.Background()

// 	type args struct {
// 		ctx       context.Context
// 		projectID string
// 		topicID   string
// 	}
// 	tests := []struct {
// 		name string
// 		args args
// 		want *GcpPubSub
// 	}{
// 		{
// 			name: "test",
// 			args: args{
// 				ctx:       ctx,
// 				topicID:   "projects/detikcom-179007/topics/test",
// 				projectID: "detikcom-179007",
// 			},
// 			want: &GcpPubSub{
// 				ctx:   ctx,
// 				topic: "projects/detikcom-179007/topics/test",
// 			},
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			got, err := NewGcpPubSub(tt.args.ctx, tt.args.projectID, tt.args.topicID)
// 			if err != nil {
// 				t.Error(err)
// 			}
// 			tt.want.client = got.client
// 			if !reflect.DeepEqual(got, tt.want) {
// 				t.Errorf("NewGcpPubSub() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }

// func TestGcpPubSub_Publish(t *testing.T) {
// 	type fields struct {
// 		ctx     context.Context
// 		client  *pubsub.Client
// 		topicID string
// 	}
// 	type args struct {
// 		messageJSON []byte
// 	}
// 	tests := []struct {
// 		name   string
// 		fields fields
// 		args   args
// 	}{
// 		{
// 			name: "test1",
// 			fields: fields{
// 				ctx:     gcp.ctx,
// 				client:  gcp.client,
// 				topicID: gcp.topicID,
// 			},
// 			args: args{
// 				messageJSON: []byte("{\"time\":\"" + time.Now().String() + "\"}"),
// 			},
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			g := GcpPubSub{
// 				ctx:     tt.fields.ctx,
// 				client:  tt.fields.client,
// 				topicID: tt.fields.topicID,
// 			}
// 			if _, err := g.Publish(tt.args.messageJSON); err != nil {
// 				t.Errorf("failed to publish message: %v", err)
// 			}
// 		})
// 	}
// }

// func TestGcpPubSub_Close(t *testing.T) {
// 	type fields struct {
// 		ctx     context.Context
// 		client  *pubsub.Client
// 		topicID string
// 	}
// 	tests := []struct {
// 		name   string
// 		fields fields
// 	}{
// 		{
// 			name: "test1",
// 			fields: fields{
// 				ctx:     context.Background(),
// 				client:  gcp.client,
// 				topicID: gcp.topicID,
// 			},
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			g := GcpPubSub{
// 				ctx:     tt.fields.ctx,
// 				client:  tt.fields.client,
// 				topicID: tt.fields.topicID,
// 			}
// 			if err := g.Close(); err != nil {
// 				t.Errorf("failed to close connection: %s", err.Error())
// 			}
// 		})
// 	}
// }

func TestNewGcpPubSub(t *testing.T) {
	cred := "../../files/prod.json"
	type args struct {
		ctx       context.Context
		projectID string
		topicGET  string
		topicPOST string
	}
	tests := []struct {
		name       string
		googleCred *string
		args       args
		wantErr    bool
	}{
		{
			name:       "test-1",
			googleCred: &cred,
			args: args{
				ctx:       context.Background(),
				projectID: "detikcom-179007",
				topicGET:  "test-get",
				topicPOST: "test-post",
			},
			wantErr: false,
		},
		{
			name:       "test with no credentials",
			googleCred: nil,
			args: args{
				ctx:       context.Background(),
				projectID: "detikcom-179007",
				topicGET:  "test-get",
				topicPOST: "test-post",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.googleCred != nil {
				t.Log("set env variable")
				os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", *tt.googleCred)
			}
			_, err := NewGcpPubSub(tt.args.ctx, tt.args.projectID, tt.args.topicGET, tt.args.topicPOST)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewGcpPubSub() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
		})
	}
}

func TestGcpPubSub_PublishGET(t *testing.T) {
	type fields struct {
		ctx       context.Context
		client    *pubsub.Client
		topicGET  *pubsub.Topic
		topicPOST *pubsub.Topic
	}
	type args struct {
		messageJSON []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GcpPubSub{
				ctx:       tt.fields.ctx,
				client:    tt.fields.client,
				topicGET:  tt.fields.topicGET,
				topicPOST: tt.fields.topicPOST,
			}
			got, err := g.PublishGET(tt.args.messageJSON)
			if (err != nil) != tt.wantErr {
				t.Errorf("GcpPubSub.PublishGET() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GcpPubSub.PublishGET() = %v, want %v", got, tt.want)
			}
		})
	}
}
