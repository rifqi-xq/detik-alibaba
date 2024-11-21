package services

import (
	"golang.org/x/net/context"

	"github.com/inconshreveable/log15"

	"analytic-collector/services/gcppubsub"
	"analytic-collector/services/workers/collector"
)

// Service will handles application business logic
type Service struct {
	collectorWorkerPool collector.WorkerPool
	gcpPubSub           *gcppubsub.GcpPubSub
}

// ServiceInterface list of service functions
type ServiceInterface interface {
	StartWorkers()
	StopWorkers()
	AddCollectorGETJob(payload map[string][]string)
	AddCollectorPOSTJob(payload string)
}

// NewService initiate service
func NewService(ctx context.Context, maxCollectorWorker int, gcpProjectID, pubsubTopicGET, pubsubTopicPOST string) (*Service, error) {
	// initiate PubSub
	gcpPubSub, err := gcppubsub.NewGcpPubSub(ctx, gcpProjectID, pubsubTopicGET, pubsubTopicPOST)
	if err != nil {
		return nil, err
	}

	collectorPool := collector.NewWorkerPool(maxCollectorWorker, gcpPubSub)
	return &Service{
		collectorWorkerPool: collectorPool,
		gcpPubSub:           gcpPubSub,
	}, nil
}

// AddCollectorGETJob register article to collector job queue to be processed
func (s *Service) AddCollectorGETJob(payload map[string][]string) {
	log15.Debug("adding GET job")
	s.collectorWorkerPool.AddGETJob(payload)
}

// AddCollectorPOSTJob register article to collector job queue to be processed
func (s *Service) AddCollectorPOSTJob(payload string) {
	log15.Debug("adding POST job: " + payload[:10])
	s.collectorWorkerPool.AddPOSTJob(payload)
}

// StartWorkers start all workers
func (s *Service) StartWorkers() {
	s.collectorWorkerPool.StartWorkers()
}

// StopWorkers stop all workers
func (s *Service) StopWorkers() {
	if err := s.gcpPubSub.Close(); err != nil {
		log15.Error(err.Error())
	}
	s.collectorWorkerPool.StopWorkers()
}
