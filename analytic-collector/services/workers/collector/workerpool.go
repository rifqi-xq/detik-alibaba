package collector

import (
	"strconv"

	"github.com/inconshreveable/log15"
	"analytic-collector/services/gcppubsub"
)

var (
	startedWorkers []Worker
)

// WorkerPool is worker pooler management
type WorkerPool struct {
	poolGet     chan chan JobGet
	jobGetQueue chan JobGet

	poolPost     chan chan JobPost
	jobPostQueue chan JobPost

	quit       chan bool
	maxWorkers int
	gcpPubSub  *gcppubsub.GcpPubSub
}

// NewWorkerPool creates new WorkerPool instance
func NewWorkerPool(maxWorkers int, gcpPubSub *gcppubsub.GcpPubSub) WorkerPool {
	// Get
	poolGet := make(chan chan JobGet, maxWorkers)
	jobGetQueue := make(chan JobGet)

	// Post
	poolPost := make(chan chan JobPost, maxWorkers)
	jobPostQueue := make(chan JobPost)

	startedWorkers = make([]Worker, maxWorkers)

	return WorkerPool{
		poolGet:     poolGet,
		jobGetQueue: jobGetQueue,

		poolPost:     poolPost,
		jobPostQueue: jobPostQueue,

		quit:       make(chan bool),
		maxWorkers: maxWorkers,
		gcpPubSub:  gcpPubSub,
	}
}

// AddGETJob adding a GET job to queue
func (p WorkerPool) AddGETJob(payload map[string][]string) {
	p.jobGetQueue <- JobGet{Payload: payload}
}

// AddPOSTJob adding a POST job to queue
func (p WorkerPool) AddPOSTJob(payload string) {
	p.jobPostQueue <- JobPost{Payload: payload}
}

// StartWorkers start all workers
func (p WorkerPool) StartWorkers() {
	log15.Info("starting collector workers: " + strconv.Itoa(p.maxWorkers) + " workers")
	for i := 0; i < p.maxWorkers; i++ {
		worker := NewWorker(p.poolGet, p.poolPost, p.gcpPubSub)
		worker.Start()

		// register started worker
		startedWorkers = append(startedWorkers, worker)
	}

	go p.dispatchJobQueue()
}

// StopWorkers stop all workers
func (p WorkerPool) StopWorkers() {
	log15.Info("stopping job dispatcher")
	go func() {
		p.quit <- true
	}()

	log15.Info("stopping collector workers")
	for _, worker := range startedWorkers {
		w := worker
		w.Stop()
	}
}

func (p WorkerPool) dispatchJobQueue() {
	for {
		select {
		case job := <-p.jobGetQueue:
			log15.Debug("GET job received")
			// a job request has been received
			go func(job JobGet) {
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				jobChannel := <-p.poolGet

				// dispatch the job to the worker job channel
				jobChannel <- job
			}(job)
		case job := <-p.jobPostQueue:
			log15.Debug("POST job received")
			go func(job JobPost) {
				jobChannel := <-p.poolPost

				jobChannel <- job
			}(job)
		case <-p.quit:
			// we have received a signal to stop
			return
		}
	}
}
