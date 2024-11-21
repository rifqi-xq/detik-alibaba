package collector

import (
	"encoding/json"

	"analytic-collector/services/gcppubsub"

	"github.com/inconshreveable/log15"
)

// JobGet handles Get Request Payload
type JobGet struct {
	Payload map[string][]string
}

// JobPost handles Post Request Payload
type JobPost struct {
	Payload string
}

// Worker represents the worker that executes the job
type Worker struct {
	// GET Job
	PoolGet       chan chan JobGet
	JobGetChannel chan JobGet

	// POST Job
	PoolPost       chan chan JobPost
	JobPostChannel chan JobPost

	gcpPubSub *gcppubsub.GcpPubSub
	quit      chan bool
}

// NewWorker initiate worker
func NewWorker(poolGet chan chan JobGet, poolPost chan chan JobPost, gcpPubSub *gcppubsub.GcpPubSub) Worker {
	return Worker{
		PoolGet:        poolGet,
		JobGetChannel:  make(chan JobGet),
		PoolPost:       poolPost,
		JobPostChannel: make(chan JobPost),
		quit:           make(chan bool),
		gcpPubSub:      gcpPubSub,
	}
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w Worker) Start() {
	go func() {
		for {
			// register the current worker into the worker queue.
			w.PoolGet <- w.JobGetChannel

			select {
			case job := <-w.JobGetChannel:
				// we have received a work from GET request.
				// send data to PubSub
				jsPayload, _ := json.Marshal(job.Payload)
				ID, err := w.gcpPubSub.PublishGET(jsPayload)
				if err != nil {
					log15.Warn("GET: failed to publish: " + err.Error())
				} else {
					log15.Debug(string(jsPayload))
					log15.Debug("GET: message has been published with ID: " + *ID)
				}
			case <-w.quit:
				// we have received a signal to stop
				return
			}
		}
	}()

	go func() {
		for {
			// register the current worker into the worker queue.
			w.PoolPost <- w.JobPostChannel

			select {
			case job := <-w.JobPostChannel:
				// we have received a work from POST request
				ID, err := w.gcpPubSub.PublishPOST([]byte(job.Payload))
				if err != nil {
					log15.Warn("POST: failed to publish: " + err.Error())
				} else {
					log15.Debug(job.Payload[0:100])
					log15.Debug("POST: message has been published with ID: " + *ID)
				}
			case <-w.quit:
				// we have received a signal to stop
				return
			}
		}
	}()
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}
