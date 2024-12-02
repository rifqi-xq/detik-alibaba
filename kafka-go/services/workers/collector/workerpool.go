package controllers

import (
	"fmt"
	"sync"
)

// WorkerPool manages a pool of workers to process jobs concurrently
type WorkerPool struct {
	workerCount int
	jobQueue    chan map[string]interface{}
	waitGroup   *sync.WaitGroup
	stopChan    chan struct{}
}

// NewWorkerPool initializes a WorkerPool with the given number of workers
func NewWorkerPool(workerCount int) *WorkerPool {
	return &WorkerPool{
		workerCount: workerCount,
		jobQueue:    make(chan map[string]interface{}, 100), // Buffered channel for jobs
		waitGroup:   &sync.WaitGroup{},
		stopChan:    make(chan struct{}),
	}
}

// Start initializes the workers and begins processing jobs
func (wp *WorkerPool) Start() {
	for i := 0; i < wp.workerCount; i++ {
		wp.waitGroup.Add(1)

		go func(workerID int) {
			defer wp.waitGroup.Done()

			for {
				select {
				case job := <-wp.jobQueue:
					// Process the job
					wp.processJob(workerID, job)
				case <-wp.stopChan:
					// Stop the worker
					fmt.Printf("Worker %d stopping\n", workerID)
					return
				}
			}
		}(i + 1)
	}
}

// Stop gracefully stops all workers
func (wp *WorkerPool) Stop() {
	close(wp.stopChan)  // Signal workers to stop
	wp.waitGroup.Wait() // Wait for all workers to finish
	close(wp.jobQueue)  // Close job queue
	fmt.Println("WorkerPool stopped")
}

// AddJob adds a job to the queue for processing
func (wp *WorkerPool) AddJob(job map[string]interface{}) {
	select {
	case wp.jobQueue <- job:
		fmt.Println("Job added to the queue")
	default:
		fmt.Println("Job queue is full, dropping job")
	}
}

// processJob processes a single job
func (wp *WorkerPool) processJob(workerID int, job map[string]interface{}) {
	fmt.Printf("Worker %d processing job: %v\n", workerID, job)

	// Simulate job processing
	if payload, ok := job["payload"]; ok {
		fmt.Printf("Worker %d processed payload: %v\n", workerID, payload)
	} else {
		fmt.Printf("Worker %d received invalid job structure\n", workerID)
	}
}
