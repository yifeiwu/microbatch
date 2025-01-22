package microbatch

import (
	"errors"
	"log"
	"sync"
	"time"
)

// Generic input
type Job interface {
}

// Generic output
type JobResult interface {
}

// FutureResult is a promise that resolves to a JobResult.
type FutureResult struct {
	resultChan chan JobResult
}

// Get returns an answer if available
func (fr *FutureResult) Get() JobResult {
	return <-fr.resultChan
}

var ErrInShutdown = errors.New("Job not processed, batcher in shutdown state")

// Generic processor
type BatchProcessor interface {
	ProcessBatch([]Job) []JobResult
}

// Config holds the configuration for the batching behavior.
type Config struct {
	BatchSize    int           // Number of jobs before flushing
	FlushTimeout time.Duration // Time to wait before flushing even if the batch isn't full
}

type MicroBatch struct {
	jobs           []Job
	results        []*FutureResult
	config         Config
	batchProcessor BatchProcessor
	flushTicker    *time.Ticker
	inShutdown     bool
	mu             sync.Mutex
	done           chan struct{}
}

// Constructor
func NewMicroBatch(batchProcessor BatchProcessor, config Config) *MicroBatch {
	batcher := MicroBatch{
		config:         config,
		batchProcessor: batchProcessor,
		jobs:           make([]Job, 0, config.BatchSize),
		results:        make([]*FutureResult, 0, config.BatchSize),
		flushTicker:    time.NewTicker(config.FlushTimeout),
		done:           make(chan struct{}),
	}
	go batcher.run()
	return &batcher
}

// Submit a job, get a Future that eventually can Get() a result
func (mb *MicroBatch) SubmitJob(job Job) (*FutureResult, error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if mb.inShutdown {
		return nil, ErrInShutdown
	}

	futureResult := &FutureResult{
		resultChan: make(chan JobResult, 1),
	}

	mb.jobs = append(mb.jobs, job)
	mb.results = append(mb.results, futureResult)

	if len(mb.jobs) == mb.config.BatchSize {
		mb.flushBatch()
		// Reset ticker to make flush behavior more consistent with expectations
		mb.flushTicker.Reset(mb.config.FlushTimeout)
	}

	return futureResult, nil
}

// Flush jobs to batchprocessor
func (mb *MicroBatch) flushBatch() {
	if len(mb.jobs) == 0 {
		return
	}

	jobsToProcess := mb.jobs
	resultsToProcess := mb.results

	mb.jobs = []Job{}
	mb.results = []*FutureResult{}

	results := mb.batchProcessor.ProcessBatch(jobsToProcess)
	for i, result := range results {
		resultsToProcess[i].resultChan <- result
		close(resultsToProcess[i].resultChan)
	}
}

func (mb *MicroBatch) run() {
	defer mb.flushTicker.Stop()

	for {
		select {
		case <-mb.flushTicker.C:
			mb.mu.Lock()
			mb.flushBatch()
			mb.mu.Unlock()
		case <-mb.done:
			return
		}
	}
}
func (mb *MicroBatch) Shutdown() {
	mb.mu.Lock()
	if mb.inShutdown {
		mb.mu.Unlock()
		return
	}
	mb.inShutdown = true

	log.Println("batcher received shutdown")
	mb.flushBatch() //Process previously submitted jobs
	mb.mu.Unlock()

	close(mb.done)
	log.Println("Previously submitted jobs processed")
}
