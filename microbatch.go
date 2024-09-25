package microbatch

import (
	"errors"
	"fmt"
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

// Get returns an answer if available or nil.
func (fr *FutureResult) Get() JobResult {
	select {
	case res := <-fr.resultChan:
		return res
	default:
		fmt.Println("result not yet available")
		return nil
	}
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
	shutdown       chan bool
	inShutdown     bool
}

// Constructor
func NewMicroBatch(batchProcessor BatchProcessor, config Config) *MicroBatch {
	batcher := MicroBatch{
		config:         config,
		batchProcessor: batchProcessor,
		jobs:           make([]Job, 0, config.BatchSize),
		results:        make([]*FutureResult, 0, config.BatchSize),
		flushTicker:    time.NewTicker(config.FlushTimeout),
		shutdown:       make(chan bool),
	}
	go batcher.run()
	return &batcher
}

// Submit a job, get a Future that eventually can Get() a result
func (mb *MicroBatch) SubmitJob(job Job) (*FutureResult, error) {

	if mb.inShutdown {
		return &FutureResult{}, ErrInShutdown
	}

	futureResult := &FutureResult{
		resultChan: make(chan JobResult, 1),
	}

	mb.jobs = append(mb.jobs, job)
	mb.results = append(mb.results, futureResult)

	if len(mb.jobs) == mb.config.BatchSize {
		mb.FlushBatch()
	}

	return futureResult, nil
}

// Flush jobs to batchprocessor
func (mb *MicroBatch) FlushBatch() {

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
	for {
		select {
		case <-mb.shutdown:
			mb.inShutdown = true
			mb.FlushBatch()
		case <-mb.flushTicker.C:
			mb.FlushBatch()
		}
	}
}

func (mb *MicroBatch) Shutdown() {
	fmt.Println("batcher received shutdown")
	mb.shutdown <- true
	close(mb.shutdown)
}
