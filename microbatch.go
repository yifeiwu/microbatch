package main

import (
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

// Get blocks until the job result is available and then returns it.
func (fr *FutureResult) Get() JobResult {
	select {
	case res := <-fr.resultChan:
		return res
	default:
		fmt.Println("not available")
		return nil
	}
}

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
}

// Constructor
func NewMicroBatch(batchProcessor BatchProcessor, config Config) *MicroBatch {
	batcher := MicroBatch{
		config:         config,
		batchProcessor: batchProcessor,
		jobs:           make([]Job, 0, config.BatchSize),
		results:        make([]*FutureResult, 0, config.BatchSize),
		flushTicker:    time.NewTicker(config.FlushTimeout),
	}
	go batcher.run()
	return &batcher
}

// Submit a job, get a Future that eventually can Get() a result
func (mb *MicroBatch) SubmitJob(job Job) *FutureResult {

	futureResult := &FutureResult{
		resultChan: make(chan JobResult, 1),
	}

	mb.jobs = append(mb.jobs, job)
	mb.results = append(mb.results, futureResult)

	if len(mb.jobs) == mb.config.BatchSize {
		mb.FlushBatch()
	}

	return futureResult
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
		case <-mb.flushTicker.C:
			mb.FlushBatch()
		}
	}
}

// Mock Implementation

type mockBatchProcessor struct {
}

func (mbp *mockBatchProcessor) ProcessBatch(jobs []Job) []JobResult {
	results := make([]JobResult, len(jobs))
	for i, job := range jobs {
		results[i] = job.(string) + "_processed"
	}
	return results
}

func main() {
	exampleBatchProcessor := mockBatchProcessor{}

	microBatcher := NewMicroBatch(&exampleBatchProcessor, Config{BatchSize: 2, FlushTimeout: 10000 * time.Millisecond})

	jr1 := microBatcher.SubmitJob("job1")

	time.Sleep(1 * time.Second)
	fmt.Println(jr1)
	fmt.Println(jr1.Get())

	jr2 := microBatcher.SubmitJob("job2")

	fmt.Println(jr1.Get())
	fmt.Println(jr2.Get())
}
