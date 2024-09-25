package microbatch_test

import (
	"time"
	mb "ywu/microbatch"

	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockBatchProcessor struct {
}

func (mbp *mockBatchProcessor) ProcessBatch(jobs []mb.Job) []mb.JobResult {
	results := make([]mb.JobResult, len(jobs))
	for i, job := range jobs {
		results[i] = job.(string) + "_processed"
	}
	return results
}

// Processes a batch when time greater than FlushTimeout but batches is less than BatchSize
func TestMicroBatcherFlushTimeout(t *testing.T) {

	exampleBatchProcessor := mockBatchProcessor{}

	microBatcher := mb.NewMicroBatch(&exampleBatchProcessor, mb.Config{BatchSize: 2, FlushTimeout: 50 * time.Millisecond})

	jr1, err1 := microBatcher.SubmitJob("job1")
	assert.NoError(t, err1)

	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, "job1_processed", jr1.Get())
	microBatcher.Shutdown()
}

// Processes a batch when time less than FlushTimeout but batch is full
func TestMicroBatcherBatchMax(t *testing.T) {

	exampleBatchProcessor := mockBatchProcessor{}

	microBatcher := mb.NewMicroBatch(&exampleBatchProcessor, mb.Config{BatchSize: 2, FlushTimeout: 500 * time.Millisecond})

	jr1, _ := microBatcher.SubmitJob("job1")
	jr2, _ := microBatcher.SubmitJob("job2")
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, "job1_processed", jr1.Get())
	assert.Equal(t, "job2_processed", jr2.Get())

	microBatcher.Shutdown()
}

func TestMicroBatchConcurrentSubmissions(t *testing.T) {
	exampleBatchProcessor := mockBatchProcessor{}
	microBatcher := mb.NewMicroBatch(&exampleBatchProcessor, mb.Config{BatchSize: 5, FlushTimeout: 500 * time.Millisecond})

	var wg sync.WaitGroup
	jobs := []string{"job1", "job2", "job3", "job4", "job5"}

	for _, job := range jobs {
		wg.Add(1)
		go func(job string) {
			defer wg.Done()
			jr, err := microBatcher.SubmitJob(job)
			assert.NoError(t, err)
			assert.Equal(t, job+"_processed", jr.Get())
		}(job)
	}

	wg.Wait()
	microBatcher.Shutdown()
}

// Returns an error if submitting a job during shutdown
func TestMicroBatcherSubmitJobOnShutdown(t *testing.T) {

	exampleBatchProcessor := mockBatchProcessor{}

	microBatcher := mb.NewMicroBatch(&exampleBatchProcessor, mb.Config{BatchSize: 2, FlushTimeout: 500 * time.Millisecond})

	jr1, _ := microBatcher.SubmitJob("job1")

	microBatcher.Shutdown()

	_, err := microBatcher.SubmitJob("job2")

	assert.Error(t, err, "Job not processed, batcher in shutdown state")
	assert.Equal(t, "job1_processed", jr1.Get())

}
