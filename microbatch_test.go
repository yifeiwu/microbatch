package microbatch_test

import (
	"time"
	mb "ywu/microbatch"

	"testing"

	"github.com/stretchr/testify/assert"
)

// Mock Implementation

type mockBatchProcessor struct {
}

func (mbp *mockBatchProcessor) ProcessBatch(jobs []mb.Job) []mb.JobResult {
	results := make([]mb.JobResult, len(jobs))
	for i, job := range jobs {
		results[i] = job.(string) + "_processed"
	}
	return results
}

func TestMicroBatcherSubmitJob(t *testing.T) {
	// Successfully submit and processes a job

	exampleBatchProcessor := mockBatchProcessor{}

	microBatcher := mb.NewMicroBatch(&exampleBatchProcessor, mb.Config{BatchSize: 2, FlushTimeout: 100 * time.Millisecond})

	jr1, err1 := microBatcher.SubmitJob("job1")
	assert.NoError(t, err1)

	time.Sleep(200 * time.Millisecond)

	assert.Equal(t, "job1_processed", jr1.Get())
	microBatcher.Shutdown()
}
