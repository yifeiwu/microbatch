
# microbatching

Micro-batching is a technique used in processing pipelines where individual tasks are grouped together into small batches. This can improve throughput by reducing the number of requests made to a downstream system. This library provides a generic batching library with variable batch size and frequency. 


## Usage
```go
package main

import (
	"fmt"
)

func main() {

// create and run a micro-batching service
mockBatchProcessor:=  mockBatchProcessor{} //takes in a slice of jobs and returns a slice of job results
microBatcher:= mb.NewMicroBatch(&mockBatchProcessor, mb.Config{BatchSize: 2, FlushTimeout: 50*time.Millisecond})

// submit a job, returns a future
jr1, err1  :=  microBatcher.SubmitJob("job1")

time.Sleep(100  *  time.Millisecond) // wait for job to be flushed

if err != nil {
    fmt.Println(err)
} else {
    fmt.Println(jr1.Get())
}

microBatcher.Shutdown()  
```
### Config 
The config defines two parameters
`BatchSize` - determines number of jobs to collate before flushing for processing
`FlushTimeout` - determines duration to wait before flushing even if the batch is not full

### BatchProcessor dependency
The batch processor should implement the `BatchProcessor` interface. 

### Job results
Job results are returned immediately as a `Future` that can be queried later for the value via the `Get()` function. 

### Shutdown
When `Shutdown()` is called, previously submitted jobs are flushed and processed. Jobs submitted after shutdown has been called will return nil with an error.

## Tests

```bash
go test -v
```