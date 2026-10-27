// Command scenarios measures fixed-workload query behavior under expiration, outage, timeout and batching.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/Waterkyuu/soroi"
	"github.com/redis/go-redis/v9"
)

// configuration records the complete closed-loop workload alongside every result.
type configuration struct {
	// Requests is the fixed number of caller operations in each case.
	Requests int `json:"requests"`
	// Workers bounds concurrent callers; no arrival-rate generator is used.
	Workers int `json:"workers"`
	// SourceDelay models one individual or bulk backend call's service time.
	SourceDelay time.Duration `json:"source_delay_ns"`
	// Timeout bounds each downstream-timeout caller independently.
	Timeout time.Duration `json:"timeout_ns"`
	// MaxConcurrent bounds protected backend calls in outage and timeout cases.
	MaxConcurrent int `json:"max_concurrent"`
	// BatchSize caps unique keys in each bulk call.
	BatchSize int `json:"batch_size"`
	// BatchWait sets the collection window for automatic batching.
	BatchWait time.Duration `json:"batch_wait_ns"`
}

// measurement separates admission outcomes from successful-request latency and resource deltas.
type measurement struct {
	// Successes counts callers receiving a value without an error.
	Successes int `json:"successes"`
	// Rejected counts overload failures that never executed a source callback.
	Rejected int `json:"rejected"`
	// Timeouts counts callers whose context or query deadline elapsed.
	Timeouts int `json:"timeouts"`
	// Failures counts other errors, including cancellation.
	Failures int `json:"other_failures"`
	// SuccessRate is successes divided by all attempted caller operations.
	SuccessRate float64 `json:"success_rate"`
	// P95MS measures all outcomes, including immediate rejection.
	P95MS float64 `json:"all_p95_ms"`
	// P99MS measures all outcomes, including immediate rejection.
	P99MS float64 `json:"all_p99_ms"`
	// SuccessP95MS is null when no caller succeeded.
	SuccessP95MS *float64 `json:"success_p95_ms"`
	// SuccessP99MS is null when no caller succeeded.
	SuccessP99MS *float64 `json:"success_p99_ms"`
	// ElapsedMS includes scheduling and the complete fixed request population.
	ElapsedMS float64 `json:"elapsed_ms"`
	// AllocatedBytes counts process-wide allocations during the workload, including instrumentation.
	AllocatedBytes uint64 `json:"allocated_bytes"`
	// HeapEndBytes reports live and not-yet-collected heap at workload completion, not RSS.
	HeapEndBytes uint64 `json:"heap_end_bytes"`
	// GCCount counts collections during the measured workload.
	GCCount uint32 `json:"gc_count"`
}

// caseResult records one comparable variant and its actual source work.
type caseResult struct {
	// Scenario names the injected business condition.
	Scenario string `json:"scenario"`
	// Mode selects baseline behavior or soroi's documented policy.
	Mode string `json:"mode"`
	// Configuration describes the workload rather than relying on undocumented defaults.
	Configuration configuration `json:"configuration"`
	// GoVersion identifies the executing toolchain.
	GoVersion string `json:"go_version"`
	// Platform identifies the runtime OS and architecture.
	Platform string `json:"platform"`
	// Procs records the runtime scheduling limit.
	Procs int `json:"gomaxprocs"`
	// Measurement holds latency, outcome and allocation populations.
	Measurement measurement `json:"measurement"`
	// SourceCalls counts physical individual or bulk callback entries, excluding setup.
	SourceCalls int64 `json:"source_calls"`
	// PeakSourceCalls reports maximum simultaneous source callbacks.
	PeakSourceCalls int64 `json:"peak_source_calls"`
	// SharedReadErrors counts observed Redis read failures in the outage population.
	SharedReadErrors uint64 `json:"shared_read_errors"`
}

// main runs both variants of each scenario and emits one JSON object per case.
func main() {
	config := configuration{}
	flag.IntVar(&config.Requests, "requests", 1000, "caller operations per case")
	flag.IntVar(&config.Workers, "workers", 32, "concurrent closed-loop callers")
	flag.DurationVar(&config.SourceDelay, "source-delay", 2*time.Millisecond, "individual or bulk source service time")
	flag.DurationVar(&config.Timeout, "timeout", time.Millisecond, "per-caller deadline in the downstream-timeout case")
	flag.IntVar(&config.MaxConcurrent, "max-concurrent", 8, "protected source concurrency")
	flag.IntVar(&config.BatchSize, "batch-size", 64, "maximum keys per bulk callback")
	flag.DurationVar(&config.BatchWait, "batch-wait", time.Millisecond, "batch collection window")
	flag.Parse()
	if err := run(config); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run validates workload bounds and owns the disposable Redis fixture for outage cases.
func run(config configuration) error {
	invalidCapacity := config.Requests <= 0 || config.Workers <= 0 || config.MaxConcurrent <= 0 || config.BatchSize <= 0
	invalidTiming := config.SourceDelay < 0 || config.Timeout <= 0 || config.BatchWait < 0
	if invalidCapacity || invalidTiming {
		return errors.New("capacities and timeout must be positive; source and batch delays must be nonnegative")
	}
	ctx := context.Background()
	encoder := json.NewEncoder(os.Stdout)
	for _, scenario := range []string{"hot-expiry", "redis-outage", "downstream-timeout", "batching"} {
		for _, mode := range []string{"baseline", "soroi"} {
			var remote *redis.Client
			cleanup := func() {}
			if scenario == "redis-outage" {
				var err error
				remote, cleanup, err = stoppedRedis(ctx)
				if err != nil {
					return err
				}
			}
			result, err := runCase(ctx, config, scenario, mode, remote)
			cleanup()
			if err != nil {
				return err
			}
			if err := encoder.Encode(result); err != nil {
				return err
			}
		}
	}
	return nil
}

// measure times caller service from worker dispatch to completion; it does not model an open-loop arrival rate.
func measure(config configuration, call func(int) error, finish func()) measurement {
	latencies := make([]time.Duration, config.Requests)
	outcomes := make([]error, config.Requests)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	started := time.Now()
	start := make(chan struct{})
	var workers sync.WaitGroup
	for worker := 0; worker < config.Workers; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			<-start
			for i := worker; i < config.Requests; i += config.Workers {
				requestStart := time.Now()
				outcomes[i] = call(i)
				latencies[i] = time.Since(requestStart)
			}
		}(worker)
	}
	close(start)
	workers.Wait()
	if finish != nil {
		finish()
	}
	elapsed := time.Since(started)
	runtime.ReadMemStats(&after)
	result := measurement{
		ElapsedMS:      float64(elapsed) / float64(time.Millisecond),
		AllocatedBytes: after.TotalAlloc - before.TotalAlloc,
		HeapEndBytes:   after.HeapAlloc,
		GCCount:        after.NumGC - before.NumGC,
	}
	successes := make([]time.Duration, 0, config.Requests)
	for i, err := range outcomes {
		switch {
		case err == nil:
			result.Successes++
			successes = append(successes, latencies[i])
		case errors.Is(err, soroi.ErrQueueFull):
			result.Rejected++
		case errors.Is(err, context.DeadlineExceeded):
			result.Timeouts++
		default:
			result.Failures++
		}
	}
	slices.Sort(latencies)
	slices.Sort(successes)
	result.P95MS = float64(percentile(latencies, 95)) / float64(time.Millisecond)
	result.P99MS = float64(percentile(latencies, 99)) / float64(time.Millisecond)
	result.SuccessRate = float64(result.Successes) / float64(config.Requests)
	if len(successes) > 0 {
		p95 := float64(percentile(successes, 95)) / float64(time.Millisecond)
		p99 := float64(percentile(successes, 99)) / float64(time.Millisecond)
		result.SuccessP95MS, result.SuccessP99MS = &p95, &p99
	}
	return result
}

// percentile returns the nearest-rank duration from sorted samples; empty populations return zero.
func percentile(samples []time.Duration, percent int) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	return samples[(len(samples)*percent+99)/100-1]
}
