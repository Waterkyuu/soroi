package main

import (
	"context"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/soroi"
	redistore "github.com/Waterkyuu/soroi/store/redis"
	"github.com/redis/go-redis/v9"
)

// source tracks physical backend calls independently of logical soroi loads and retries.
type source struct {
	// delay models service time shared by individual and bulk operations.
	delay time.Duration
	// mu serializes callback admission with final resource measurement.
	mu sync.Mutex
	// stopped prevents canceled library goroutines from starting work after measurement ends.
	stopped bool
	// pending waits for all source callbacks admitted before shutdown.
	pending sync.WaitGroup
	// calls counts entries after setup, including canceled work.
	calls atomic.Int64
	// active counts callbacks that have entered but not returned.
	active atomic.Int64
	// peak retains the maximum simultaneously active callback count.
	peak atomic.Int64
}

// load simulates cancellable backend work while measuring actual callback occupancy.
func (s *source) load(ctx context.Context) (int, error) {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return 0, context.Canceled
	}
	s.pending.Add(1)
	s.mu.Unlock()
	defer s.pending.Done()
	s.calls.Add(1)
	active := s.active.Add(1)
	defer s.active.Add(-1)
	for peak := s.peak.Load(); active > peak; peak = s.peak.Load() {
		if s.peak.CompareAndSwap(peak, active) {
			break
		}
	}
	if s.delay == 0 {
		return 42, ctx.Err()
	}
	timer := time.NewTimer(s.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-timer.C:
		return 42, nil
	}
}

// finish drains admitted source work and prevents late callback accounting from crossing case boundaries.
func (s *source) finish() {
	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()
	s.pending.Wait()
}

// runCase prepares one workload outside measurement and waits for all client-owned callbacks before reading counters.
func runCase(
	ctx context.Context,
	config configuration,
	scenario string,
	mode string,
	remote *redis.Client,
) (caseResult, error) {
	backend := &source{delay: config.SourceDelay}
	client := soroi.NewClient(soroi.Options{StaleTime: time.Minute})
	var call func(int) error
	shared := make([]*soroi.SharedCache[int], 0)
	var batcher *soroi.Batcher[int, int]
	switch scenario {
	case "hot-expiry":
		var clock atomic.Int64
		client.Close()
		client = soroi.NewClient(soroi.Options{
			StaleTime: time.Minute, Clock: func() time.Time { return time.Unix(0, clock.Load()) },
		})
		if err := soroi.Set(client, "hot", 1); err != nil {
			client.Close()
			return caseResult{}, err
		}
		// Advance the freshness clock once; latency still uses the real monotonic clock.
		clock.Store(int64(time.Minute))
		call = func(int) error {
			if mode == "baseline" {
				_, err := backend.load(ctx)
				return err
			}
			_, err := soroi.Fetch(ctx, client, "hot", backend.load)
			return err
		}
	case "redis-outage", "downstream-timeout":
		options := soroi.Options{}
		if mode == "soroi" {
			budget, err := soroi.NewExecutor(soroi.ExecutionOptions{MaxConcurrent: config.MaxConcurrent})
			if err != nil {
				client.Close()
				return caseResult{}, err
			}
			options.Executor = budget
		}
		client.Close()
		client = soroi.NewClient(options)
		var store *redistore.Store
		if scenario == "redis-outage" {
			var err error
			store, err = redistore.New(remote, redistore.Options{Prefix: "soroi-business-benchmark"})
			if err != nil {
				client.Close()
				return caseResult{}, err
			}
		}
		definitions := make([]soroi.Definition[int], config.Requests)
		for i := range definitions {
			definition, err := soroi.QueryDefinition(i, backend.load)
			if err != nil {
				client.Close()
				return caseResult{}, err
			}
			if store != nil {
				definition.Shared, err = soroi.NewSharedCache(store, soroi.SharedCacheOptions[int]{
					Key: strconv.Itoa(i), Format: "int.v1", TTL: time.Minute, Timeout: 20 * time.Millisecond,
				})
				if err != nil {
					client.Close()
					return caseResult{}, err
				}
				shared = append(shared, definition.Shared)
			}
			definitions[i] = definition
		}
		call = func(i int) error {
			operation := ctx
			if scenario == "downstream-timeout" {
				var cancel context.CancelFunc
				operation, cancel = context.WithTimeout(ctx, config.Timeout)
				defer cancel()
			}
			_, err := definitions[i].Fetch(operation, client)
			return err
		}
	case "batching":
		var err error
		batcher, err = soroi.NewBatcher(
			ctx,
			func(ctx context.Context, keys []int) (map[int]soroi.BatchResult[int], error) {
				value, err := backend.load(ctx)
				if err != nil {
					return nil, err
				}
				values := make(map[int]soroi.BatchResult[int], len(keys))
				for _, key := range keys {
					values[key] = soroi.BatchResult[int]{Data: value}
				}
				return values, nil
			},
			soroi.BatchOptions{Wait: config.BatchWait, MaxBatchSize: config.BatchSize},
		)
		if err != nil {
			client.Close()
			return caseResult{}, err
		}
		call = func(i int) error {
			if mode == "baseline" {
				_, err := backend.load(ctx)
				return err
			}
			_, err := batcher.Load(ctx, i)
			return err
		}
	}
	result := caseResult{
		Scenario: scenario, Mode: mode, Configuration: config,
		GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH, Procs: runtime.GOMAXPROCS(0),
		Measurement: measure(config, call, func() {
			client.Close()
			if batcher != nil {
				batcher.Close()
			}
			backend.finish()
		}),
	}
	result.SourceCalls, result.PeakSourceCalls = backend.calls.Load(), backend.peak.Load()
	for _, cache := range shared {
		result.SharedReadErrors += cache.Stats().ReadErrors
	}
	return result, nil
}
