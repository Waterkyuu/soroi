package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Waterkyuu/soroi"
)

// TestPercentile uses nearest-rank boundaries, including an empty success population.
func TestPercentile(t *testing.T) {
	for _, test := range []struct {
		// samples contains sorted caller durations.
		samples []time.Duration
		// percentile selects the nearest rank in the population.
		percentile int
		// want is the duration at that rank or zero for no samples.
		want time.Duration
	}{
		{samples: []time.Duration{}, percentile: 99},
		{samples: []time.Duration{7}, percentile: 95, want: 7},
		{samples: []time.Duration{1, 2, 3, 4}, percentile: 50, want: 2},
		{samples: []time.Duration{1, 2, 3, 4}, percentile: 99, want: 4},
	} {
		if got := percentile(test.samples, test.percentile); got != test.want {
			t.Errorf("percentile(%v, %d) = %v, want %v", test.samples, test.percentile, got, test.want)
		}
	}
}

// TestMeasure keeps successes, overload rejections, timeouts and other failures in separate populations.
func TestMeasure(t *testing.T) {
	outcomes := []error{nil, soroi.ErrQueueFull, context.DeadlineExceeded, errors.New("source failed")}
	config := configuration{Requests: len(outcomes), Workers: 2}
	result := measure(config, func(i int) error { return outcomes[i] }, nil)
	if result.Successes != 1 || result.Rejected != 1 || result.Timeouts != 1 || result.Failures != 1 {
		t.Fatalf("counts = %+v", result)
	}
	if result.SuccessRate != 0.25 || result.SuccessP95MS == nil || result.SuccessP99MS == nil {
		t.Fatalf("success population = %+v", result)
	}
	failed := measure(config, func(int) error { return soroi.ErrQueueFull }, nil)
	if failed.SuccessP95MS != nil || failed.SuccessP99MS != nil || failed.Rejected != config.Requests {
		t.Fatalf("failed population = %+v", failed)
	}
}

// TestHotExpiry confirms an expired hot key reloads once and subsequent callers share its new result.
func TestHotExpiry(t *testing.T) {
	config := configuration{Requests: 20, Workers: 4, MaxConcurrent: 2, BatchSize: 8}
	for _, mode := range []string{"baseline", "soroi"} {
		result, err := runCase(context.Background(), config, "hot-expiry", mode, nil)
		if err != nil {
			t.Fatal(err)
		}
		want := int64(config.Requests)
		if mode == "soroi" {
			want = 1
		}
		if result.SourceCalls != want || result.Measurement.Successes != config.Requests {
			t.Fatalf("%s = %+v", mode, result)
		}
	}
}
