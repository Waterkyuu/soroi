// Command failure_protection demonstrates bounded fallback using an intentionally closed Redis client.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/soroi"
	redistore "github.com/Waterkyuu/soroi/store/redis"
	"github.com/redis/go-redis/v9"
)

// main reports a reproducible cache outage without requiring or stopping a Redis server.
func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := run(ctx, os.Stdout)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run holds one source call open while another client attempts a different key under the same budget.
func run(ctx context.Context, output io.Writer) error {
	remote := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", MaxRetries: -1})
	// Closing the connection pool injects an immediate transport failure; no server is contacted.
	if err := remote.Close(); err != nil {
		return err
	}
	store, err := redistore.New(remote, redistore.Options{Prefix: "soroi-failure-demo"})
	if err != nil {
		return err
	}
	budget, err := soroi.NewExecutor(soroi.ExecutionOptions{MaxConcurrent: 1, MaxQueued: 0})
	if err != nil {
		return err
	}
	options := soroi.Options{Executor: budget, StaleTime: time.Minute, Timeout: time.Second, Retry: 2}
	a, b := soroi.NewClient(options), soroi.NewClient(options)
	defer a.Close()
	defer b.Close()
	if err := soroi.Set(b, "cached", "cached"); err != nil {
		return err
	}
	started, release := make(chan struct{}), make(chan struct{})
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	var calls atomic.Int32
	loader := func(ctx context.Context) (string, error) {
		calls.Add(1)
		close(started)
		select {
		case <-release:
			return "loaded", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	first, err := soroi.QueryDefinition("first", loader)
	if err != nil {
		return err
	}
	second, err := soroi.QueryDefinition("second", loader)
	if err != nil {
		return err
	}
	for _, definition := range []*soroi.Definition[string]{&first, &second} {
		definition.Shared, err = soroi.NewSharedCache(store, soroi.SharedCacheOptions[string]{
			Key: definition.Key.(string), Format: "string.v1", TTL: time.Minute, Timeout: 50 * time.Millisecond,
		})
		if err != nil {
			return err
		}
	}
	done := make(chan error, 1)
	go func() {
		value, err := first.Fetch(work, a)
		if err == nil && value != "loaded" {
			err = errors.New("unexpected first result")
		}
		done <- err
	}()
	select {
	case <-started:
	case err := <-done:
		return fmt.Errorf("first load did not start: %w", err)
	}
	local, err := soroi.Fetch[string](ctx, b, "cached", nil)
	if err != nil {
		return err
	}
	_, err = second.Fetch(ctx, b)
	if !errors.Is(err, soroi.ErrQueueFull) {
		return fmt.Errorf("expected overload rejection, got %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		return err
	}
	_, err = fmt.Fprintf(output,
		"local hit: %s\nsecond rejected: true\nfirst: loaded\nsource calls: %d\nrejected: %d\nshared read errors: %d\n",
		local, calls.Load(), budget.Stats().Rejected, first.Shared.Stats().ReadErrors)
	return err
}
