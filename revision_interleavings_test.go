package soroi_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Waterkyuu/soroi"
)

// TestRevisionInterleavings checks real changes while an Update callback is outside the lock.
func TestRevisionInterleavings(t *testing.T) {
	for _, action := range []string{"same-value", "invalidate", "remove-recreate", "clear-recreate", "load-recreate", "unrelated"} {
		t.Run(action, func(t *testing.T) {
			client := soroi.NewClient(soroi.Options{StaleTime: time.Hour})
			defer client.Close()
			for iteration := 0; iteration < 16; iteration++ {
				if err := soroi.Set(client, "target", uint64(100)); err != nil {
					t.Fatal(err)
				}
				entered, resume := make(chan struct{}), make(chan struct{})
				result := make(chan error, 1)
				go func() {
					result <- soroi.Update(client, "target", func(v uint64) (uint64, error) {
						close(entered)
						<-resume
						return v + 10, nil
					})
				}()
				<-entered
				var err error
				switch action {
				case "same-value":
					err = soroi.Set(client, "target", uint64(100))
				case "invalidate":
					err = client.Invalidate("target")
				case "remove-recreate":
					err = client.Remove("target")
					if err == nil {
						err = soroi.Set(client, "target", uint64(100))
					}
				case "clear-recreate":
					client.Clear()
					err = soroi.Set(client, "target", uint64(100))
				case "load-recreate":
					err = client.Remove("target")
					if err == nil {
						var value uint64
						value, err = soroi.Fetch(
							context.Background(),
							client,
							"target",
							func(context.Context) (uint64, error) { return 100, nil },
						)
						if err == nil && value != 100 {
							err = fmt.Errorf("load returned %d", value)
						}
					}
				case "unrelated":
					for i := 0; i < 256 && err == nil; i++ {
						err = soroi.Set(client, fmt.Sprintf("other:%d", i), uint64(iteration+1))
					}
				}
				close(resume)
				updateErr := <-result
				if err != nil {
					t.Fatal(err)
				}
				want := uint64(100)
				if action == "unrelated" {
					want = 110
					if updateErr != nil {
						t.Fatalf("unrelated write caused conflict: %v", updateErr)
					}
				} else if !errors.Is(updateErr, soroi.ErrUpdateConflict) {
					t.Fatalf("lost intervening change: %v", updateErr)
				}
				state := soroi.Get[uint64](client, "target")
				if !state.HasData || state.Data != want {
					t.Fatalf("unexpected final data: %+v", state)
				}
			}
		})
	}
}

// TestRevisionParallelValues verifies all committed changes across independent keys and Clear cycles.
func TestRevisionParallelValues(t *testing.T) {
	client := soroi.NewClient(soroi.Options{StaleTime: time.Hour})
	defer client.Close()
	for cycle := 0; cycle < 3; cycle++ {
		client.Clear()
		var workers sync.WaitGroup
		for worker := 0; worker < 32; worker++ {
			workers.Add(1)
			go func(worker int) {
				defer workers.Done()
				key := fmt.Sprintf("worker:%d", worker)
				if err := soroi.Set(client, key, uint64(0)); err != nil {
					t.Error(err)
					return
				}
				for i := 0; i < 100; i++ {
					if err := soroi.Update(
						client,
						key,
						func(v uint64) (uint64, error) { return v + 1, nil },
					); err != nil {
						t.Error(err)
						return
					}
				}
			}(worker)
		}
		workers.Wait()
		for worker := 0; worker < 32; worker++ {
			value := soroi.Get[uint64](client, fmt.Sprintf("worker:%d", worker))
			if !value.HasData || value.Data != 100 {
				t.Fatalf("lost committed updates: %+v", value)
			}
		}
	}
}
