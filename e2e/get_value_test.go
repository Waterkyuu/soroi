package e2e_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	soroi "github.com/Waterkyuu/soroi"
)

// TestHTTPGetKeepsStaleValues reads an HTTP result until its maximum age without reloading it.
func TestHTTPGetKeepsStaleValues(t *testing.T) {
	api := newUserAPI(t)
	var now atomic.Int64
	client := soroi.NewClient(soroi.Options{
		Clock:     func() time.Time { return time.Unix(0, now.Load()) },
		StaleTime: time.Minute,
		MaxAge:    2 * time.Minute,
	})
	t.Cleanup(client.Close)
	loader := readJSON[user](api.server.Client(), api.server.URL+"/users/42")
	if _, err := soroi.Fetch(context.Background(), client, "user:42", loader); err != nil {
		t.Fatal(err)
	}
	now.Store(int64(time.Minute))
	value, ok := soroi.Get[user](client, "user:42")
	if !ok || value.Name != "Alice" {
		t.Fatalf("stale value = (%+v, %v)", value, ok)
	}
	state := soroi.GetState[user](client, "user:42")
	if !state.HasData || !state.Stale || state.Fetching {
		t.Fatalf("stale state = %+v", state)
	}
	now.Store(int64(2 * time.Minute))
	value, ok = soroi.Get[user](client, "user:42")
	if ok || value.Name != "" {
		t.Fatalf("over-age value = (%+v, %v)", value, ok)
	}
	if api.count("/users/42") != 1 {
		t.Fatal("reading cached data started an HTTP request")
	}
}
