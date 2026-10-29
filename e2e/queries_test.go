package e2e_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	soroi "github.com/Waterkyuu/soroi"
)

// TestHTTPQueries streams partial heterogeneous results and later cache updates through one public subscription.
func TestHTTPQueries(t *testing.T) {
	userStarted, settingsStarted := make(chan struct{}), make(chan struct{})
	userRelease, settingsRelease := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var release <-chan struct{}
		switch r.URL.Path {
		case "/user":
			close(userStarted)
			release = userRelease
		case "/settings":
			close(settingsStarted)
			release = settingsRelease
		}
		select {
		case <-r.Context().Done():
			return
		case <-release:
		}
		if r.URL.Path == "/user" {
			json.NewEncoder(w).Encode(user{Name: "Alice"})
			return
		}
		json.NewEncoder(w).Encode(settings{Theme: "dark"})
	}))
	t.Cleanup(server.Close)
	client := soroi.NewClient(soroi.Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	detail, err := soroi.QueryDefinition("user", readJSON[user](server.Client(), server.URL+"/user"))
	if err != nil {
		t.Fatal(err)
	}
	config, err := soroi.QueryDefinition("settings", readJSON[settings](server.Client(), server.URL+"/settings"))
	if err != nil {
		t.Fatal(err)
	}
	group := soroi.Queries(client, detail, config)
	t.Cleanup(group.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, started := range []<-chan struct{}{userStarted, settingsStarted} {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("independent HTTP queries did not start concurrently")
		}
	}
	close(settingsRelease)
	state := awaitHTTPQueries(t, ctx, group, func(s []soroi.Snapshot[any]) bool { return s[1].HasData })
	if state[0].HasData || !state[0].Fetching || state[1].Data.(settings).Theme != "dark" {
		t.Fatalf("partial HTTP result = %+v", state)
	}
	close(userRelease)
	state = awaitHTTPQueries(t, ctx, group, func(s []soroi.Snapshot[any]) bool {
		ready := s[0].HasData && s[1].HasData
		fetching := s[0].Fetching || s[1].Fetching
		return ready && !fetching
	})
	if state[0].Err != nil || state[1].Err != nil || state[0].Data.(user).Name != "Alice" {
		t.Fatalf("complete HTTP result = %+v", state)
	}
	if err := soroi.Set(client, detail.Key, user{Name: "Bob"}); err != nil {
		t.Fatal(err)
	}
	state = <-group.Updates()
	if state[0].Data.(user).Name != "Bob" || state[1].Data.(settings).Theme != "dark" {
		t.Fatalf("later shared update = %+v", state)
	}
	group.Close()
	if _, open := <-group.Updates(); open {
		t.Fatal("released HTTP query group left updates open")
	}
}

// TestHTTPQueriesWithCombine applies a typed view projection to initial HTTP results and later invalidation.
func TestHTTPQueriesWithCombine(t *testing.T) {
	api := newUserAPI(t)
	client := soroi.NewClient(soroi.Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	detail := soroi.Definition[user]{
		Key:     "user",
		Fetcher: readJSON[user](api.server.Client(), api.server.URL+"/users/42"),
	}
	config := soroi.Definition[settings]{
		Key:     "settings",
		Fetcher: readJSON[settings](api.server.Client(), api.server.URL+"/settings"),
	}
	group := soroi.QueriesWithCombine(
		client,
		func(results []soroi.Snapshot[any]) string {
			_ = client.Stats()
			if !results[0].HasData || !results[1].HasData {
				return "loading"
			}
			return results[0].Data.(user).Name + ":" + results[1].Data.(settings).Theme
		},
		detail,
		config,
	)
	t.Cleanup(group.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	awaitHTTPQueries(t, ctx, group, func(value string) bool { return value == "Alice:dark" })
	api.update(t, user{Name: "Bob"})
	if err := client.Invalidate("user"); err != nil {
		t.Fatal(err)
	}
	awaitHTTPQueries(t, ctx, group, func(value string) bool { return value == "Bob:dark" })
	if group.Snapshot() != "Bob:dark" || api.count("/users/42") != 2 || api.count("/settings") != 1 {
		t.Fatal("custom projection lost independent query execution or snapshot state")
	}
	group.Close()
	for range group.Updates() {
	}
}

// awaitHTTPQueries bounds public update waits without depending on HTTP response timing.
func awaitHTTPQueries[V any](
	t *testing.T,
	ctx context.Context,
	group *soroi.QueriesHandle[V],
	matches func(V) bool,
) V {
	t.Helper()
	for {
		select {
		case state, open := <-group.Updates():
			if !open {
				t.Fatal("HTTP query group closed before the expected state")
			}
			if matches(state) {
				return state
			}
		case <-ctx.Done():
			t.Fatal("HTTP query group did not publish the expected state")
		}
	}
}
