package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	soroi "github.com/Waterkyuu/soroi"
)

// taskPage represents a cursor-based HTTP response with independently retained pages.
type taskPage struct {
	// Tasks contains the visible task titles for this response.
	Tasks []string `json:"tasks"`
	// NextCursor identifies the next page, or is empty at the end.
	NextCursor string `json:"nextCursor"`
}

// TestHTTPInfiniteQuery loads and retains task pages, reports append failure, and refreshes through public APIs.
func TestHTTPInfiniteQuery(t *testing.T) {
	var calls atomic.Int32
	var fail atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Query().Get("pageSize") != "2" {
			http.Error(w, "missing page size", http.StatusBadRequest)
			return
		}
		page := taskPage{Tasks: []string{"Write docs", "Fix tests"}, NextCursor: "next"}
		if r.URL.Query().Get("cursor") == "next" {
			if fail.Load() {
				http.Error(w, "offline", http.StatusServiceUnavailable)
				return
			}
			page = taskPage{Tasks: []string{"Publish release"}}
		}
		json.NewEncoder(w).Encode(page)
	}))
	t.Cleanup(server.Close)
	client := soroi.NewClient(soroi.Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	key := struct{ PageSize int }{PageSize: 2}
	load := func(ctx context.Context, cursor string) (taskPage, error) {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"?pageSize=2&cursor="+cursor, nil)
		if err != nil {
			return taskPage{}, err
		}
		response, err := server.Client().Do(request)
		if err != nil {
			return taskPage{}, err
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return taskPage{}, fmt.Errorf("load tasks: HTTP %d", response.StatusCode)
		}
		var page taskPage
		err = json.NewDecoder(response.Body).Decode(&page)
		return page, err
	}
	query := soroi.InfiniteQuery(
		client,
		key,
		"",
		load,
		func(last taskPage, _ []taskPage, _ string, _ []string) (string, bool) {
			return last.NextCursor, last.NextCursor != ""
		},
		soroi.QueryOptions{Disable: true},
	)
	t.Cleanup(query.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if data, err := query.FetchNextPage(ctx); err != nil || len(data.Pages) != 1 || !query.Snapshot().HasNextPage {
		t.Fatalf("first HTTP page = %+v, %v", data, err)
	}
	fail.Store(true)
	if _, err := query.FetchNextPage(ctx); err == nil || len(query.Snapshot().Data.Pages) != 1 {
		t.Fatal("failed append did not retain the first page")
	}
	fail.Store(false)
	if data, err := query.FetchNextPage(ctx); err != nil || len(data.Pages) != 2 || query.Snapshot().HasNextPage {
		t.Fatalf("second HTTP page = %+v, %v", data, err)
	}
	if _, err := query.FetchNextPage(ctx); err != nil || calls.Load() != 3 {
		t.Fatal("exhausted pagination made another HTTP request")
	}
	if err := query.SetEnabled(true); err != nil {
		t.Fatal(err)
	}
	if _, err := client.InvalidateAndWait(ctx, key); err != nil || calls.Load() != 5 {
		t.Fatalf("invalidated HTTP sequence = %v; calls = %d", err, calls.Load())
	}
	cancel()
	if _, err := query.FetchNextPage(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled append = %v", err)
	}
	query.Close()
	for range query.Updates() {
	}
}

// TestHTTPInfiniteQueryOffsets paginates a bare HTTP array using the last request parameter and page history.
func TestHTTPInfiniteQueryOffsets(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		offset, err := strconv.Atoi(r.URL.Query().Get("offset"))
		if err != nil || offset < 0 || offset > 4 {
			http.Error(w, "invalid offset", http.StatusBadRequest)
			return
		}
		items := []int{offset, offset + 1}
		if offset == 4 {
			items = []int{offset}
		}
		json.NewEncoder(w).Encode(items)
	}))
	t.Cleanup(server.Close)
	client := soroi.NewClient(soroi.Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	load := func(ctx context.Context, offset int) ([]int, error) {
		request, err := http.NewRequestWithContext(
			ctx,
			http.MethodGet,
			server.URL+"?offset="+strconv.Itoa(offset),
			nil,
		)
		if err != nil {
			return nil, err
		}
		response, err := server.Client().Do(request)
		if err != nil {
			return nil, err
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("load tasks: HTTP %d", response.StatusCode)
		}
		var items []int
		err = json.NewDecoder(response.Body).Decode(&items)
		return items, err
	}
	next := func(last []int, pages [][]int, lastOffset int, offsets []int) (int, bool) {
		if len(pages) != len(offsets) {
			t.Errorf("HTTP continuation history = %v, %v", pages, offsets)
		}
		return lastOffset + len(last), len(last) == 2
	}
	query := soroi.InfiniteQuery(client, "tasks", 0, load, next, soroi.QueryOptions{Disable: true})
	defer func() {
		query.Close()
		for range query.Updates() {
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for range 3 {
		if _, err := query.FetchNextPage(ctx); err != nil {
			t.Fatal(err)
		}
	}
	state := query.Snapshot()
	if state.HasNextPage || !reflect.DeepEqual(state.Data.PageParams, []int{0, 2, 4}) ||
		!reflect.DeepEqual(state.Data.Pages, [][]int{{0, 1}, {2, 3}, {4}}) {
		t.Fatalf("HTTP offset sequence = %+v", state)
	}
	if _, err := query.FetchNextPage(ctx); err != nil || calls.Load() != 3 {
		t.Fatalf("exhausted HTTP offset sequence = %v; calls = %d", err, calls.Load())
	}
	if data, err := query.Refetch(ctx); err != nil || !reflect.DeepEqual(data.PageParams, []int{0, 2, 4}) {
		t.Fatalf("refreshed HTTP offset sequence = %+v, %v", data, err)
	}
}
