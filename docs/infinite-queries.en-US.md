# Infinite queries

[简体中文](infinite-queries.zh-CN.md)

`InfiniteQuery` supports a task list's “load more” action: each request appends one page while retaining the pages already displayed. The handle provides the accumulated data, loading state, errors, and whether another page is available.

## Usage

### Create a paginated query

First define the response returned by one page request:

```go
// TaskPage is one response from the task service.
type TaskPage struct {
	// Items contains this page's task titles.
	Items []string
	// NextCursor is empty when no more pages remain.
	NextCursor string
}
```

The following snippets run inside `main` with `context`, `fmt`, and `soroi` imported. This setup creates a Client and defines `fetchPage` with two fixed responses; replace its body with the application's backend request.

`InfiniteQuery` returns `*InfiniteQueryHandle[TaskPage, string]`, with both types inferred from the arguments. The example passes `Disable: true` so construction subscribes without loading. Call `FetchNextPage` to load the first page explicitly. Omitting this option starts missing or stale data loading in the background.

```go
client := soroi.NewClient(soroi.Options{})
defer client.Close()
ctx := context.Background()

fetchPage := func(_ context.Context, cursor string) (TaskPage, error) {
	if cursor == "" {
		return TaskPage{Items: []string{"Write docs"}, NextCursor: "next"}, nil
	}
	return TaskPage{Items: []string{"Fix tests"}}, nil
}

tasks := soroi.InfiniteQuery(
	client,
	"tasks",
	"",
	fetchPage,
	func(last TaskPage, _ []TaskPage, _ string, _ []string) (string, bool) {
		return last.NextCursor, last.NextCursor != ""
	},
	soroi.QueryOptions{Disable: true},
)
defer tasks.Close()
```

The arguments have these roles:

| Argument | Usage |
| --- | --- |
| `client` | Share cached pages and active requests across consumers; reuse an application-owned Client |
| `key` | Identify the complete list; include filters, page size, and identity scope when they affect the result |
| `initialPageParam` | Parameter for the first request; here `""` means the first page |
| `fetchPage` | `func(context.Context, Param) (Page, error)`; fetch one page using the supplied parameter |
| `getNextPageParam` | `func(Page, []Page, Param, []Param) (Param, bool)`; derive the next parameter from pages and request parameters; false ends pagination |
| `options` | Zero or one existing `QueryOptions`; omitted settings inherit the Client policy |

The changing cursor belongs to the page sequence, rather than the list key. Set page size in the backend request and include it in the key; it is not a separate pagination option. A zero-valued page parameter remains valid when the continuation boolean is true.

### Calculate the next page parameter

The continuation callback receives four arguments in this order:

| Argument | Meaning |
| --- | --- |
| `lastPage` | The last successfully loaded response |
| `allPages` | All successfully loaded responses, including `lastPage`, in request order |
| `lastPageParam` | The request parameter used to load `lastPage` |
| `allPageParams` | The corresponding parameters, including `lastPageParam`, in the same order as `allPages` |

During an append or state read, these arguments describe the retained sequence. During a refresh, they describe only the successfully reloaded prefix; old pages and parameters are excluded. Treat every argument as read-only, including slices and nested values. The callback must be pure and may run concurrently.

For a backend that returns a bare array without a cursor, calculate the next offset from `lastPageParam`. This snippet reuses `client` and `ctx` from the setup and simulates a backend where a response shorter than `pageSize` ends pagination:

```go
pageSize := 2
titles := []string{"Write docs", "Fix tests", "Publish release"}
fetchItems := func(_ context.Context, offset int) ([]string, error) {
	if offset >= len(titles) {
		return []string{}, nil
	}
	return titles[offset:min(offset+pageSize, len(titles))], nil
}
offsetTasks := soroi.InfiniteQuery(
	client,
	"tasks-offset-size-2",
	0,
	fetchItems,
	func(last []string, _ [][]string, lastOffset int, _ []int) (int, bool) {
		return lastOffset + len(last), len(last) == pageSize
	},
	soroi.QueryOptions{Disable: true},
)
defer offsetTasks.Close()

for range 2 {
	if _, err := offsetTasks.FetchNextPage(ctx); err != nil {
		panic(err)
	}
}
fmt.Println(offsetTasks.Snapshot().Data.PageParams) // [0 2]
```

The callback signature requires all four arguments. Use `_` for unused arguments, as in the cursor example. The `(Param, bool)` return contract is unchanged; false ends pagination.

### Load the first or next page

`FetchNextPage(ctx)` waits for a page request and returns `(InfiniteData[TaskPage, string], error)`. Without data it loads the first page; subsequent calls append one page. This snippet reuses `tasks` and `ctx` from the setup:

```go
data, err := tasks.FetchNextPage(ctx)
if err != nil {
	fmt.Println("Load failed:", err)
	return
}
fmt.Println("pages:", len(data.Pages))
```

`data.Pages` contains all loaded responses, and `data.PageParams` contains their corresponding request parameters. A request failure preserves prior cached pages. With no next page, the call returns current data and nil without requesting the backend. Run blocking calls in a TUI's asynchronous command.

### Read current data and state

`Snapshot()` returns `InfiniteSnapshot[TaskPage, string]` immediately, without starting or waiting for a request. After loading, render the accumulated pages:

```go
state := tasks.Snapshot()
for _, page := range state.Data.Pages {
	fmt.Println(page.Items)
}
fmt.Println("has next:", state.HasNextPage)
```

| Field | Usage |
| --- | --- |
| `Data.Pages` | Render the loaded pages in order |
| `Data.PageParams` | Read the request parameter corresponding to each page |
| `HasData` | Distinguish available data from an initial query without a result |
| `HasNextPage` | Enable “load more”; false before data exists or after the last page |
| `Fetching` | Detect any active load, including initial loading, appending, and refreshing |
| `FetchingNextPage` | Show a loading indicator specifically for an append |
| `Err` | Report the latest request failure while continuing to display available data |

`InfiniteSnapshot` embeds the existing `Snapshot[InfiniteData[Page, Param]]`; `Status`, `Stale`, `UpdatedAt`, and `ExpiresAt` retain their ordinary query meanings. Cached data is shared and must be treated as immutable.

### Subscribe to updates

`Updates()` returns a channel of `InfiniteSnapshot` values. Use it in the UI's state consumer, separately from the actions that call `FetchNextPage` or `Refetch`:

```go
for state := range tasks.Updates() {
	fmt.Println(state.Data.Pages, state.FetchingNextPage, state.Err)
}
```

The channel delivers initial and latest states; slow consumers can skip intermediate changes. The loop waits for updates until the subscription closes. Use `Snapshot()` when only a single current value is needed.

### Refresh loaded pages

`Refetch(ctx)` waits for a refresh and returns the same `InfiniteData` type as `FetchNextPage`. It starts from the initial parameter and derives each subsequent cursor from the new response, rather than appending another page:

```go
if _, err := tasks.Refetch(ctx); err != nil {
	fmt.Println("Refresh failed:", err)
	return
}
```

A refresh reloads at most the current page count; an earlier end shortens the list. Old pages remain visible until the complete replacement succeeds. A refresh failure retains the old list and reports the error.

### Control automatic loading and release the subscription

`SetEnabled(true)` enables automatic loading of missing or stale data and participation in polling. This resumes automatic behavior for the disabled handle from the setup:

```go
if err := tasks.SetEnabled(true); err != nil {
	fmt.Println("Enable failed:", err)
	return
}
```

`SetEnabled(false)` disables only this consumer's automatic loading; it still receives shared updates, and explicit `FetchNextPage` and `Refetch` calls still execute. Existing work and other consumers are unaffected.

Call `tasks.Close()` when the consumer leaves, normally with the `defer tasks.Close()` registered immediately after construction. It releases the subscription and its polling participation without canceling shared requests. The update channel closes after already-received states have been processed. Close the Client when the application shuts down.

## Complete example

Save this program as `main.go` in a module importing soroi and run `go run .` from that directory. It loads two pages and displays the retained task titles:

```go
package main

import (
	"context"
	"fmt"

	"github.com/Waterkyuu/soroi"
)

// TaskPage is one response from the task service.
type TaskPage struct {
	// Items contains this page's task titles.
	Items []string
	// NextCursor is empty when no more pages remain.
	NextCursor string
}

// main loads two task pages and reads the accumulated result.
func main() {
	client := soroi.NewClient(soroi.Options{})
	defer client.Close()
	ctx := context.Background()

	fetchPage := func(_ context.Context, cursor string) (TaskPage, error) {
		if cursor == "" {
			return TaskPage{Items: []string{"Write docs"}, NextCursor: "next"}, nil
		}
		return TaskPage{Items: []string{"Fix tests"}}, nil
	}

	tasks := soroi.InfiniteQuery(
		client,
		"tasks",
		"",
		fetchPage,
		func(last TaskPage, _ []TaskPage, _ string, _ []string) (string, bool) {
			return last.NextCursor, last.NextCursor != ""
		},
		soroi.QueryOptions{Disable: true},
	)
	defer tasks.Close()

	for range 2 {
		data, err := tasks.FetchNextPage(ctx)
		if err != nil {
			panic(err)
		}
		fmt.Println("pages:", len(data.Pages), "more:", tasks.Snapshot().HasNextPage)
	}
	for _, page := range tasks.Snapshot().Data.Pages {
		fmt.Println(page.Items)
	}
}
```

Expected output:

```text
pages: 1 more: true
pages: 2 more: false
[Write docs]
[Fix tests]
```

## Configuration

Reuse [QueryOptions](query-options.en-US.md) for loading policy:

| Configuration | Effect |
| --- | --- |
| `Disable`, `SetEnabled` | Control automatic loading; the default permits loading |
| `RefetchInterval` | Refresh loaded pages; non-positive values disable polling |
| `Retry`, `RetryIf`, `RetryDelay` | Retry the failed page without repeating earlier successful pages within the operation |
| `Timeout`, `Executor` | Bound the entire operation, including waits; acquire execution capacity for each page callback |

## Error handling and common problems

| Condition | Handling |
| --- | --- |
| `ErrQueryBusy` | Appending conflicts with initial loading or refreshing, or refreshing conflicts with appending. Wait for the active operation and try again. |
| Page request fails | Report the returned error or `Snapshot().Err`; continue displaying retained data and retry the operation as needed. |
| Context canceled or timed out | Handle the context error; shared-request cancellation follows the ordinary query ownership rules. |
| Invalid key, incompatible type, nil fetcher, or extra options | Inspect `Snapshot().Err` or the initial update; existing query errors apply and no load starts. |
| Nil `getNextPageParam` | Supply a continuation callback; construction panics before subscribing when it is nil. |

## Behavior and limitations

- Same-key handles share pages and in-flight requests. Concurrent appends merge, as do concurrent refreshes; conflicting operation kinds are not queued. Use consistent page types, parameter types, and pagination semantics for the same key.
- The continuation callback must return promptly and be pure. It runs outside the Client lock during loading, snapshot reads, and update delivery, and can run concurrently.
- The complete sequence is one cache value and one LRU entry. Freshness and maximum age apply to the aggregate; a successful append installs a new value and renews those times. The application controls the number of retained pages.
- Existing `Get`, `Set`, `Update`, `Invalidate`, `InvalidateAndWait`, `Cancel`, and `Remove` operate on the list key. For manual updates, copy modified data and keep `Pages` and `PageParams` equal in length and corresponding order.
- Pagination proceeds forward and ends according to the continuation boolean, independently of page length.

## Complete examples and related guides

- [Queries](queries.en-US.md): ordinary subscriptions and shared cancellation rules.
- [Invalidation](invalidation.en-US.md): refresh after writes and await the result.
- [Atomic cache updates](updates.en-US.md): modify cached pages without overwriting concurrent changes.
