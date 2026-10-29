# Infinite queries

[简体中文](infinite-queries.zh-CN.md)

`InfiniteQuery` retains loaded task pages when a CLI or TUI requests more tasks. One subscription exposes the accumulated pages, whether another page exists, and loading or failure state.

## Installation

Requires Go 1.22 or later and no external service. To run against a checkout containing this API, create a separate example directory and run these commands there. Replace `/absolute/path/to/soroi` with the checkout root:

```sh
go mod init example.com/tasks
go mod edit -replace=github.com/Waterkyuu/soroi=/absolute/path/to/soroi
go get github.com/Waterkyuu/soroi
```

## Usage

`InfiniteQuery(client, key, initialPageParam, fetchPage, getNextPageParam, options...)` creates a subscription and normally loads the first page automatically. Go infers the page and parameter types. `fetchPage` receives the context and current page parameter; return one backend response or an error. `getNextPageParam` returns the next parameter and a boolean indicating whether another page exists. Zero-valued parameters are valid when the boolean is true.

Keep page size, filters, and identity scope in the comparable query key and the loader. The changing cursor is managed inside the sequence. Reuse one application-owned `Client` and close each handle when its consumer leaves.

### Complete example

Save the following as `main.go` in the example module. The in-memory loader returns at most two task titles per page. `Disable: true` makes the first request explicit, keeping this example sequential; later calls append another page.

```go
package main

import (
	"context"
	"fmt"

	"github.com/Waterkyuu/soroi"
)

// TaskPage contains tasks and the next offset, or zero at the end.
type TaskPage struct {
	// Items contains this page's task titles.
	Items []string
	// Next is the offset for the next request.
	Next int
}

// TaskListKey separates lists requested with different page sizes.
type TaskListKey struct {
	// PageSize is the maximum number of tasks per page.
	PageSize int
}

// main loads two pages while retaining the first page's tasks.
func main() {
	client := soroi.NewClient(soroi.Options{})
	defer client.Close()

	all := []string{"Write docs", "Fix tests", "Publish release"}
	pageSize := 2
	tasks := soroi.InfiniteQuery(
		client,
		TaskListKey{PageSize: pageSize},
		0,
		func(_ context.Context, offset int) (TaskPage, error) {
			end := min(offset+pageSize, len(all))
			var next int
			if end < len(all) {
				next = end
			}
			return TaskPage{Items: all[offset:end], Next: next}, nil
		},
		func(last TaskPage) (int, bool) {
			return last.Next, last.Next != 0
		},
		soroi.QueryOptions{Disable: true},
	)
	defer tasks.Close()

	for range 2 {
		data, err := tasks.FetchNextPage(context.Background())
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

Run `go run .` from the example directory. Expected output:

```text
pages: 1 more: true
pages: 2 more: false
[Write docs Fix tests]
[Publish release]
```

The page count grows from one to two, and both pages remain available. The final continuation flag is false.

### Observe the list

In a UI consumer of the same `tasks` handle, use `Updates()` to receive state changes. The following snippet uses `fmt` from the complete example:

```go
for state := range tasks.Updates() {
    fmt.Println(len(state.Data.Pages), state.FetchingNextPage, state.HasNextPage, state.Err)
}
```

`InfiniteSnapshot` embeds `Snapshot[InfiniteData[Page, Param]]`: `Data.Pages` contains ordered responses, and `Data.PageParams` contains their matching request parameters. `HasData`, `Status`, `Err`, `Fetching`, `Stale`, `UpdatedAt`, and `ExpiresAt` retain their ordinary query meanings. `HasNextPage` is false before data is available. `FetchingNextPage` distinguishes appending from initial loading and refreshing. Slow readers can skip intermediate states.

### Load more and refresh

| Operation | Result |
| --- | --- |
| `tasks.FetchNextPage(ctx)` | Wait for one additional page, or the first page when no data exists; return the accumulated `InfiniteData` and an error |
| `tasks.Refetch(ctx)` | Reload the current sequence from its initial parameter, following the newly returned cursors |
| `tasks.Snapshot()` | Read current data and state without loading |
| `tasks.SetEnabled(enabled)` | Control this consumer's automatic loading and polling |
| `tasks.Close()` | Release this subscription without canceling shared work |

Blocking operations belong in the TUI's asynchronous command, while its message loop consumes updates. Once no next page exists, `FetchNextPage` returns the current data and nil without a backend call. A failed operation leaves prior pages available through `Snapshot()` and `Updates()`; inspect `Err` and retry the operation when appropriate.

## Configuration

Supply zero or one existing `QueryOptions`; omitted options inherit the Client policy. See [query options](query-options.en-US.md) for individual settings.

| Setting | Pagination behavior |
| --- | --- |
| `Disable`, `SetEnabled` | Control automatic initial loading, stale refreshes, and polling; explicit calls still execute |
| `RefetchInterval` | Refresh the loaded sequence using the existing shared timer; skip occupied intervals |
| `Retry`, `RetryIf`, `RetryDelay` | Retry the failed page, without reloading earlier successful pages within that operation |
| `Timeout`, `Executor` | Bound the entire operation, including queue and retry waits; acquire capacity separately for each page callback |

## Error handling and common problems

| Symptom | Action |
| --- | --- |
| `ErrQueryBusy` | An append conflicts with initial loading or refreshing, or a refresh conflicts with an append. Wait for completion and try again; conflicting operations are not queued. |
| Page request fails | Continue displaying retained pages; report `Snapshot().Err` and retry the failed append or refresh. |
| Canceled context or timeout | Handle the returned context error. Cancellation follows the existing shared-request ownership rules and does not discard prior pages. |
| Invalid key, incompatible cached type, nil fetcher, or extra options | Inspect the failed handle's snapshot or initial update; no loading is admitted. Existing error values apply. |
| Nil `getNextPageParam` | Supply a continuation function; construction panics before registration when it is nil. |

## Behavior and limitations

- Same-key handles share one cached sequence and one active request. Concurrent appends merge; concurrent refreshes merge. Callers must use consistent page types, parameter types, and pagination semantics for that key.
- A refresh loads at most the current page count, starting from the initial parameter and deriving each next cursor from the new preceding response. An earlier end shortens the sequence. Old pages remain visible until the complete replacement succeeds.
- `getNextPageParam` runs outside the Client lock during loading, snapshot reads, and update projection. It must be prompt and pure; concurrent invocations require synchronization for any captured mutable state. After `Close`, captured projections finish before `Updates()` closes.
- The entire sequence is one cache value and one LRU entry. Freshness and maximum age apply to that value, rather than each page separately. Successful appends install a new aggregate and renew its ordinary freshness and age. The application controls the number of retained pages.
- `Stats().Loads` counts shared operations, not individual pages. Diagnostic `Attempts` counts actual page callbacks, while `Stats().Retries` counts retries of failed pages.
- Existing `Get`, `Set`, `Update`, `Invalidate`, `InvalidateAndWait`, `Cancel`, and `Remove` operate on the same key. Treat cached pages as immutable. Copy modified slices and keep `Pages` and `PageParams` equal in length and corresponding order.
- This API loads pages forward. It does not infer termination from an empty page; only the continuation boolean ends the sequence.

## Complete examples and related guides

- [Queries](queries.en-US.md): ordinary subscriptions, enablement, and shared cancellation.
- [Invalidation](invalidation.en-US.md): awaited refreshes and bounded follow-up loads.
- [Atomic cache updates](updates.en-US.md): update cached values without overwriting concurrent changes.
