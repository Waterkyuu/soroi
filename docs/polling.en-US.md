# Subscription polling for task progress

[简体中文](polling.zh-CN.md)

A TUI task view needs to show task 42 progressing from 20% to 50% while the backend changes independently. Use `QueryOptions.RefetchInterval` to refresh the subscribed query every three seconds and receive results through `Updates()`. Closing the view releases its subscription and stops its contribution to polling.

The example follows this lifecycle:

1. Open the view and subscribe to `task:42`; the missing result loads immediately.
2. Every three seconds, reload the task even though its local data is still fresh.
3. Render completed snapshots from `Updates()` as progress changes.
4. Close the subscription when the view leaves; its polling timer is released if no other enabled polling subscriber remains.

## Installation

Requires Go 1.22 or later and this repository checkout. No external service or TUI dependency is required. The complete example uses an in-memory loader to simulate backend progress; an application can supply its HTTP or RPC loader unchanged.

From the **repository root**, create an isolated module pointing to the checkout containing the polling API:

```sh
SOROI_SOURCE_DIR="$(pwd)"
SOROI_DEMO_DIR="$(mktemp -d)"
cd "$SOROI_DEMO_DIR"
go mod init example.com/task-progress
go mod edit -replace="github.com/Waterkyuu/soroi=$SOROI_SOURCE_DIR"
go get github.com/Waterkyuu/soroi@v0.0.0
```

The replacement uses local source so the example does not depend on an already published release containing `RefetchInterval`. Keep this terminal in the new module directory.

## Usage

The following snippets are excerpts from `main` in the complete example below. `Task` is that program's result type, with task identity `ID` and percentage `Progress`; `loadTask` is its application-owned `func(context.Context) (Task, error)` loader. The complete program includes their definitions and all imports.

### Open the task view

Create one application-owned Client and a query handle for the view:

```go
client := soroi.NewClient(soroi.Options{StaleTime: time.Minute})
defer client.Close()
query := soroi.Query(client, "task:42", loadTask, soroi.QueryOptions{
	RefetchInterval: 3 * time.Second,
})
defer query.Close()
```

`Query` infers `Task` from `loadTask` and starts loading missing or stale data in the background. The handle owns the subscription until `Close`. `StaleTime` controls ordinary cache reuse; `RefetchInterval` schedules refreshes even while the local data is fresh. `Definition.Query` accepts the same setting through `Definition.Options`.

### Receive progress and close the view

Read the handle's update channel inside the view's update adapter:

```go
for state := range query.Updates() {
	if state.Err != nil {
		panic(state.Err)
	}
	if state.HasData && !state.Fetching {
		fmt.Printf("task %d: %d%%\n", state.Data.ID, state.Data.Progress)
		if state.Data.Progress == 100 {
			query.Close()
			break
		}
	}
}
```

`Updates()` delivers the initial and latest snapshots; slow readers may skip intermediate transitions. A refresh sets `Fetching` while retaining available data. This snippet prints completed results and closes the view at 100%. In a Bubble Tea application, forward snapshots through a `tea.Cmd` and close the handle when removing its owning view. The complete example prints each result directly so it runs without a UI framework.

### Pause and resume automatic loading

Use the existing enablement API to suspend this view's automatic loading:

```go
if err := query.SetEnabled(false); err != nil {
	panic(err)
}
if err := query.SetEnabled(true); err != nil {
	panic(err)
}
```

Disabling removes this subscription from interval selection. Enabling reuses fresh data or loads missing/stale data, then starts a new polling interval when this subscription supplies the shared schedule. Explicit `Refetch` remains available while disabled.

## Complete example

Save the following program as `main.go` in the module created above. Its loader simulates backend reads returning 20%, 50%, then 100%; a production loader would read actual task state instead.

```go
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/soroi"
)

// Task is the progress snapshot displayed by the task view.
type Task struct {
	// ID identifies the background task independently of its current progress.
	ID int
	// Progress is the completed percentage, from zero through one hundred.
	Progress int
}

// main opens a task view, receives periodic progress updates, and closes the subscription.
func main() {
	client := soroi.NewClient(soroi.Options{StaleTime: time.Minute})
	defer client.Close()

	var reads atomic.Int32
	loadTask := func(context.Context) (Task, error) {
		progress := 100
		switch reads.Add(1) {
		case 1:
			progress = 20
		case 2:
			progress = 50
		}
		return Task{ID: 42, Progress: progress}, nil
	}
	query := soroi.Query(client, "task:42", loadTask, soroi.QueryOptions{
		RefetchInterval: 3 * time.Second,
	})
	defer query.Close()

	for state := range query.Updates() {
		if state.Err != nil {
			panic(state.Err)
		}
		if !state.HasData || state.Fetching {
			continue
		}
		fmt.Printf("task %d: %d%%\n", state.Data.ID, state.Data.Progress)
		if state.Data.Progress == 100 {
			query.Close()
			fmt.Println("view closed; polling stopped")
			break
		}
	}
}
```

Run from that module directory:

```sh
go run .
```

Expected output, over approximately six seconds:

```text
task 42: 20%
task 42: 50%
task 42: 100%
view closed; polling stopped
```

The first result loads immediately. The later reads happen at three-second intervals even though `StaleTime` is one minute. Closing `query` stops the view's polling before the Client is closed. The sample exits on a loading error; library polling itself continues after failed attempts while an enabled polling subscriber remains.

## Configuration

| Setting | Purpose | Default and constraints |
| --- | --- | --- |
| `QueryOptions.RefetchInterval` | Request a fixed polling interval for this subscription | `0`: disabled; negative durations also disable it; copied when constructing a handle |
| `QueryOptions.Disable` / `SetEnabled` | Control this consumer's automatic loading, including polling | Enabled by default; disabled subscriptions still receive shared updates |
| `QueryOptions.StaleTime` | Determine freshness for ordinary cache reads | Inherits Client policy; does not suppress interval refreshes |
| Retry, timeout, and Executor settings | Bound each interval-triggered load | Same policy and errors as existing query loads |

`Fetch` and `Prefetch` ignore `RefetchInterval` and create no polling subscription. Existing handles do not change when the original options or definition are modified; construct a new handle to use another interval. `Options.Clock` remains the freshness/retention clock; intervals use elapsed time independently.

## Error handling and common problems

| Symptom | Cause to check | Action |
| --- | --- | --- |
| Old data remains after an error | A refresh failed and retained data is still available | Inspect `Snapshot.Err` or `Updates()`; later intervals continue using the same retry policy |
| Refreshes happen less often than requested | A previous same-key load, including retries, is still active | Occupied intervals are skipped without queuing a follow-up |
| Backend calls happen more often than this view requested | Another enabled subscriber uses a shorter interval | Inspect other subscriptions for the same key |
| Closing a view leaves requests active | Another polling subscriber remains, or a shared load already started | Close every owned handle; use Client cancellation APIs when canceling shared work is intended |
| A refresh returns `ErrQueueFull` | The selected Executor is overloaded | Handle the error through query state; a later interval may recover |

## Behavior and limitations

- One Client owns at most one polling timer per key. The shortest positive interval among enabled subscriptions wins. For equal intervals, the oldest subscription supplies the loader, retries, timeout, and execution budget.
- Changing the selected subscription restarts a full interval. Adding or removing unrelated subscribers preserves the existing schedule.
- An interval executes the existing load path regardless of local freshness. An attached shared result cache can still supply a fresh remote result without calling the source.
- When a same-key load is active, the interval does not start or join another load, invalidate its result, or accumulate queued refreshes. The next interval checks again.
- Closing or disabling the last polling subscriber stops future scheduling. It does not cancel an already-started shared load. `Client.Close` stops all interval timers and cancels Client-owned loads.
- `Cancel` stops current work; `Remove` and `Clear` remove cached data. Live polling subscriptions remain registered and may reload at a later interval. Disable or close them to stop future polling.
- Polling loads emit `TriggerPoll` (`poll`) with `ReasonRefetch` diagnostic events. A skipped interval emits no load event and does not count as a merged request.
- Applications own backend consistency and must honor loader contexts. A timer interval is a refresh cadence, not a real-time delivery guarantee.

## Complete examples and related guides

- [Queries and conditions](queries.en-US.md): snapshots, updates, enablement, and explicit reads.
- [Query options](query-options.en-US.md): reusable typed definitions, retry policies, and execution budgets.
- [Bubble Tea example](bubbletea.en-US.md): connect subscription updates to a UI message loop.
- [Diagnostic events](events.en-US.md): inspect load causes, retries, and outcomes.
