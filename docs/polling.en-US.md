# Subscription polling

[简体中文](polling.zh-CN.md)

`QueryOptions.RefetchInterval` automatically refreshes enabled subscriptions at a fixed interval. Use it when subscribed data changes independently and the application needs periodic updates.

For example, a TUI can subscribe to task 42, refresh its progress every three seconds, receive 20% → 50% through `Updates()`, and close the subscription when leaving the view.

## Usage

Polling is part of the core package and requires Go 1.22 or later, with no additional dependency. Use a checkout containing `RefetchInterval`. The snippets below extend the [query guide](queries.en-US.md): `client` is its application-owned Client, and `getUsers` is its `func(context.Context) ([]User, error)` loader. Keep the existing imports and add `time` if needed.

### Enable polling

Pass a positive interval when constructing a subscription:

```go
users := soroi.Query(
	client,
	"users",
	getUsers,
	soroi.QueryOptions{RefetchInterval: 3 * time.Second},
)
defer users.Close()
```

`Query` infers `[]User` from the loader and returns `*QueryHandle[[]User]`. Missing or stale data loads immediately; fresh data is initially reused. Every subsequent interval can refresh even fresh local data. Construction and loading errors are reported through `Snapshot().Err` and `Updates()`.

The loader performs the application's I/O. Polling reuses its existing retries, timeout, and Executor policy. Configure `Definition.Options.RefetchInterval` when using `Definition.Query`.

### Receive results

Use the existing `users.Snapshot()` and `users.Updates()` APIs. Refreshing publishes `Fetching: true` while retaining available data, followed by the result or error. Reading state does not create another polling schedule. See [query updates](queries.en-US.md) for channel handling.

### Pause, resume, and close

`SetEnabled(false)` pauses this subscriber's automatic loading; `SetEnabled(true)` resumes it. These calls return lifecycle errors, so handle them in a function returning `error`:

```go
if err := users.SetEnabled(false); err != nil {
	return err
}
if err := users.SetEnabled(true); err != nil {
	return err
}
```

Call `users.Close()` when its owner leaves. In a UI, call it from the view's shutdown path; a `defer` after a blocking update loop cannot end that loop. Disabling or closing a handle does not cancel an already-started shared request. `Client.Close` stops all schedules and cancels Client-owned loads.

## Configuration

| Setting | Purpose | Default | Constraints |
| --- | --- | --- | --- |
| `RefetchInterval` | Schedule subscription refreshes | `0`: disabled | `time.Duration`; negative values also disable polling; copied at handle construction |
| `Disable` / `SetEnabled` | Control automatic loading | Enabled | Disabled handles still receive updates from other consumers |
| `StaleTime` | Determine ordinary cache freshness | Inherit Client policy | Does not suppress interval refreshes |

With the Client freshness policy `StaleTime` set to `time.Minute`, ordinary reads can reuse data for one minute. A subscription with `RefetchInterval` set to `3 * time.Second` still schedules a refresh every three seconds. `Fetch` and `Prefetch` ignore the interval and create no polling subscription. To change an existing handle's interval, create a replacement handle.

## Error handling and common problems

| Symptom | Cause to check | Action |
| --- | --- | --- |
| Refresh failed but old data remains | The load exhausted its retry policy | Inspect `Snapshot.Err` or updates; subsequent intervals continue |
| Fewer refreshes than expected | A same-key load is still running, including queuing and retries | Occupied intervals are skipped without accumulating follow-up work |
| More frequent refreshes than requested | Another enabled same-key subscriber has a shorter interval | Check the other subscribers' configuration |
| Requests continue after closing a handle | Another polling subscriber remains, or a shared load already started | Release all owned handles; cancel shared work explicitly when appropriate |

## Behavior and limitations

- Same-key subscriptions share the shortest positive interval among enabled handles. The oldest handle with that interval supplies the loader and loading policy. Changing that selected handle restarts a full interval.
- Interval loads reuse the existing request and cache machinery. An attached shared cache can satisfy a load without accessing the source.
- `Cancel`, `Remove`, and `Clear` do not release subscriptions; a later interval can load again. Disable or close the subscriber to stop future polling.
- Polling does not change local data retention or `MaxAge`. `Options.Clock` controls freshness and retention; intervals use elapsed time independently.
- Polling loads emit `TriggerPoll` with `ReasonRefetch`. Skipped intervals do not create loads or count as merged requests.

## Related guides

- [Queries and conditions](queries.en-US.md): runnable setup, loaders, snapshots, and updates.
- [Query options](query-options.en-US.md): reusable definitions and per-query loading policies.
- [Diagnostic events](events.en-US.md): load causes, retries, and outcomes.
