# 分页追加查询

[English](infinite-queries.en-US.md)

`InfiniteQuery` 用于任务列表的“加载更多”：每次请求追加一页，保留已经展示的页。返回的 Handle 提供累积数据、加载状态、错误，以及是否还有下一页。

## 使用方式

### 创建分页查询

先定义一次取页请求返回的数据类型：

```go
// TaskPage is one response from the task service.
type TaskPage struct {
	// Items contains this page's task titles.
	Items []string
	// NextCursor is empty when no more pages remain.
	NextCursor string
}
```

以下片段放在导入 `context`、`fmt` 和 `soroi` 的 `main` 函数内。这里创建 Client，并用两个固定响应定义 `fetchPage`；接入业务时，将函数体替换为实际后端请求。

`InfiniteQuery` 返回 `*InfiniteQueryHandle[TaskPage, string]`，两个类型均从参数推断。示例传入 `Disable: true`，创建时只订阅、不加载，再通过 `FetchNextPage` 显式加载第一页。省略该配置时，会在后台加载缺失或过期的数据。

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
	func(last TaskPage) (string, bool) {
		return last.NextCursor, last.NextCursor != ""
	},
	soroi.QueryOptions{Disable: true},
)
defer tasks.Close()
```

各参数的用途如下：

| 参数 | 使用方式 |
| --- | --- |
| `client` | 在使用方之间共享缓存页和进行中的请求；复用应用拥有的 Client |
| `key` | 标识完整列表；筛选条件、每页条数和身份范围影响结果时，应包含在 key 中 |
| `initialPageParam` | 第一次请求的参数；这里用 `""` 表示第一页 |
| `fetchPage` | `func(context.Context, Param) (Page, error)`，使用传入参数获取一页 |
| `getNextPageParam` | `func(Page) (Param, bool)`，从最后一页取下一次参数；false 表示分页结束 |
| `options` | 零个或一个现有 `QueryOptions`，省略的配置继承 Client 策略 |

变化的游标由分页序列管理，不放入列表 key。每页条数在后端请求中配置，并包含在 key 中，不需要单独的分页配置项。继续加载标记为 true 时，零值也可以作为有效分页参数。

### 加载第一页或下一页

`FetchNextPage(ctx)` 等待取页完成，返回 `(InfiniteData[TaskPage, string], error)`。尚无数据时加载第一页；后续调用每次追加一页。下面复用创建步骤中的 `tasks` 和 `ctx`：

```go
data, err := tasks.FetchNextPage(ctx)
if err != nil {
	fmt.Println("Load failed:", err)
	return
}
fmt.Println("pages:", len(data.Pages))
```

`data.Pages` 保存所有已加载的响应，`data.PageParams` 保存一一对应的请求参数。请求失败时保留已有缓存页；没有下一页时，返回当前数据和 nil，不再请求后端。TUI 中应将这类阻塞调用放入异步命令执行。

### 读取当前数据和状态

`Snapshot()` 立即返回 `InfiniteSnapshot[TaskPage, string]`，不会发起或等待请求。加载完成后，可按顺序展示累积页：

```go
state := tasks.Snapshot()
for _, page := range state.Data.Pages {
	fmt.Println(page.Items)
}
fmt.Println("has next:", state.HasNextPage)
```

| 字段 | 使用方式 |
| --- | --- |
| `Data.Pages` | 按顺序展示已加载的页 |
| `Data.PageParams` | 读取每一页对应的请求参数 |
| `HasData` | 区分已有可用数据与尚无结果的初始查询 |
| `HasNextPage` | 控制“加载更多”是否可用；尚无数据或已到最后一页时为 false |
| `Fetching` | 判断是否正在执行首次加载、追加或刷新 |
| `FetchingNextPage` | 单独展示“正在加载下一页”的提示 |
| `Err` | 展示最近一次请求错误，同时继续展示仍可用的数据 |

`InfiniteSnapshot` 嵌入现有 `Snapshot[InfiniteData[Page, Param]]`，`Status`、`Stale`、`UpdatedAt` 和 `ExpiresAt` 沿用普通查询的含义。缓存数据由使用方共享，必须只读。

### 订阅状态更新

`Updates()` 返回 `InfiniteSnapshot` 通道。界面通过该通道消费状态，加载按钮等操作另外调用 `FetchNextPage` 或 `Refetch`：

```go
for state := range tasks.Updates() {
	fmt.Println(state.Data.Pages, state.FetchingNextPage, state.Err)
}
```

通道提供初始和最新状态，慢速接收方可能跳过中间变化。循环会持续等待更新，直到订阅关闭；只需读取一次当前值时使用 `Snapshot()`。

### 刷新已加载的页

`Refetch(ctx)` 等待刷新完成，返回与 `FetchNextPage` 相同的 `InfiniteData` 类型。它从初始参数重新取页，并根据新响应计算后续游标，而不是再追加一页：

```go
if _, err := tasks.Refetch(ctx); err != nil {
	fmt.Println("Refresh failed:", err)
	return
}
```

一次刷新最多重取当前页数；提前到达末页时缩短列表。完整替换成功前继续展示旧页；刷新失败时保留旧列表并报告错误。

### 控制自动加载和释放订阅

`SetEnabled(true)` 启用缺失或过期数据的自动加载，并参与轮询。下面让创建步骤中禁用的 Handle 恢复自动加载：

```go
if err := tasks.SetEnabled(true); err != nil {
	fmt.Println("Enable failed:", err)
	return
}
```

`SetEnabled(false)` 只禁用当前使用方的自动加载，仍然接收共享更新，显式 `FetchNextPage` 和 `Refetch` 也仍会执行。已经开始的请求和其他使用方不受影响。

使用方离开时调用 `tasks.Close()`，通常在创建后立即注册 `defer tasks.Close()`。它释放当前订阅及其轮询参与权，不取消共享请求；已接收的状态处理完后关闭更新通道。应用退出时关闭 Client。

## 完整示例

将程序保存为已引入 soroi 的 Go 模块中的 `main.go`，在该目录运行 `go run .`。程序加载两页，随后展示保留下来的任务标题：

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
		func(last TaskPage) (string, bool) {
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

预期输出：

```text
pages: 1 more: true
pages: 2 more: false
[Write docs]
[Fix tests]
```

## 配置说明

加载策略复用现有 [QueryOptions](query-options.zh-CN.md)：

| 配置 | 效果 |
| --- | --- |
| `Disable`、`SetEnabled` | 控制自动加载；默认允许加载 |
| `RefetchInterval` | 刷新已加载的页；非正值禁用轮询 |
| `Retry`、`RetryIf`、`RetryDelay` | 仅重试失败页，不重复本次操作中已经成功的前页 |
| `Timeout`、`Executor` | 覆盖整个操作，包括等待；每个取页回调分别获取执行容量 |

## 错误处理与常见问题

| 情况 | 处理方式 |
| --- | --- |
| `ErrQueryBusy` | 追加与首次加载、刷新冲突，或刷新与追加冲突；等待当前操作结束后重试。 |
| 页请求失败 | 读取返回错误或 `Snapshot().Err`；继续展示保留的数据，按需重试对应操作。 |
| 上下文取消或超时 | 处理上下文错误；共享请求的取消沿用普通查询的所有权规则。 |
| key 无效、类型不兼容、加载函数为 nil 或配置过多 | 检查 `Snapshot().Err` 或初始更新；沿用已有查询错误，不会发起加载。 |
| `getNextPageParam` 为 nil | 必须提供继续加载回调；为空时在注册订阅前 panic。 |

## 行为与限制

- 同 key 的 Handle 共享页和进行中的请求。并发追加合并，并发刷新也合并；不同类型的操作发生冲突时不排队。同 key 必须使用一致的页类型、参数类型和分页语义。
- 继续加载回调必须快速返回且无副作用，在加载、快照读取和更新投递时于 Client 锁外执行，可能被并发调用。
- 完整序列是一份缓存值，占一个 LRU 条目。新鲜期和最大数据年龄针对整份结果；追加成功会安装新值并更新相应时间。保留多少页由应用控制。
- 现有 `Get`、`Set`、`Update`、`Invalidate`、`InvalidateAndWait`、`Cancel` 和 `Remove` 操作列表 key。手动修改时复制相关数据，保持 `Pages` 和 `PageParams` 等长且顺序对应。
- 分页向前追加，是否结束由继续加载标记决定，与单页数据长度无关。

## 完整示例与相关文档

- [查询](queries.zh-CN.md)：普通订阅与共享取消规则。
- [失效与刷新](invalidation.zh-CN.md)：写入后刷新并等待结果。
- [原子缓存更新](updates.zh-CN.md)：修改缓存页，避免覆盖并发变化。
