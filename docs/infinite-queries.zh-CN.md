# 分页追加查询

[English](infinite-queries.en-US.md)

`InfiniteQuery` 用于 CLI 或 TUI 的任务列表，在加载更多任务时保留已有页。一个订阅提供累积的分页数据、是否还有下一页，以及加载和错误状态。

## 安装

要求 Go 1.22 或更高版本，不需要外部服务。使用包含该 API 的仓库检出版本运行示例时，在单独的示例目录执行以下命令，将 `/absolute/path/to/soroi` 替换为仓库根目录的绝对路径：

```sh
go mod init example.com/tasks
go mod edit -replace=github.com/Waterkyuu/soroi=/absolute/path/to/soroi
go get github.com/Waterkyuu/soroi
```

## 使用方式

`InfiniteQuery(client, key, initialPageParam, fetchPage, getNextPageParam, options...)` 创建订阅，默认自动加载第一页。Go 从参数推断页类型和分页参数类型。`fetchPage` 接收上下文及当前分页参数，返回一份后端响应或错误。`getNextPageParam` 返回下一次请求的参数，以及是否还有下一页；布尔值为 true 时，零值也可以是有效分页参数。

将每页条数、筛选条件和身份范围放入可比较的查询 key，并在加载函数中使用。变化的游标由分页序列内部管理。复用应用拥有的 `Client`，使用方离开时关闭对应 Handle。

### 完整示例

将下面的程序保存为示例模块中的 `main.go`。内存加载函数每页最多返回两个任务标题。`Disable: true` 让首次加载显式执行，便于顺序演示；后续调用追加下一页。

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

在示例目录运行 `go run .`，预期输出：

```text
pages: 1 more: true
pages: 2 more: false
[Write docs Fix tests]
[Publish release]
```

页数从一页增加到两页，两页数据都保留，最终的继续加载标记为 false。

### 观察列表

界面使用同一个 `tasks` Handle 的 `Updates()` 接收状态变化。下面的片段复用完整示例的 `fmt` 导入：

```go
for state := range tasks.Updates() {
    fmt.Println(len(state.Data.Pages), state.FetchingNextPage, state.HasNextPage, state.Err)
}
```

`InfiniteSnapshot` 嵌入 `Snapshot[InfiniteData[Page, Param]]`：`Data.Pages` 按顺序保存响应，`Data.PageParams` 保存一一对应的请求参数。`HasData`、`Status`、`Err`、`Fetching`、`Stale`、`UpdatedAt` 和 `ExpiresAt` 沿用普通查询的含义。数据尚不可用时，`HasNextPage` 为 false；`FetchingNextPage` 区分追加与首次加载、刷新。慢速接收方可能跳过中间状态。

### 加载更多与刷新

| 操作 | 结果 |
| --- | --- |
| `tasks.FetchNextPage(ctx)` | 等待追加一页；尚无数据时加载第一页，返回累积的 `InfiniteData` 和错误 |
| `tasks.Refetch(ctx)` | 从初始参数重新加载当前序列，根据新响应计算后续游标 |
| `tasks.Snapshot()` | 读取当前数据和状态，不发起加载 |
| `tasks.SetEnabled(enabled)` | 控制该使用方的自动加载和轮询 |
| `tasks.Close()` | 释放当前订阅，不取消共享工作 |

阻塞操作应放在 TUI 的异步命令中执行，消息循环接收更新。没有下一页时，`FetchNextPage` 返回当前数据和 nil，不调用后端。操作失败时，已有页仍可通过 `Snapshot()` 和 `Updates()` 读取；检查 `Err`，在适当时机重试对应操作。

## 配置说明

可传入零个或一个现有 `QueryOptions`，省略时继承 Client 策略。各配置项见[查询配置](query-options.zh-CN.md)。

| 配置 | 分页行为 |
| --- | --- |
| `Disable`、`SetEnabled` | 控制自动首次加载、过期刷新和轮询；显式调用仍会执行 |
| `RefetchInterval` | 使用现有共享定时器刷新已加载序列；已有请求时跳过该次轮询 |
| `Retry`、`RetryIf`、`RetryDelay` | 仅重试失败的那一页，不重复加载本次操作中已成功的前页 |
| `Timeout`、`Executor` | 超时覆盖整个操作，包括排队和重试等待；每个取页回调分别获取执行容量 |

## 错误处理与常见问题

| 现象 | 处理方式 |
| --- | --- |
| `ErrQueryBusy` | 追加与首次加载、刷新冲突，或刷新与追加冲突。等待当前操作结束后重试；冲突操作不排队。 |
| 页请求失败 | 保留并继续展示已有页，读取 `Snapshot().Err`，重试失败的追加或刷新。 |
| 上下文取消或超时 | 处理返回的上下文错误。取消沿用现有共享请求的所有权规则，不丢弃已有页。 |
| key 无效、缓存类型不兼容、加载函数为 nil 或配置传入过多 | 检查失败 Handle 的快照或初始更新；不会发起加载，沿用已有错误值。 |
| `getNextPageParam` 为 nil | 必须提供继续加载函数；为空时在注册订阅前 panic。 |

## 行为与限制

- 同 key 的 Handle 共享一份缓存序列和一个进行中的请求；并发追加合并，并发刷新合并。该 key 的页类型、参数类型和分页语义必须一致。
- 刷新从初始参数开始，最多重取当前页数，每次根据新响应计算下一页游标。提前结束时缩短序列；完整替换成功前继续保留旧页。
- `getNextPageParam` 在加载、快照读取和更新投影时于 Client 锁外执行，必须快速返回且无副作用。并发调用涉及的外部可变状态需要同步。`Close` 后，已捕获的投影结束才关闭 `Updates()`。
- 整个序列是一份缓存值，占用一个 LRU 条目。新鲜期和最大数据年龄针对整份结果，而非单独一页；追加成功安装新的聚合结果，更新普通新鲜期和年龄基准。保留多少页由应用控制。
- `Stats().Loads` 统计共享操作数，而非页数；诊断事件的 `Attempts` 统计实际取页回调次数，`Stats().Retries` 只统计失败页的重试。
- 现有 `Get`、`Set`、`Update`、`Invalidate`、`InvalidateAndWait`、`Cancel` 和 `Remove` 操作同一个 key。缓存页必须只读；修改时复制相关切片，保持 `Pages` 与 `PageParams` 等长且顺序对应。
- 该 API 向前追加页，不根据空页推断结束；是否继续由回调返回的布尔值决定。

## 完整示例与相关文档

- [查询](queries.zh-CN.md)：普通订阅、启用控制和共享取消规则。
- [失效与刷新](invalidation.zh-CN.md)：等待刷新及有界补刷。
- [原子缓存更新](updates.zh-CN.md)：修改缓存值时避免覆盖并发变化。
