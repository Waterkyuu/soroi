# 通过订阅轮询显示任务进度

[English](polling.en-US.md)

TUI 任务界面需要显示任务 42 从 20% 更新到 50%，而后端进度会独立变化。通过 `QueryOptions.RefetchInterval` 每三秒刷新订阅的查询，并通过 `Updates()` 接收结果。关闭界面时释放订阅，停止该订阅对轮询的贡献。

示例展示以下生命周期：

1. 打开界面，订阅 `task:42`；缺失的数据立即加载。
2. 每隔三秒重新加载任务，即使本地数据仍处于新鲜期。
3. 从 `Updates()` 接收已完成的快照，更新显示的进度。
4. 界面关闭时释放订阅；没有其他已启用的轮询订阅时，释放该 key 的定时器。

## 安装

需要 Go 1.22 或更新版本，以及本仓库源码。无需外部服务或 TUI 依赖。完整示例使用内存加载函数模拟后端进度；实际应用可以直接提供已有的 HTTP 或 RPC 加载函数。

从**仓库根目录**创建独立模块，引用包含轮询 API 的本地源码：

```sh
SOROI_SOURCE_DIR="$(pwd)"
SOROI_DEMO_DIR="$(mktemp -d)"
cd "$SOROI_DEMO_DIR"
go mod init example.com/task-progress
go mod edit -replace="github.com/Waterkyuu/soroi=$SOROI_SOURCE_DIR"
go get github.com/Waterkyuu/soroi@v0.0.0
```

本地替换使示例无需依赖已经发布且包含 `RefetchInterval` 的版本。后续操作在此终端的新模块目录中执行。

## 使用方式

以下片段摘自后面的完整示例 `main`。`Task` 是该程序的结果类型，包含任务标识 `ID` 和完成百分比 `Progress`；`loadTask` 是程序中由应用提供的 `func(context.Context) (Task, error)` 加载函数。完整程序包含这些声明和全部导入。

### 打开任务界面

创建由应用持有的 Client，以及由界面持有的查询句柄：

```go
client := soroi.NewClient(soroi.Options{StaleTime: time.Minute})
defer client.Close()
query := soroi.Query(client, "task:42", loadTask, soroi.QueryOptions{
	RefetchInterval: 3 * time.Second,
})
defer query.Close()
```

`Query` 从 `loadTask` 推断结果类型为 `Task`，并在后台加载缺失或陈旧的数据。句柄持有订阅，直到调用 `Close`。`StaleTime` 控制普通缓存复用；`RefetchInterval` 在本地数据仍然新鲜时也会调度刷新。通过 `Definition.Options` 配置后，`Definition.Query` 使用相同规则。

### 接收进度并关闭界面

在界面的更新适配逻辑中读取句柄的更新通道：

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

`Updates()` 提供初始状态和最新快照，慢消费者可能跳过中间状态。刷新时 `Fetching` 为真，同时保留仍可用的数据。此片段打印已完成结果，在进度达到 100% 时关闭界面。Bubble Tea 应用可以通过 `tea.Cmd` 转发快照，并在移除所属界面时关闭句柄。完整示例直接打印结果，因此无需 UI 框架即可运行。

### 暂停与恢复自动加载

使用已有的条件开关暂停此界面的自动加载：

```go
if err := query.SetEnabled(false); err != nil {
	panic(err)
}
if err := query.SetEnabled(true); err != nil {
	panic(err)
}
```

禁用后，该订阅不再参与轮询间隔选择。启用时先复用新鲜数据，或加载缺失、陈旧的数据；若此订阅提供共享调度配置，则重新开始完整的轮询间隔。禁用期间仍可显式调用 `Refetch`。

## 完整示例

将以下程序保存为上面创建的模块中的 `main.go`。加载函数模拟后端依次返回 20%、50% 和 100%；生产加载函数应读取实际任务状态。

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

从该模块目录运行：

```sh
go run .
```

约六秒内的预期输出：

```text
task 42: 20%
task 42: 50%
task 42: 100%
view closed; polling stopped
```

首次结果立即加载。即使 `StaleTime` 为一分钟，后续读取仍按三秒间隔执行。`query` 关闭后，先停止界面的轮询，再关闭 Client。示例遇到加载错误时退出；库本身在失败后仍会继续轮询，直到没有已启用的轮询订阅。

## 配置说明

| 配置 | 用途 | 默认值与约束 |
| --- | --- | --- |
| `QueryOptions.RefetchInterval` | 为此订阅指定固定轮询间隔 | `0` 表示禁用；负数同样禁用；创建句柄时复制 |
| `QueryOptions.Disable` / `SetEnabled` | 控制此消费者的自动加载，包括轮询 | 默认启用；禁用的订阅仍接收共享更新 |
| `QueryOptions.StaleTime` | 判断普通缓存读取的新鲜期 | 继承 Client 策略；不会阻止间隔刷新 |
| 重试、超时与 Executor 配置 | 限制每次间隔触发的加载 | 沿用现有查询加载的策略与错误契约 |

`Fetch`、`Prefetch` 忽略 `RefetchInterval`，不会创建轮询订阅。修改原始配置或定义不会改变已有句柄；更换间隔需要创建新句柄。`Options.Clock` 仍用于新鲜期与保留时间，轮询间隔独立使用实际经过的时间。

## 错误处理与常见问题

| 现象 | 需要检查的原因 | 处理方式 |
| --- | --- | --- |
| 报错后仍保留旧数据 | 刷新失败，旧数据仍然可用 | 检查 `Snapshot.Err` 或 `Updates()`；后续间隔继续使用相同重试策略 |
| 刷新频率低于指定值 | 同 key 的上一次加载仍在执行，包括重试 | 跳过被占用的间隔，不排队补刷 |
| 后端请求比当前界面的配置更频繁 | 另一个已启用订阅使用更短间隔 | 检查相同 key 的其他订阅 |
| 关闭界面后仍有请求 | 仍有其他轮询订阅，或共享加载已经启动 | 关闭所有所属句柄；确需取消共享工作时使用 Client 取消 API |
| 刷新返回 `ErrQueueFull` | 选定的 Executor 已过载 | 通过查询状态处理错误，后续间隔可以恢复 |

## 行为与限制

- 每个 Client 对每个 key 最多持有一个轮询定时器。已启用订阅中最短的正间隔生效；间隔相同时，由最早创建的订阅提供加载函数、重试、超时和执行预算。
- 调度所选订阅改变时，重新开始完整间隔。新增或移除无关订阅不会重置已有调度。
- 间隔触发时忽略本地新鲜期，执行已有加载流程。配置共享结果缓存后，仍可复用新鲜的远端结果而不调用数据源。
- 同 key 已有加载时，该间隔不会启动或加入另一加载，不会使当前结果失效，也不会积压补刷任务；下一个间隔再次检查。
- 关闭或禁用最后一个轮询订阅会停止后续调度，但不会取消已经启动的共享加载。`Client.Close` 停止全部轮询定时器，并取消 Client 持有的加载。
- `Cancel` 停止当前工作；`Remove`、`Clear` 移除缓存数据。仍然存活的轮询订阅可以在后续间隔重新加载；停止后续轮询应禁用或关闭订阅。
- 轮询加载产生 `TriggerPoll`（`poll`）和 `ReasonRefetch` 诊断事件。被跳过的间隔不产生加载事件，也不计为请求合并。
- 后端一致性由应用负责，加载函数必须响应 context。定时间隔表示刷新频率，不保证实时交付。

## 完整示例与相关文档

- [查询与条件请求](queries.zh-CN.md)：快照、更新、条件开关和显式读取。
- [查询配置](query-options.zh-CN.md)：可复用的类型化定义、重试策略与执行预算。
- [Bubble Tea 示例](bubbletea.zh-CN.md)：将订阅更新接入界面消息循环。
- [诊断事件](events.zh-CN.md)：检查加载原因、重试和结果。
