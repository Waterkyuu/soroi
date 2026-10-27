# 订阅轮询

[English](polling.en-US.md)

`QueryOptions.RefetchInterval` 按固定间隔自动刷新已启用的订阅，适用于数据会独立变化、应用需要定期获取更新的场景。

例如，TUI 订阅任务 42 后，每三秒读取一次进度，通过 `Updates()` 接收 20% → 50% 的变化，离开界面时关闭订阅。

## 使用方式

轮询属于核心包，需要 Go 1.22 或更新版本，无需额外依赖。使用包含 `RefetchInterval` 的源码版本。以下片段延续[查询文档](queries.zh-CN.md)中的程序：`client` 是应用持有的 Client，`getUsers` 是其中的 `func(context.Context) ([]User, error)` 加载函数。沿用已有导入，缺少 `time` 时补充导入。

### 启用轮询

创建订阅时传入正数间隔：

```go
users := soroi.Query(
	client,
	"users",
	getUsers,
	soroi.QueryOptions{RefetchInterval: 3 * time.Second},
)
defer users.Close()
```

`Query` 从加载函数推断结果类型 `[]User`，返回 `*QueryHandle[[]User]`。数据缺失或陈旧时立即加载；已有新鲜数据时先复用缓存。后续间隔即使遇到新鲜的本地数据也会刷新。构造与加载错误通过 `Snapshot().Err` 和 `Updates()` 报告。

应用的 I/O 由加载函数执行；轮询沿用其已有的重试、超时和 Executor 策略。使用 `Definition.Query` 时，在 `Definition.Options.RefetchInterval` 中配置间隔。

### 接收结果

使用已有的 `users.Snapshot()` 和 `users.Updates()`。刷新时发布 `Fetching: true`，同时保留仍可用的数据，完成后发布结果或错误。读取状态不会创建额外的轮询调度。通道处理方式见[查询更新](queries.zh-CN.md)。

### 暂停、恢复与关闭

`SetEnabled(false)` 暂停此订阅的自动加载，`SetEnabled(true)` 恢复。两者可能返回生命周期错误，应在返回 `error` 的函数中处理：

```go
if err := users.SetEnabled(false); err != nil {
	return err
}
if err := users.SetEnabled(true); err != nil {
	return err
}
```

所属消费者退出时调用 `users.Close()`。UI 应从界面退出路径关闭句柄；阻塞更新循环后的 `defer` 不能结束该循环。禁用或关闭句柄不会取消已经启动的共享请求。`Client.Close` 停止全部调度，并取消 Client 持有的加载。

## 配置说明

| 配置 | 用途 | 默认值 | 约束 |
| --- | --- | --- | --- |
| `RefetchInterval` | 调度订阅刷新 | `0`：禁用 | 类型为 `time.Duration`；负数同样禁用；创建句柄时复制 |
| `Disable` / `SetEnabled` | 控制自动加载 | 启用 | 禁用的句柄仍接收其他消费者产生的更新 |
| `StaleTime` | 判断普通缓存的新鲜期 | 继承 Client 策略 | 不会阻止间隔刷新 |

Client 的新鲜期 `StaleTime` 设为 `time.Minute` 时，普通读取可以在一分钟内复用数据；订阅的 `RefetchInterval` 设为 `3 * time.Second` 时，仍然每三秒调度一次刷新。`Fetch`、`Prefetch` 忽略此间隔，不创建轮询订阅。修改已有句柄的间隔需要创建替代句柄。

## 错误处理与常见问题

| 现象 | 需要检查的原因 | 处理方式 |
| --- | --- | --- |
| 刷新失败但保留旧数据 | 加载已耗尽重试策略 | 检查 `Snapshot.Err` 或更新状态；后续间隔仍继续调度 |
| 刷新频率低于预期 | 同 key 的加载仍在执行，包括排队与重试 | 跳过被占用的间隔，不积压补刷工作 |
| 刷新比指定间隔更频繁 | 另一个已启用的同 key 订阅使用更短间隔 | 检查其他订阅的配置 |
| 关闭句柄后仍有请求 | 还有其他轮询订阅，或共享加载已经启动 | 释放所有所属句柄；确有需要时显式取消共享工作 |

## 行为与限制

- 同 key 的已启用订阅共享最短正间隔，由采用该间隔且最早创建的句柄提供加载函数和加载策略。所选句柄改变时，重新开始完整间隔。
- 间隔加载复用已有的请求与缓存机制。已配置的共享缓存可以直接提供结果，无需访问数据源。
- `Cancel`、`Remove`、`Clear` 不会释放订阅，后续间隔仍可重新加载。停止后续轮询应禁用或关闭订阅。
- 轮询不改变本地数据保留规则与 `MaxAge`。`Options.Clock` 控制新鲜期和保留时间，轮询间隔独立使用实际经过的时间。
- 轮询加载产生 `TriggerPoll` 和 `ReasonRefetch`。被跳过的间隔不会创建加载，也不计为请求合并。

## 相关文档

- [查询与条件请求](queries.zh-CN.md)：可运行的初始化程序、加载函数、快照与更新。
- [查询配置](query-options.zh-CN.md)：可复用定义与逐查询加载策略。
- [诊断事件](events.zh-CN.md)：加载原因、重试和结果。
