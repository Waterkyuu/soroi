# 业务场景基准

[English](business-benchmarks.en-US.md)

衡量查询共享、有界回源和批量加载如何影响实际回源工作量、调用方延迟和成功响应。
基准以固定请求总量运行热点过期、Redis 传输故障、下游截止时间和批量读取场景，
展示策略取舍，不以缓存查找吞吐量替代业务结果。

## 安装

要求 Go 1.26+、PATH 中的 `redis-server` 和当前仓库。核心 soroi 仍然零依赖并支持 Go 1.22。
基准的 Redis 依赖保留在独立模块中，不使用已有 Redis 服务或凭据。

从仓库根目录执行：

```sh
cd benchmarks/scenarios
go mod download
go build -o /tmp/soroi-business-benchmarks .
GOMAXPROCS=4 /tmp/soroi-business-benchmarks -requests 1000 -workers 32 -max-concurrent 8
```

Windows 环境应选择适用的可执行文件输出路径，并按 shell 语法设置 `GOMAXPROCS`。
Redis fixture 需要本地 `redis-server` 可执行文件。先构建后计时，避免包含编译成本。

## 使用方式

### 比较相同的调用方总体

每次运行输出八个 JSON 对象：四种场景各有 baseline 和 soroi。
`configuration` 记录请求数、worker、延迟和容量；运行时信息记录工具链、平台和 GOMAXPROCS。
标准错误包含 go-redis 连接诊断；标准输出只包含 JSON。

| 场景 | Baseline | soroi 方案 |
| --- | --- | --- |
| `hot-expiry` | 每个调用方直接执行数据源 | 预装一个 key，将注入的新鲜度时钟推进到过期边界一次，再以一分钟新鲜期 Fetch 同一 key |
| `redis-outage` | 不限并发的 soroi 不同 key 加载，Redis 失败后回源 | 相同路径，额外共享 MaxConcurrent 8、MaxQueued 0 的 Executor |
| `downstream-timeout` | 不同 key 的 soroi 加载，每个调用方期限为 1 ms | 相同期限，额外使用 MaxConcurrent 8、MaxQueued 0 的 Executor |
| `batching` | 每个不同 key 执行独立回调 | Batcher.Load 收集并发的不同 key，收集窗口为 1 ms，每批最多 64 个 key |

数据源模拟一次可取消的 2 ms 操作，单条请求和整个批次均使用这一服务时间。
它模拟具有高效批量 API 的后端，不测量真实数据库，也不承诺生产批次延迟与大小无关。
fixture 在预留的 loopback 端口启动临时 Redis，确认就绪后结束自己创建的进程，保留客户端
以产生真实传输错误。每种故障方案使用新 fixture 和连接池。Redis 命令与拨号均设置为一次尝试
（`MaxRetries: -1`、`DialerRetries: 1`），单次存储访问限制为 20 ms。
此场景模拟本地 Redis 停止，不模拟丢包、慢服务或故障切换。

### 解释延迟与资源字段

`all_p95_ms`、`all_p99_ms` 按 nearest-rank 计算全部调用的延迟，包括拒绝与超时。
`success_p95_ms`、`success_p99_ms` 仅计算成功调用；没有成功请求时均为 null。
`success_rate` 为成功调用数除以全部尝试数。每条记录中的成功、拒绝、超时和其他失败数之和等于请求总量。

固定 worker 串行发出请求，因此这是闭环负载，不是开环到达率测试或生产 SLO 测试。
单次延迟从 worker 发起调用开始，不包含等待 worker 的时间。
`elapsed_ms` 包括 worker 启动和数据源清理。进入下一场景前等待已准入的回调结束。

`source_calls` 统计实际回调进入次数，包括失败工作；`peak_source_calls` 统计同时执行的回调。
调用方可能在回调开始前超时，因此超时数不必等于回源次数。准备阶段不计入。
`allocated_bytes` 为 worker 运行和清理期间的进程分配增量；`heap_end_bytes` 是结束时进程堆占用，
不等于 RSS 或单查询内存。`gc_count` 是区间内的 GC 次数。资源统计包括基准和驱动开销。
各 baseline 使用相同的模拟数据源与调用方数量。

## 完整示例

在 `benchmarks/scenarios` 中执行三次并保留完整输出：

```sh
for run in 1 2 3; do
    GOMAXPROCS=4 /tmp/soroi-business-benchmarks -requests 1000 -workers 32 -max-concurrent 8 > "/tmp/soroi-business-$run.jsonl"
done
```

每个文件包含八条记录。默认负载下，热点共享和批量方案应以更少的回源调用完成全部读取。
故障保护方案拒绝超额工作，将回源峰值限制为八。
超时场景的数据源服务时间故意超过期限，通常没有成功响应；具体时间和拒绝数量会变化。

完整实现位于 `benchmarks/scenarios/main.go`、`scenarios.go` 和 `redis.go`。
测试不依赖 Redis 服务，覆盖分位数边界、互斥的结果分类和热点复用。

## 实测记录

测量日期为 2026-10-27，环境为 Apple M3 Pro、Darwin arm64、Go 1.27.1、
GOMAXPROCS=4、Redis 8.10.0，使用前述默认负载。
表格对每个字段分别取三次运行的中位数。
[原始 JSONL 记录](../benchmarks/business-darwin-arm64.jsonl) 保留全部 24 条场景记录、源码版本、
命令、环境和整进程资源统计。CPU 汇总覆盖全部八种场景及自身 Redis 子进程；
`cumulative_children_peak_rss_bytes` 是跨已结束子进程的累计高水位，单位为 Darwin 字节，
不代表独立场景的 RSS。

| Scenario | Variant | Source calls | Peak source calls | Success % | Rejected | Timeouts | All P95 ms | All P99 ms | Success P99 ms | Allocated KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| hot-expiry | baseline | 1000 | 32 | 100.0 | 0 | 0 | 2.339 | 2.377 | 2.377 | 276.1 |
| hot-expiry | soroi | 1 | 1 | 100.0 | 0 | 0 | 0.197 | 2.668 | 2.668 | 56.9 |
| redis-outage | baseline | 1000 | 32 | 100.0 | 0 | 0 | 2.617 | 3.125 | 3.125 | 4085.0 |
| redis-outage | soroi | 123 | 8 | 12.3 | 877 | 0 | 2.371 | 2.506 | 2.637 | 1636.2 |
| downstream-timeout | baseline | 1000 | 32 | 0.0 | 0 | 1000 | 1.272 | 1.305 | n/a | 2009.3 |
| downstream-timeout | soroi | 119 | 8 | 0.0 | 871 | 129 | 1.038 | 1.177 | n/a | 1677.2 |
| batching | baseline | 1000 | 32 | 100.0 | 0 | 0 | 2.361 | 2.405 | 2.405 | 247.6 |
| batching | soroi | 32 | 1 | 100.0 | 0 | 0 | 3.643 | 4.085 | 4.085 | 540.5 |

热点共享减少了回源次数，但等待首次重新加载的调用方仍需承担其延迟。
在本负载下，批量收集窗口增加了调用延迟，同时减少实际后端调用。
Redis 故障时，保护方案通过拒绝超额请求将回源并发限制为八；这是负载保护，不是成功响应增加。
任何超时方案都无法让服务时间超过期限的数据源恢复成功。

## 配置说明

| 参数 | 默认值 | 约束 |
| --- | --- | --- |
| `-requests` | 1000 | 每种方案的请求总量，必须为正 |
| `-workers` | 32 | 闭环并发，必须为正 |
| `-source-delay` | 2ms | 单条或批量回源服务时间，不得为负 |
| `-timeout` | 1ms | 超时场景的调用方期限，必须为正 |
| `-max-concurrent` | 8 | 故障保护的回源并发，必须为正；不排队 |
| `-batch-size` | 64 | 每批不同 key 上限，必须为正 |
| `-batch-wait` | 1ms | 收集窗口，不得为负 |

## 错误处理与常见问题

| 现象 | 可能原因 | 处理 |
| --- | --- | --- |
| 无法启动 Redis | 缺少程序或预留端口被占用 | 安装 Redis 或检查本地监听后重试 |
| stderr 出现 Redis 诊断 | fixture 停止后的预期重连失败 | 单独保存 stdout，不将日志混入 JSONL |
| 保护场景成功率较低 | 零队列按设计拒绝过载 | 将成功数、拒绝数与延迟一起分析 |
| 成功分位数为空 | 没有请求成功 | null 表示没有样本，不是零延迟成功 |

## 行为与限制

这是单机、单进程的合成负载，不证明集群准入、数据源正确性、生产容量、故障切换行为或
稳定的尾延迟改善。宣传性能前，应在部署硬件上使用真实 key 分布、批量成本、数据源延迟与
故障模式重复测量。默认场景顺序固定；分析重复样本时需考虑进程预热、分配历史和调度噪声。

## 完整示例与相关文档

- [HTTP 与 gRPC 接入](service-integrations.zh-CN.md)：请求追踪和 Prometheus 指标。
- [共享缓存故障保护](shared-cache.zh-CN.md#缓存故障期间保护数据源)：现有 API 组合与限制。
- [自动批量加载](batching.zh-CN.md)：逐 key 结果与回调容量。
