# nns-endpoint

> NNS 端点工厂——per-service 熔断（4xx 不计失败）、HTTP 换实例重试、gRPC 连接池、弹性指标

> 项目主页：**https://oekit.github.io/** · 名称由来：**O**pen **E**asy **Kit**（开源 · 易用 · 工具集合）
> 🤖 AI Agent：本仓规范与硬规则见 [AGENTS.md](AGENTS.md)；机器可读索引见 [llms.txt](https://oekit.github.io/llms.txt)（接口签名以源码为准）。

本仓库是 [ekit](https://git.enjoye.top/enjoydream/ekit) 拆分矩阵的成员（Round-4 微库化）。

## 安装

```bash
go get github.com/oekit/nns-endpoint@v0.1.2
```

## 能做什么

- **HTTP 端点**：服务发现 + per-service 熔断 + 失败**换实例**重试，调用即函数
- **gRPC 端点**：连接池 + 同款熔断语义
- **熔断口径**：4xx 不计失败（调用方错误与对端健康无关），5xx/网络错误计失败
- **弹性指标**：WithMetrics 一行接入
- **ErrCircuitOpen** 可判定（与 httpx 同一 sentinel）

## 使用示例

### HTTP 端点：调用即函数
```go
import endpoint "github.com/oekit/nns-endpoint"

f := endpoint.NewHTTPEndpointFactory(nnsc.ServiceClient(),
    endpoint.WithMetrics(metrics.Registry, "nns"))
ep, _ := f.MakeEndpoint("payment-svc")
resp, err := ep(ctx, endpoint.HTTPRequest{ /* Method/Path/Body */ })
```
### gRPC 端点（连接池）
```go
gf := endpoint.NewGRPCEndpointFactory(nnsc.ServiceClient())
gep, _ := gf.MakeEndpoint("user-rpc")
resp, err := gep(ctx, endpoint.GRPCRequest{ /* ... */ })

> ⚠️ **熔断口径**：4xx 不计熔断失败；`ResetBalancer(serviceName)` 可手动重置负载均衡状态。
## 包清单

- 根包 `endpoint` — HTTPEndpointFactory/GRPCEndpointFactory，经 `nns.ServiceClient()` 构造（Round-4 自 ekit-nns 分家）

依赖：[ekit-nns](https://github.com/oekit/nns)（ServiceClient）、[ekit-httpx](https://github.com/oekit/httpx)、[ekit-metrics](https://github.com/oekit/metrics)、[ekit-logx](https://github.com/oekit/logx)、[ekit-registry](https://github.com/oekit/registry)、[ekit-concurrency](https://github.com/oekit/concurrency)。

原 `nns.MakeHTTPEndpoint/MakeGRPCEndpoint` 门面 API 随本仓迁移：改用 `endpoint.NewHTTPEndpointFactory(nnsClient.ServiceClient())`。

## License

见 [LICENSE](LICENSE)

## 矩阵总览（23 活跃模块，自动生成）

| 模块 | 内容 | 矩阵内依赖 |
|---|---|---|
| [pkg](https://github.com/oekit/pkg) | 微工具集 16 合 1（errors/validator/stringx/slicex/enumx/convert/encoding/jsonrepair/httpjson/mathx/pathx/pagination/ptrx/timex/uidgen/cache） | — |
| [concurrency](https://github.com/oekit/concurrency) | 并发与流控原语（async/group/singleflight/progress/syncx/ratelimit/retry） | — |
| [logx](https://github.com/oekit/logx) | 统一日志门面（多后端可替换 + Sink + caller 归属）+ 优雅关停 | — |
| [core](https://github.com/oekit/core) | 应用基建件（governance 治理 schema / lifecycle 契约 / network IP 工具 / version） | — |
| [istio](https://github.com/oekit/istio) | Istio Proxyless——Kitex xDS 动态熔断 | — |
| [metrics](https://github.com/oekit/metrics) | Prometheus 门面 + 弹性指标族 | [core](https://github.com/oekit/core) |
| [tracing](https://github.com/oekit/tracing) | OpenTelemetry 追踪 + trace_id 真相源 | [logx](https://github.com/oekit/logx) |
| [registry](https://github.com/oekit/registry) | 注册发现核心 + Nacos 实现 + 配置加载 + sdbridge | [core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx) |
| [data](https://github.com/oekit/data) | 缓存检查器（TTL/failTTL/inflight 合并）+ LocalCache | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx) |
| [gorm](https://github.com/oekit/gorm) | SQL/GORM 全家——连接池增强、框架无关 CRUD Service、仓储 | [logx](https://github.com/oekit/logx)、[pkg](https://github.com/oekit/pkg) |
| [redis](https://github.com/oekit/redis) | Redis 全家——客户端封装、缓存后端、分布式锁 | [concurrency](https://github.com/oekit/concurrency)、[data](https://github.com/oekit/data)、[logx](https://github.com/oekit/logx) |
| [httpx](https://github.com/oekit/httpx) | HTTP 客户端 + 负载均衡——服务发现/熔断/重试/指标/OTel 透传 | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
| [network](https://github.com/oekit/network) | 轻量网络客户端——WebSocket/NATS/MQ 抽象、连接与流工具、Octopus 推送 | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx)、[tracing](https://github.com/oekit/tracing) |
| [timertask](https://github.com/oekit/timertask) | 分片定时任务（FNV-1a、心跳续约） | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx) |
| [middleware](https://github.com/oekit/middleware) | Hertz 自研标准中间件栈 + contrib 桥接 + hmw | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[pkg](https://github.com/oekit/pkg)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
| [nns](https://github.com/oekit/nns) | NNS 服务治理客户端——provider 无关发现/配置/实例缓存 | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[registry](https://github.com/oekit/registry) |
| [nns-endpoint](https://github.com/oekit/nns-endpoint) | NNS 端点工厂——per-service 熔断、换实例重试、gRPC 池 | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[httpx](https://github.com/oekit/httpx)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[nns](https://github.com/oekit/nns)、[registry](https://github.com/oekit/registry) |
| [bootstrap](https://github.com/oekit/bootstrap) | 应用生命周期引导——bootstrap.Run 骨架 + healthz | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx) |
| [handler](https://github.com/oekit/handler) | HTTP handler 层——通用 CRUD Handler、参数绑定、版本端点 | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[gorm](https://github.com/oekit/gorm)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[middleware](https://github.com/oekit/middleware)、[pkg](https://github.com/oekit/pkg)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
| [hertz](https://github.com/oekit/hertz) | Hertz 运行时集成——日志桥接、生命周期、SSE/health/metrics、适配器、OTel | [bootstrap](https://github.com/oekit/bootstrap)、[concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[nns](https://github.com/oekit/nns)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
| [kitex](https://github.com/oekit/kitex) | Kitex 生态——日志桥接、治理热更、生命周期、BizError 桥接、kitexotel | [bootstrap](https://github.com/oekit/bootstrap)、[concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[nns](https://github.com/oekit/nns)、[pkg](https://github.com/oekit/pkg)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
| [workflow](https://github.com/oekit/workflow) | 组合式流程编排——串行/并行/异步/条件分支 + Hook AOP | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx) |
| [kitex-examples](https://github.com/oekit/kitex-examples) | Kitex echo 示例——client/server + IDL + 治理配置 | [bootstrap](https://github.com/oekit/bootstrap)、[concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[kitex](https://github.com/oekit/kitex)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[nns](https://github.com/oekit/nns)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |