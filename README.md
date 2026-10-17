# nns-endpoint

> 端点工厂——服务发现+熔断+换实例重试封装成一个函数

> 🌐 项目主页：**https://oekit.github.io/** · 名称由来：**O**pen **E**asy **Kit**（开源 · 易用 · 工具集合）
> 🤖 AI Agent：本仓规范与硬规则见 [AGENTS.md](AGENTS.md)；机器可读索引见 [llms.txt](https://oekit.github.io/llms.txt)（接口签名以源码为准）。

## 安装

```bash
go get github.com/oekit/nns-endpoint@v0.1.3
```

## 这个仓库做什么

`nns-endpoint` 是**端点工厂**：把「服务发现 + per-service 熔断 + 失败换实例重试 +
gRPC 连接池」封装成一个可调用的 `Endpoint` 函数。

## 熔断语义（重点）

- 熔断按服务分片，**4xx 不计熔断失败**（调用方参数错误与对端健康无关）
- 5xx / 网络错误计失败；open 状态错误用 `errors.Is(err, endpoint.ErrCircuitOpen)` 判定
- HTTP 端点失败自动**换实例重试**；gRPC 端点不做重试（streaming/非幂等语义复杂）

## 快速使用

```go
import endpoint "github.com/oekit/nns-endpoint"

factory := endpoint.NewHTTPEndpointFactory(nnsc.ServiceClient())
ep, _ := factory.MakeEndpoint("payment-svc")

resp, err := ep(ctx, req) // 背后：发现 + 熔断 + 换实例重试
```

## 边界

- 依赖 [nns](https://github.com/oekit/nns) 的 `ServiceClient()`、[httpx](https://github.com/oekit/httpx) 的发现/负载均衡
- 历史注记：原为 nns 门面的 `MakeHTTPEndpoint`，拆分后独立成仓


## 矩阵总览（21 活跃模块，自动生成）

| 模块 | 内容 | 矩阵内依赖 |
|---|---|---|
| [pkg](https://github.com/oekit/pkg) | 微工具集 21 合 1（errors/bloomx/contextx/container/validator/stringx/slicex/enumx/convert/encoding/jsonrepair/httpjson/mathx/pathx/pagination/ptrx/timex/uidgen/cache/bufiox/strmap）；cache（v0.4.0 起）含缓存接口 CacheAside + BatchGetter 批量能力（MGet）+ LocalCache/SingleFlightCache + cachetest 契约 + Checker；container（v0.9.0 起，收编原 collectionx）泛型容器 Set/Ring/Queue/SafeMap + Map（sync.Map 背书）/Slice（并发安全）；timex/stringx（v0.7.0 起）时间段边界 BeginningOf/EndOf 七对（分/时/日/周/月/季/年，周起始显式参数）+ trie 多模式替换器 Replacer（最长匹配）/ NewMask 敏感词遮罩；singleflighttest（v0.8.0 起）singleflight 共享契约测试套件（internal 与 concurrency 双侧引用） | — |
| [concurrency](https://github.com/oekit/concurrency) | 并发与流控原语（async/gopool/group/singleflight/progress/syncx/ratelimit/retry）+ window 滑动窗口 / breaker 自适应熔断（规范哨兵 ErrCircuitOpen）/ shedding CPU 自适应降载 / batch 攒批执行器（满条数/满字节/窗口去重）/ timewheel 单级时间轮 | — |
| [logx](https://github.com/oekit/logx) | 统一日志门面（多后端可替换 + Sink + caller 归属）+ 优雅关停；桥接子包 hlog/hertzlog/kitexlog 为嵌套模块按需引入 | — |
| [core](https://github.com/oekit/core) | 应用基建件（governance 治理 schema / config 配置加载 + default/env/range/options tag 校验 / lifecycle 契约 / network IP 工具 / version） | [logx](https://github.com/oekit/logx) |
| [metrics](https://github.com/oekit/metrics) | Prometheus 门面 + 弹性指标族 | [core](https://github.com/oekit/core) |
| [tracing](https://github.com/oekit/tracing) | OpenTelemetry 追踪 + trace_id 真相源 | [logx](https://github.com/oekit/logx) |
| [registry](https://github.com/oekit/registry) | 注册发现核心 + Nacos 实现 + Nacos 配置源 + sdbridge | [core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx) |
| [data](https://github.com/oekit/data) | 数据访问门面——复刻 ekit data/ 能力面：cache/rediscache/crud/distlock/gormx/redisx/timertask/cachetest 八子包 re-export（类型同一，实现住 pkg 与各子仓）+ cached 缓存读写编排（写后失效/事务提交后失效/查询缓存版本号失效/GetByIDs 批量点读 MGet 快路径/bloom 防穿透）+ twolevel 两级缓存与跨实例失效广播（v0.7.0 起本仓唯二非 re-export 实现）；all/ 聚合模块已废弃 retract | [concurrency](https://github.com/oekit/concurrency)、[gormx](https://github.com/oekit/gormx)、[logx](https://github.com/oekit/logx)、[pkg](https://github.com/oekit/pkg)、[redisx](https://github.com/oekit/redisx)、[timertask](https://github.com/oekit/timertask) |
| [gormx](https://github.com/oekit/gormx) | SQL/GORM 全家——连接池增强、框架无关 CRUD Service、仓储（原 oekit/gorm，v0.3.0 更名） | [logx](https://github.com/oekit/logx)、[pkg](https://github.com/oekit/pkg) |
| [redisx](https://github.com/oekit/redisx) | Redis 全家——客户端封装（根包）、分布式缓存 cache/、分布式锁 distlock、分布式限流 ratelimit（Lua 三态 + 故障本地兜底） | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx)、[pkg](https://github.com/oekit/pkg) |
| [httpx](https://github.com/oekit/httpx) | HTTP 客户端 + 负载均衡——服务发现/熔断（规范哨兵 ErrCircuitOpen）/重试/指标/OTel 透传/一致性哈希（权重感知虚拟副本） | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
| [network](https://github.com/oekit/network) | 轻量网络客户端——WebSocket/NATS/MQ 抽象、连接与流工具 | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx)、[tracing](https://github.com/oekit/tracing) |
| [timertask](https://github.com/oekit/timertask) | 分片定时任务（FNV-1a、心跳续约） | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx) |
| [middleware](https://github.com/oekit/middleware) | Hertz 自研标准中间件栈 + contrib 桥接 + hmw + SheddingMiddleware 自适应降载 | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[pkg](https://github.com/oekit/pkg)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
| [nns](https://github.com/oekit/nns) | NNS 服务治理客户端——provider 无关发现/配置/实例缓存 | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[registry](https://github.com/oekit/registry) |
| [nns-endpoint](https://github.com/oekit/nns-endpoint) | NNS 端点工厂——per-service 熔断、换实例重试、gRPC 池 | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[httpx](https://github.com/oekit/httpx)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[nns](https://github.com/oekit/nns)、[registry](https://github.com/oekit/registry) |
| [bootstrap](https://github.com/oekit/bootstrap) | 应用生命周期引导——bootstrap.Run 骨架 + healthz | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx) |
| [hertz](https://github.com/oekit/hertz) | Hertz 运行时集成——日志桥接、生命周期、SSE/health/metrics、适配器、OTel、crudh/validatorh/versionh（原 handler 微库收编） | [bootstrap](https://github.com/oekit/bootstrap)、[concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[gormx](https://github.com/oekit/gormx)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[middleware](https://github.com/oekit/middleware)、[nns](https://github.com/oekit/nns)、[pkg](https://github.com/oekit/pkg)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
| [kitex](https://github.com/oekit/kitex) | Kitex 生态——日志桥接、治理热更、生命周期、BizError 桥接、kitexotel、istio（xDS 动态熔断，原 istio 微库收编） | [bootstrap](https://github.com/oekit/bootstrap)、[concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[nns](https://github.com/oekit/nns)、[pkg](https://github.com/oekit/pkg)、[registry](https://github.com/oekit/registry) |
| [workflow](https://github.com/oekit/workflow) | 组合式流程编排——串行/并行/异步/条件分支 + Hook AOP | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx) |
| [kitex-examples](https://github.com/oekit/kitex-examples) | Kitex echo 示例——client/server + IDL + 治理配置 | [bootstrap](https://github.com/oekit/bootstrap)、[concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[kitex](https://github.com/oekit/kitex)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[nns](https://github.com/oekit/nns)、[pkg](https://github.com/oekit/pkg)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
> 依赖严格自底向上；微库已合并为 pkg，旧微库仓已删除。

> 本仓在矩阵中的位置与依赖见上表「矩阵内依赖」列。历史沿革：源自 ekit 单体库
> （git.enjoye.top/enjoydream/ekit v0.34.0，冻结维护），2026-10 拆分为独立 module。

## License

MIT — 见 [LICENSE](LICENSE)
