# nns-endpoint

> 端点工厂——服务发现+熔断+换实例重试封装成一个函数

> 🌐 项目主页：**https://oekit.github.io/** · 名称由来：**O**pen **E**asy **Kit**（开源 · 易用 · 工具集合）
> 🤖 AI Agent：本仓规范与硬规则见 [AGENTS.md](AGENTS.md)；机器可读索引见 [llms.txt](https://oekit.github.io/llms.txt)（接口签名以源码为准）。

## 安装

```bash
go get github.com/oekit/nns-endpoint@v0.2.3
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


## 矩阵总览（24 活跃模块，自动生成）

| 模块 | 内容 | 矩阵内依赖 |
|---|---|---|
| [pkg](https://github.com/oekit/pkg) | 微工具集 21 合 1（errors/bloomx/contextx/container/stringx/slicex/enumx/convert/encoding/jsonrepair/httpjson/mathx/pathx/pagination/ptrx/timex/cache/bufiox/strmap/singleflighttest/urlguard；v0.10.0 起 uidgen/validator 拆至 kitx）；errors（v0.12.0 起）New/As/Is/Unwrap 与 stdlib 同型透传——下游哨兵/As 判定单 import 即可，类型同一性红线可完整执行；cache（v0.4.0 起）含缓存接口 CacheAside + BatchGetter 批量能力（MGet）+ LocalCache/SingleFlightCache + cachetest 契约（Run 真实时间 + RunWithClock 假时钟确定性 TTL 边界）+ Checker；LocalCache 时钟可注入（LocalConfig.Now——TTL/NullTTL 全路径确定性测试，v0.13.0 起）；container（v0.9.0 起，收编原 collectionx）泛型容器 Set/Ring/Queue/SafeMap + Map（sync.Map 背书）/Slice（并发安全）；mathx/jsonrepair（v0.11.0）Unstable 单边向上抖动 Upward / 宽松解析 UnmarshalLenient；timex/stringx（v0.7.0 起）时间段边界 BeginningOf/EndOf 七对（分/时/日/周/月/季/年，周起始显式参数）+ trie 多模式替换器 Replacer（最长匹配）/ NewMask 敏感词遮罩；singleflighttest（v0.8.0 起）singleflight 共享契约测试套件（internal 与 concurrency 双侧引用）；urlguard（v0.15.0 起）出站 SSRF 防护原语——CheckURL 字面量校验（私网/loopback/链路本地含云元数据 169.254.169.254/CGNAT/ULA/组播保留、.internal·.local·localhost 域名、scheme 白名单、IPv4-mapped IPv6 归一）+ DialFunc 拨号层预解析校验与实际 RemoteAddr 端点核对闭合 TOCTOU（Happy-Eyeballs 覆盖），构造态豁免 WithAllowPrivate/WithAllowCIDRs/WithAllowDomains，与 httpx WithDialFunc 一行组合（httpx#11） | — |
| [concurrency](https://github.com/oekit/concurrency) | 并发与流控原语（async/gopool/group/singleflight/progress/syncx/ratelimit/retry）+ window 滑动窗口 / breaker 自适应熔断（规范哨兵 ErrCircuitOpen）/ shedding CPU 自适应降载 / batch 攒批执行器（满条数/满字节/窗口去重）/ timewheel 单级时间轮；v0.5.0 起 retry 指数退避+抖动（MaxDelay 封顶/溢出饱和）、loop 周期原语（忙时丢 tick/单轮 panic 收编/FirstDelay 可打断）、async labeled RunSafe（panic 按任务标签上报）、syncx.FanInErr 逐槽错误（panic 槽显式标记）、KeyedLimiter 增强（容量护栏/RetryAfter/速率热更）并接入 pkg/singleflighttest 契约测试；v0.8.0 起 singleflight DoShared 折叠可观测（Result.Waiters）、async panic 统一收编 typed *PanicError（errors.As 可辨、携带栈）+ 实例级 Runner panic 路由、retry DelayHint（服务端 Retry-After 建议优先）+ JitterRatio 比例抖动 + retry/httpx Retryable 可重试性判定；v0.9.0 起 breaker 显式三态状态机 closed/open/half-open——冷却（WithProbeTimeout 缺省 1m）期满首个放行即半开探测、探测成功关闸清窗/失败重开、失联超时自动回收 + AbandonGen 显式放弃，GenBreaker 代绑定（AllowGen/SuccessGen/FailureGen/AbandonGen：代仅在探测放行自增、陈旧代回报整体作废、开放期失败不后移冷却）；v0.10.0 起 WithCooldown 独立业务冷却期（缺省回落 probeTimeout）+ WithConsecutiveFailures 连续失败计数触发（与速率型并联，补低 QPS 信号，对齐 Envoy consecutive_5xx）；v0.11.0 起 ratelimit Lockout 防爆破锁（fail2ban 形态：滑窗失败计数→阈值锁定拒绝 LockDuration→成功 Reset 清零/到期自解，CheckLocked 返回剩余时长）；v0.12.0 起 retry/httpx RetryableWithHeaders（403 + Retry-After/X-RateLimit-Remaining 头感知可重试判定，HeaderSource 零依赖抽象）、syncx FanInLimit 受限并发扇出（固定 worker 防下游过载）+ FanInLimitCtx（v0.14.0，ctx 感知：ctx 已取消未启动项 fn 一次不调、剩余槽快速排空 Canceled、在飞项照常落槽）、ratelimit WithTokenClock 时钟注入；v0.13.0 起 breaker 熔断期收敛到探测级（forcePass 强制探活 + 冷却期满半开探测，其余拒绝——SRE 公式退役为 closed→open 触发器，WithConsecutiveFailures 触发后流量真停）+ State()/State 枚举可观测出口；v0.16.0 起 ratelimit NewKeyedLimiterByFunc[K]——factory func(K) Limiter 按 key 定配额（一个管理器按接口维度分档位，NewKeyedLimiterOf 零破坏随迁；未登记 key 拿 factory 兜底档的坑已在 doc 点名，#31）；v0.17.0 起 KeyedLimiter.Create 护栏内原子登记（表满先回收空闲与 Allow 同路、仍满拒绝计 OverCapHits、已存在不覆盖——热更走 Replace；解决 ByFunc factory 库锁内调用致消费方锁序倒置，#33） | [pkg](https://github.com/oekit/pkg) |
| [logx](https://github.com/oekit/logx) | 统一日志门面（多后端可替换 + caller 归属；Sink 已迁嵌套模块 logx/sink，根包纯 stdlib）；WithFilter 脱敏/丢弃包装（FilterKey 命中键替换 ***/FilterFunc 丢整条，子 logger 保持规则）、Slogger/SlogHandler 正向桥（三方 slog 直接消费 logx 生态）、TraceHandler 任意 slog.Handler 自动注入 trace_id、NewAuditLogger 审计留痕（级别锁 Info，不受 LOG_LEVEL 降级）、LevelEnabler 热路径提前判级、ParseLogLevel 返回 (Level, error) + ParseLogLevelOr 配错退默认（v0.7.0）；优雅关停正身在 core/lifecycle.Manager（logx/shutdown 仅余 Deprecated 别名）；桥接子包 hlog/hertzlog/kitexlog 为嵌套模块按需引入 | — |
| [core](https://github.com/oekit/core) | 应用基建件——governance 治理 schema、config 配置加载（LoadBootstrap/LoadDotEnv/Get* 系、default/env/range/options tag 校验、OverlaySource 多源叠加热更、ApplyEnvOverlay env 优先 12-factor 形态）、transport 协议无关传输上下文（Transporter/Header/Server·Client ctx 存取，仅标准库）、chain 中间件链（Handler/Middleware/Chain/Recovery/Timeout，与 nns-endpoint.Endpoint 同形）、lifecycle 契约 + Manager 优雅关停（自 logx/shutdown 迁入，日志 *slog.Logger——core 不再依赖 logx）、network IP 工具、version；v0.9.0 起 config FieldSet 显式提供集合（yaml 显式 port:0 不再被 default/env 覆盖——LoadBootstrap 默认接线，免全量指针化）+ require tag（必须 yaml/env 提供，缺失列全并附 env 提示）+ v0.9.1 修 yaml:,inline 字段在 FieldSet 下漏标记（inline 叶子显式零值曾被 default 覆盖）；v0.9.2 env 卫生改纯静态（仅格式/重名，移除依赖进程环境的拼写冒烟检查——误报 PATH/HOME、漏无人设置的真拼错，core#8），命名一致性走 LintEnvNaming 诊断 / WithEnvNamingLint 可选 fail-closed / ignorederived="true" 豁免 | — |
| [kitx](https://github.com/oekit/kitx) | 标识与校验——uidgen（Snowflake 全局单例+Extract 元数据、手写 RFC 4122 v4 UUID 零第三方依赖、RandomHex/RandomString 拒绝采样随机串）+ validator（go-playground/validator v10 接口化门面：struct tag 校验/自定义规则注册/ValidationError Unwrap 为 BizError 400 对接 middleware.Fail）+ hash（v0.2.0 起非密码学哈希助手 Md5Hex/Md5Bytes——缓存键/幂等键/短唯一 code，格式稳定承诺，pkg 2026-09-15 治理移除弱哈希后 kitx 侧收编 pkg#10），自 pkg v0.10.0 拆出 | [pkg](https://github.com/oekit/pkg) |
| [metrics](https://github.com/oekit/metrics) | Prometheus 门面 + 弹性指标族 | [core](https://github.com/oekit/core) |
| [tracing](https://github.com/oekit/tracing) | OpenTelemetry 追踪 + trace_id 真相源 | [logx](https://github.com/oekit/logx) |
| [registry](https://github.com/oekit/registry) | 注册发现核心 + Nacos 实现 + Nacos 配置源 + sdbridge | [core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx) |
| [data](https://github.com/oekit/data) | 数据访问门面——复刻 ekit data/ 能力面：cache/rediscache/crud/distlock/gormx/redisx/timertask/cachetest 八子包 re-export（类型同一，实现住 pkg 与各子仓）+ cached 缓存读写编排（写后失效/事务提交后失效/查询缓存版本号失效/GetByIDs 批量点读 MGet 快路径/bloom 防穿透）+ twolevel 两级缓存与跨实例失效广播（v0.7.0 起本仓唯二非 re-export 实现）；all/ 聚合模块已废弃 retract | [concurrency](https://github.com/oekit/concurrency)、[gormx](https://github.com/oekit/gormx)、[logx](https://github.com/oekit/logx)、[pkg](https://github.com/oekit/pkg)、[redisx](https://github.com/oekit/redisx)、[timertask](https://github.com/oekit/timertask) |
| [gormx](https://github.com/oekit/gormx) | SQL/GORM 全家——连接池增强、框架无关 CRUD Service、仓储（原 oekit/gorm，v0.3.0 更名） | [logx](https://github.com/oekit/logx)、[pkg](https://github.com/oekit/pkg) |
| [redisx](https://github.com/oekit/redisx) | Redis 全家——客户端封装（根包）、分布式缓存 cache/、分布式锁 distlock（Locker 同形可互换；Renew 手动续期（false=锁丢失，不进 Locker 接口保 etcdx 互换）+ WithAutoRenew 覆盖 TryLock 自动续期（v0.8.0 feat!：此前组合不续期）；Semaphore ZSET 槽位信号量（v0.8.0：Acquire 同 identifier 幂等刷新 + Lua 内原子清过期槽崩溃自愈、Renew 仅未过期槽可续、Cleanup 返条数、无 Redis 降级 fail-fast））、分布式限流 ratelimit（Lua 三态 + 故障本地兜底）；v0.9.0 起 distlock Lockout 分布式防爆破锁（Redis 侧 fail2ban：滑窗失败计数→阈值锁定 Penalty→Reset 清零，与 concurrency/ratelimit.Lockout 进程内孪生——多实例共享计数是存在理由；惩罚只按首次触发计算、nil client fail-fast 不降级，#6） | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx)、[pkg](https://github.com/oekit/pkg) |
| [etcdx](https://github.com/oekit/etcdx) | etcd 全家——客户端封装（根包：生产默认值/Health 探活/Close(ctx) 幂等）、registry 注册发现实现（SPI 对接 registry/nacos 链路：Lease KeepAlive 注册 + 丢约指数退避自动重注册、prefix Watch 订阅推送、ConfigStore KV 配置监听）、distlock 分布式锁（Session+concurrency.Mutex 公平排队，接口与 redisx/distlock.Locker 同形可互换，可选无 etcd 降级放行） | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[registry](https://github.com/oekit/registry) |
| [httpx](https://github.com/oekit/httpx) | HTTP 客户端 + 负载均衡——服务发现/熔断（规范哨兵 ErrCircuitOpen）/重试/指标/OTel 透传；loadbalance Selector 形态（RoundRobin/Random/Weighted/一致性哈希权重感知虚拟副本/p2c 自适应——EWMA 经 DoneFunc 回写建立、Err 计 1s 惩罚；WithFilter 按元数据过滤；v0.4.0 起 Balancer 全家族删除无兼容路径）；codec 注册制编解码器（json 内置 + form 表单，DoCodec 请求按 codec 序列化、响应按 Content-Type 反查回退请求 codec）；v0.6.0 起 net/http 桥接层 NewRoundTripper/StdClient——存量标准库外呼一行 Transport 接入熔断/重试/指标/链路（重试缺省关显式开、非 2xx 非错误、hop-by-hop 隔离，#8）；v0.8.0 起 SafeDialFunc/WithSafeDial 出站 SSRF 防护胶水（判定单源 pkg/urlguard v0.15.0：解析 IP 全量预检 + 实际 RemoteAddr 端点核对闭合重解析 TOCTOU、Happy-Eyeballs 覆盖，构造态豁免；v0.7.0 内置的独立 httpx/urlguard 实现收敛删除，#11） | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[pkg](https://github.com/oekit/pkg)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
| [network](https://github.com/oekit/network) | 轻量网络客户端——WebSocket/NATS/MQ 抽象、连接与流工具 | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx)、[tracing](https://github.com/oekit/tracing) |
| [timertask](https://github.com/oekit/timertask) | 分片定时任务（FNV-1a、心跳续约）；v0.1.5 起可选可靠投递 DeliveryAtLeastOnce（租约 + Ack + 可见性超时；过期租约回扫重投/死信、停机 best-effort 收租，v0.1.7 接入生命周期真正生效） | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx) |
| [middleware](https://github.com/oekit/middleware) | Hertz 标准中间件栈（模块根 mw，双层目录已收平）+ contrib 桥接 + hmw + SheddingMiddleware 自适应降载 + chainmw 子包（transport 感知框架无关中间件，基于 core/chain+transport：Recovery 带 panic 隔离上报钩子 WithRecoveryReporter、AccessLog skip+redact+自定义字段、MetadataPropagate——kitex 经 kitex/chainmw.ToKitex 复用同一实现，根包 Recovery/AccessLog 已薄壳化复用 chainmw 内核、下游 handler 可经 transport.FromServerContext 读协议无关传输信息）；v0.6.0 起 accesslog 凭证路径脱敏（WithAccessLogRedactPathPrefixes）、ratelimit 默认 KeyFunc 去端口（连接桶→地址桶）；v0.11.0 起 WithAccessLogSink 落库 sink（AccessRecord{Method,Path,Status,Latency,ClientIP,UserAgent,TraceID} 与日志同源、c 同传可读业务键、error/panic 只记 Warn 不影响主链路、SkipPaths 命中不触发）+ chainmw.WithSink 内核同构（kitex 随 ToKitex 获得）+ StackConfig.DisableResponseWrap 响应自管一键形态（TraceID→Recovery→AccessLog）与 AccessLogOptions 透传 + doc/README「适用边界」段（响应契约统一 vs handler 自管的推荐组合，#18）；v0.12.0 起 AccessLog panic 恢复点产出记录（AccessRecord.Panic 标记 + Err 摘要，Hertz status 按 Recovery 缺省 500）后按原值重抛——panic 请求不再零访问记录，sink/kitex 桥接同步留痕，#20 | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[pkg](https://github.com/oekit/pkg)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
| [k8s](https://github.com/oekit/k8s) | Kubernetes 客户端封装——RESTConfig/Clientset/DynamicClient 构建（显式 kubeconfig → in-cluster 自动 → KUBECONFIG/~/.kube/config 逐级回退，QPS/Burst 限流默认 50/100）+ leaderelection 子包 Lease 选主简化封装（OnStartedLeading/OnStoppedLeading/OnNewLeader 回调，主备单活，仅要求 Lease CRUD 权限） | [logx](https://github.com/oekit/logx) |
| [nns](https://github.com/oekit/nns) | NNS 服务治理客户端——provider 无关发现/配置/实例缓存 | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[registry](https://github.com/oekit/registry) |
| [nns-endpoint](https://github.com/oekit/nns-endpoint) | NNS 端点工厂——per-service 熔断、换实例重试、gRPC 池 | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[httpx](https://github.com/oekit/httpx)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[nns](https://github.com/oekit/nns)、[registry](https://github.com/oekit/registry) |
| [bootstrap](https://github.com/oekit/bootstrap) | 应用生命周期引导——New(WithServiceName/WithShutdownTimeout/WithBeforeServe) + Run(fn) + healthz；App.Ready/ReadyChan/IsReady 就绪门（幂等并发安全）——嵌套 bootstrap/nacos 自 v0.3.0 起 NNS 注册延迟到 Ready 之后（server 就绪 → userFn 触发 Ready → 才进注册中心）、关停序 nns 反注册先于 server 关闭；Metrics/Tracing 在嵌套模块 bootstrap/obs | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx) |
| [hertz](https://github.com/oekit/hertz) | Hertz 运行时集成（模块根扁平化）——日志桥接、生命周期、SSE/health/metrics、适配器、OTel、crudh/validatorh（基于 kitx/validator）/versionh（原 handler 微库收编）；RunServer 变参就绪门 WithOnReady（全部 HostPort TCP 探活可建连后回调，探活超时按启动失败优雅关机；缺省超时 5s 经 WithReadyProbeTimeout 调整/WithReadyAddr 探活地址），配合 bootstrap Ready 实现 NNS 延迟注册；transportx（hertz→core/transport 同构适配器）+ chainmw 桥接 | [bootstrap](https://github.com/oekit/bootstrap)、[concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[gormx](https://github.com/oekit/gormx)、[kitx](https://github.com/oekit/kitx)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[middleware](https://github.com/oekit/middleware)、[nns](https://github.com/oekit/nns)、[pkg](https://github.com/oekit/pkg)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
| [kitex](https://github.com/oekit/kitex) | Kitex 生态——日志桥接、治理热更、生命周期、BizError 桥接、kitexotel、istio（xDS 动态熔断，原 istio 微库收编）；RunServer 变参就绪门（WithOnReady/WithReadyAddr/WithReadyProbeTimeout）+ chainmw.ToKitex（core/chain.Middleware → kitex 中间件，与 hertz 共享 middleware/chainmw 实现）+ transportx | [bootstrap](https://github.com/oekit/bootstrap)、[concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[nns](https://github.com/oekit/nns)、[pkg](https://github.com/oekit/pkg)、[registry](https://github.com/oekit/registry) |
| [workflow](https://github.com/oekit/workflow) | 组合式流程编排——串行/并行/异步/条件分支 + Hook AOP | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx) |
| [kitex-examples](https://github.com/oekit/kitex-examples) | Kitex echo 示例——client/server + IDL + 治理配置 | [bootstrap](https://github.com/oekit/bootstrap)、[concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[kitex](https://github.com/oekit/kitex)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[nns](https://github.com/oekit/nns)、[pkg](https://github.com/oekit/pkg)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
> 依赖严格自底向上；微库已合并为 pkg，旧微库仓已删除。

> 本仓在矩阵中的位置与依赖见上表「矩阵内依赖」列。历史沿革：源自 ekit 单体库
> （git.enjoye.top/enjoydream/ekit v0.34.0，冻结维护），2026-10 拆分为独立 module。

## License

MIT — 见 [LICENSE](LICENSE)
