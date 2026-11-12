# Changelog — oekit/nns-endpoint

由 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来。所有 notable 变更记录如下。

## [v0.3.5] — 2026-11-12

- Go 1.26 现代化清扫；依赖地板对齐（core v0.15.0、concurrency v0.34.1、pkg v0.46.0、httpx v0.11.0、nns v0.8.0 已随 #146 抬升）。纯等价重构+地板追平，行为零变更。

## [v0.3.4] — 2026-11-11

- fix(http): 确定性本地错误（响应解码/请求序列化/选点）判为不可重试、不计熔断样本（#123）——错误分类维度从「有无 statusError」改为「错误归属」：新增 `localError` 包装（`errors.As` 判定），marshal/unmarshal/select 类错误重试同一输入恒失败，此前却无 statusError 被判为可重试网络错误，每个坏请求白打 (1+Retry) 次真实 HTTP、每次 done(Err) 给节点记 EWMA 故障，连续 6 次调用即把整个服务熔断打开；2xx 空响应体 + Result 非 nil 视同 json null（跳过解码返回保持零值的 Result），不再报 unexpected end of JSON input
- fix(endpoint): done 回写区分错误归属——调用方错误不惩罚节点（#124）——HTTP 4xx 与 gRPC 调用方错误码（InvalidArgument/NotFound/PermissionDenied 等按 grpc-go HTTP 映射落在 4xx 区间的码）经 `callerFault` 归属判定后回写 `DoneInfo{Err: nil}`，p2c EWMA 不再把流量推离只是碰上坏请求的健康节点（#97 同污染通道另一入口）；熔断样本（`breakerSuccessful`）与均衡器样本自此同口径。节点/网络错误（5xx/网络层/Internal/Unavailable/DeadlineExceeded）原样回写，惩罚语义不变
- chore: 现代语言特性贴合——`errors.As/Is` 错误归属判定、`for range int` 测试循环、`min()` 响应体截断

关联问题单：nns-endpoint#123、nns-endpoint#124。

## [v0.3.3] — 2026-11-10

- - build: 依赖地板对齐（第二轮发版后 floor sync，无代码变更）

## [v0.3.2] — 2026-11-10

- - build: 依赖地板对齐（floor sync，无代码变更）

## [v0.3.1] — 2026-11-10

- - build: 依赖地板对齐（floor sync，无代码变更）

## [v0.3.0] — 2026-11-09

- fix(http): 无实例/配置故障不计熔断样本（#96）
- fix(grpc): 熔断拒绝不回写节点 done——p2c EWMA 不被污染（#97）
- fix(balancer): selector 首建瞬时失败不钉死 roundrobin（#98）
- fix(pool): 空闲驱逐复核在途引用——活跃流式连接不被 Close（#99）

关联问题单：nns-endpoint#96、nns-endpoint#97、nns-endpoint#98、nns-endpoint#99。

## [v0.2.15] — 2026-11-08

- build: 依赖地板对齐——本版把 oekit 依赖地板抬到各依赖当前最新已发布版本（matrix-tools/scripts/align_floors.py 维护轮）

## [v0.2.14] — 2026-11-07

- feat(grpc): GRPCRequest.Reply 类型化解码目标——调用方传入 proto.Message 指针，Invoke 不再对 var reply any 解码（#71 #73）
- fix(pool): 满池先全局驱逐空闲超龄连接再拒绝——滚动发布换 IP 场景新地址不再被陈旧连接顶死（#72 #76）
- fix(endpoint): 超时下限与兜底链（显式 Timeout > ReadTimeout > 30s 兜底+Warn）——req.Timeout=0 不再立即 DeadlineExceeded（#75）
- fix(grpc): Invoke 失败路径回写 selector done（修复已随 #57 合入，本轮补失败路径回归钉）（#74）
- fix(pool): 默认不再注入 insecure 凭据——缺凭据显式报错，TLS 场景显式二选一（BREAKING，PR #78）（#77）
- fix(http): 适配 httpx v0.8.12 NewClient 两值签名——固定 options 走 MustNewClient 逃生舱（httpx#83 BREAKING 后续）

关联问题单：nns-endpoint#71、nns-endpoint#72、nns-endpoint#73、nns-endpoint#74、nns-endpoint#75、nns-endpoint#76、nns-endpoint#77。

## [v0.2.13] — 2026-11-06

- fix(grpc): 调用结果经 DoneFunc 回写——修复 p2c 在 gRPC 路径静默失效（[#57](https://github.com/oekit/nns-endpoint/issues/57)）——`sel.Select` 的 done 回调此前只在 `pool.Get` 失败路径回调一次，`conn.Invoke` 的最终结果（成功/错误）从不回写：httpx loadbalance 的契约是「DoneFunc 在最终结果确定后回调」，p2c 依赖它建节点级 EWMA 并对 Err 计 1s 惩罚——未回写时所有节点 EWMA 恒 0，p2c 每次都落等值随机分支，故障/慢节点永不降权（与宣称语义相反）；对照 HTTP 路径一直正确回写。修法：breaker Execute 之后（成功与错误两路）补 `done(ctx, DoneInfo{Err: err})`，与 http.go 同构
- fix(grpc+http): `gobreaker.ErrTooManyRequests` 与 `ErrOpenState` 一并映射 `ErrCircuitOpen` + 计 `ObserveRejected`（[#58](https://github.com/oekit/nns-endpoint/issues/58)）——half-open 态并发超 `MaxRequests`（缺省 3）时 gobreaker 返回的是另一个哨兵 ErrTooManyRequests：同属熔断拒绝却被裸透出去，消费方的 `errors.Is(err, ErrCircuitOpen)` 判定失效、指标漏计。两个工厂同步修复

关联问题单：nns-endpoint#57、nns-endpoint#58。

## [v0.2.12] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [v0.2.11] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [v0.2.10] — 2026-11-02

- 抬嵌套模块地板 github.com/oekit/registry/nacosimpl v0.1.0 → v0.1.1（oekit/oekit#104）

## [v0.2.9] — 2026-11-02

- build: 矩阵内依赖地板 `concurrency` v0.23.2 → v0.23.3、`httpx` v0.8.7 → v0.8.8（维护轮全量对齐；本版不含 API 变更）

## [v0.2.8] — 2026-11-02

- build: 矩阵内依赖地板 `pkg` v0.31.0 → v0.31.1（维护轮全量对齐；本版不含 API 变更）

## [v0.2.7] — 2026-11-01

- build: 矩阵内依赖地板 `httpx` v0.8.5 → v0.8.7、`nns` v0.5.4 → v0.5.5（维护轮全量对齐；本版不含 API 变更）

## [v0.2.6] — 2026-10-23

- build: 矩阵内依赖地板对齐——httpx v0.8.0 → v0.8.2、nns v0.4.2 → v0.4.4（#27）

## [v0.2.5] — 2026-10-22

- fix(metrics): 熔断状态类型化传递——`ObserveTransition` 改走 `ObserveTransitionTyped`（`resilience.CircuitState`），不再以裸 int 跨模块流转
- fix(metrics): 补 gobreaker.State ↔ resilience.CircuitState 的**编译期数值一致性钉子**（3 条），两侧数值一旦分叉即编译失败
- build: 矩阵内依赖地板对齐——metrics v0.1.7 → v0.1.9、core v0.7.0 → v0.9.2
- build: 测试改用嵌套模块 registry/nacos——registry v0.5.0 起不再提供该子包
  - 依赖地板：registry v0.4.1 → **v0.5.0**（新增 require `github.com/oekit/registry/nacos v0.1.0`，中接 `registry/nacosimpl v0.1.0`）；logx v0.7.0 → v0.7.1
  - registry v0.5.0 把 Nacos 客户端门面拆成独立发版的嵌套模块，import 路径不变但「由哪个 module 提供」变了

## [v0.2.4] — 2026-10-19

- build: 矩阵内依赖地板对齐（align_floors 不动点）：httpx v0.4.2 → v0.8.0、logx v0.6.0 → v0.7.0、nns v0.4.1 → v0.4.2

## [v0.2.3] — 2026-10-18

- fix(deps): httpx v0.4.2 地板对齐（零代码改动）

## [v0.2.2] — 2026-10-18

- fix(deps): metrics v0.1.7 地板对齐（零代码改动）

## [v0.2.1] — 2026-10-18

- fix(deps): 全部 oekit 依赖地板对齐最新（httpx v0.4.1、nns v0.4.1、registry v0.4.1、logx v0.6.0、core v0.7.0，零代码改动）

## [v0.2.0] — 2026-10-17

### Breaking（go-kratos 对齐：随 httpx v0.4.0 Selector 全量迁移）
- `balancerCache` → `selectorCache`（`loadbalance.Selector`，经服务配置 Lb 字段
  `NewSelector(NormalizeSelectorName(lb))` 构造，不可识别回退 roundrobin）
- `HTTPEndpointFactory.ResetBalancer`/`GRPCEndpointFactory.ResetBalancer` → `ResetSelector`
- HTTP/gRPC 选点从 `Balancer.Pick(索引)` 切换为 `Selector.Select`（Node 携带元数据、
  调用结果经 DoneFunc 回写）
- 新增 `WithSelector(sel loadbalance.Selector)` 工厂选项：全服务共用注入的选择器，
  覆盖服务治理配置中的 lb 策略
- 依赖升级：httpx v0.4.0 / nns v0.4.0 / registry v0.4.0（三仓均扁平化/Selector 化）

## [v0.1.7] — 2026-10-17

- httpx 地板 v0.3.1 → v0.3.2；nns 地板 v0.3.4 → v0.3.5

## [v0.1.6] — 2026-10-17

- concurrency 地板 v0.5.2 → v0.5.4（indirect 归位）

## [v0.1.5] — 2026-10-17

- fix(deps): 内部依赖地板全量对齐最新——httpx v0.3.1、logx v0.5.0、metrics v0.1.6、nns v0.3.4、registry v0.3.1；go 维持 1.26.0（httpx/tracing 生态传导）

## [v0.1.4] — 2026-10-17

- httpx 结构重置对齐（v0.3.0 起包提升至模块根）：`errors.go`/`http.go` 的 import 由旧路径 `github.com/oekit/httpx/httpx` 改为模块根；依赖地板 httpx v0.1.5 → v0.3.0（旧版本 tag 已随结构重置清空）

## [v0.1.0] — 2026-10-15

自 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来的初始公开版本，完成拆分稳定化：模块路径统一为 `github.com/oekit/nns-endpoint`，多轮拆分/合并收敛至当前模块边界，组织基建（共享 CI workflow、golangci v2、Dependabot）就位。