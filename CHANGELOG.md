# Changelog — oekit/nns-endpoint

由 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来。所有 notable 变更记录如下。

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