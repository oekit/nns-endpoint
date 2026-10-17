# Changelog — oekit/nns-endpoint

由 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来。所有 notable 变更记录如下。

## [v0.1.6] — 2026-10-17

- concurrency 地板 v0.5.2 → v0.5.4（indirect 归位）

## [v0.1.5] — 2026-10-17

- fix(deps): 内部依赖地板全量对齐最新——httpx v0.3.1、logx v0.5.0、metrics v0.1.6、nns v0.3.4、registry v0.3.1；go 维持 1.26.0（httpx/tracing 生态传导）

## [v0.1.4] — 2026-10-17

- httpx 结构重置对齐（v0.3.0 起包提升至模块根）：`errors.go`/`http.go` 的 import 由旧路径 `github.com/oekit/httpx/httpx` 改为模块根；依赖地板 httpx v0.1.5 → v0.3.0（旧版本 tag 已随结构重置清空）

## [v0.1.0] — 2026-10-15

自 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来的初始公开版本，完成拆分稳定化：模块路径统一为 `github.com/oekit/nns-endpoint`，多轮拆分/合并收敛至当前模块边界，组织基建（共享 CI workflow、golangci v2、Dependabot）就位。