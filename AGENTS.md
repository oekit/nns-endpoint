# AGENTS.md — oekit/nns-endpoint

> 端点工厂——HTTPEndpointFactory/GRPCEndpointFactory：per-service 熔断、换实例重试、gRPC 池
> 当前版本：`v0.1.2` · module：`github.com/oekit/nns-endpoint` · org：[oekit = Open Easy Kit](https://oekit.github.io)

## 本仓包含
`./`

## 硬规则速览（全矩阵适用）

### 分层与依赖
- 依赖严格自底向上，禁止反向依赖；本仓的矩阵内依赖见 README「矩阵总览」表
- 微工具集统一在 `github.com/oekit/pkg`；本仓新增基础工具前先确认 pkg 里没有

### 错误处理
- 一律使用 `github.com/oekit/pkg/errors`：`errors.Wrap(err, "ctx", kv...)`（nil-safe）、
  `errors.Cause`；业务错误用 BizError（BizCode 与 HTTPCode 双轨，命名工厂 NewNotFoundError 等）
- **类型同一性红线**：同一进程内只允许一套 errors 包。混用旧路径会把 BizError 的 As/Is
  判定打断（Not Found 变 500）
- handler 层统一 `mw.Fail(ctx, c, err)` 出口；所有 error 禁止 `_` 忽略

### 日志
- 门面 `github.com/oekit/logx`：业务服务 `logx.WithService("svc")`；
  **SDK/基础包必须 `logx.Delegating("组件名")` 懒解析**（禁止 Default()/WithService 声明包级 logger）
- kv 结构化风格为默认；`Ctx*` 方法自动携带 trace_id；禁止 printf 风格 `logx.Infof` 作为主要手段
- 后端经 `logx.SetDefault` 替换；`LOG_LEVEL` 环境变量统一生效

### 并发安全
- 禁止裸 `go func`：用 `github.com/oekit/concurrency/async` 的 GoSafe 系列（panic 被 recover）
- 共享状态 mutex / atomic 保护；包级全局可变状态用 `atomic.Pointer[T]`

### 生命周期
- 组件 Close 统一 `Close(ctx) error` 签名 + sync.Once 幂等
- 应用骨架用 `github.com/oekit/bootstrap/bootstrap.Run`；**userFn 返回即服务结束，
  长驻服务必须 `<-app.Context().Done()` 阻塞**

### Option 与命名
- 函数选项模式 `XxxOption`（禁止裸 `Option`，多包 co-import 冲突）
- 导出符号 PascalCase + godoc 注释；包文档放 doc.go

### 版本与发版
- SemVer；0.x 阶段 minor 允许 breaking；发版 = 打 annotated tag 推远端（CI 自动跑 lint+test）
- module 路径 = `github.com/oekit/<name>`；**改 module 路径必须同步**：
  全部 import、go.mod、README 安装行、logx caller 启发式（若涉及 logx）

### 文档
- 本仓 README 的「矩阵总览」表由脚本生成，结构变化后需重新生成
- 历史完整规范见 ekit 单体仓（git.enjoye.top/enjoydream/ekit，冻结维护）的 AGENTS.md 与 docs/

## 本仓范围

