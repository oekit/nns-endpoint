# nns-endpoint

> NNS 端点工厂——per-service 熔断（4xx 不计失败）、HTTP 换实例重试、gRPC 连接池、弹性指标

> 项目主页：**https://oekit.github.io/** · 名称由来：**O**pen **E**asy **Kit**（开源 · 易用 · 工具集合）
> 🤖 AI Agent：本仓规范与硬规则见 [AGENTS.md](AGENTS.md)；机器可读索引见 [llms.txt](https://oekit.github.io/llms.txt)（接口签名以源码为准）。

本仓库是 [ekit](https://git.enjoye.top/enjoydream/ekit) 拆分矩阵的成员（Round-4 微库化）。

## 安装

```bash
go get github.com/oekit/nns-endpoint@v0.1.2
```

## 快速使用

```go
import endpoint "github.com/oekit/nns-endpoint"

factory := endpoint.NewHTTPEndpointFactory(nnsc.ServiceClient())
ep, _ := factory.MakeEndpoint("payment-svc")
resp, err := ep(ctx, req) // 背后：服务发现 + per-service 熔断 + 换实例重试
```

## 包清单

- 根包 `endpoint` — HTTPEndpointFactory/GRPCEndpointFactory，经 `nns.ServiceClient()` 构造（Round-4 自 ekit-nns 分家）

依赖：[ekit-nns](https://github.com/oekit/nns)（ServiceClient）、[ekit-httpx](https://github.com/oekit/httpx)、[ekit-metrics](https://github.com/oekit/metrics)、[ekit-logx](https://github.com/oekit/logx)、[ekit-registry](https://github.com/oekit/registry)、[ekit-concurrency](https://github.com/oekit/concurrency)。

原 `nns.MakeHTTPEndpoint/MakeGRPCEndpoint` 门面 API 随本仓迁移：改用 `endpoint.NewHTTPEndpointFactory(nnsClient.ServiceClient())`。

## License

见 [LICENSE](LICENSE)
