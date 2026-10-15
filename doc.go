// Package endpoint 定义了统一的服务调用端点抽象，封装负载均衡、熔断和连接池等基础设施。
//
// 核心类型 Endpoint 是一个函数签名，接收 context 和请求并返回响应，使上层逻辑与具体协议解耦。
// HTTPEndpointFactory 和 GRPCEndpointFactory 分别为 HTTP 和 gRPC 协议创建 Endpoint，
// 内部集成负载均衡选择实例、gRPC 连接池复用连接、以及基于 gobreaker 的断路器保护。
package endpoint
