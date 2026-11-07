package endpoint

import (
	"context"
	"errors"
	"fmt"
	"time"

	nnsclient "github.com/oekit/nns/client"

	"github.com/oekit/metrics/resilience"

	"github.com/oekit/httpx/loadbalance"
	"github.com/oekit/registry"
	"github.com/sony/gobreaker"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// GRPCRequest gRPC 端点请求参数。
type GRPCRequest struct {
	ServiceName     string
	GrpcServiceName string
	Method          string
	// Request 是请求消息，必须实现 proto.Message（默认 proto codec 只
	// 接受该类型）。不满足时端点在发起调用前直接报错，而不是让编解码
	// 失败发生在 Invoke 深处、错误形态晦涩。
	Request any
	// Reply 是接收响应的类型化消息指针，由调用方传入（如
	// &helloworld.HelloReply{}），端点的返回值就是它。必填。
	//
	// #71/#73：此前解码目标是 var reply any（&reply 是 *any），默认
	// proto codec 的 Unmarshal 要求 proto.Message，类型化响应永远拿
	// 不到——gRPC happy path 从未跑通过。
	Reply proto.Message
	// Timeout 是单次调用超时；0 时回退服务治理 ReadTimeout，两级均
	// 未配置时兜底 defaultCallTimeout（#75）。
	Timeout time.Duration
}

// GRPCEndpointFactory gRPC 端点工厂，负责创建 gRPC 端点并管理连接池和熔断器。
//
// balancer 与熔断器均按服务名分片：balancer 缓存避免有状态策略（轮询）因
// 计数器重置失效；熔断器按服务隔离，避免单个依赖故障熔断全工厂。
// balancer/熔断器均在端点闭包内按调用获取（balancerCache 读锁命中，开销可忽略），
// 配置热更新（ResetBalancer）对已创建的端点同样生效。服务配置（超时等）在每次
// 调用时从内存读取，支持 WatchService 热更新。
type GRPCEndpointFactory struct {
	client        *nnsclient.ServiceClient
	pool          *GRPCPool
	selectorCache *selectorCache
	breakers      *breakerCache
	metrics       *resilience.Metrics
}

// grpcMetricsKind 是本工厂在弹性指标里的 kind 标签值。
const grpcMetricsKind = "nns-grpc"

// NewGRPCEndpointFactory 根据服务客户端创建 gRPC 端点工厂实例。
// opts 定制 per-service 熔断参数（WithBreakerSettings）等行为。
// 注意：gRPC 端点不做换实例重试——streaming/非幂等 RPC 的重试语义
// 复杂（需 header/ trailers 透传配合），如需重试在业务层按方法粒度实现。
func NewGRPCEndpointFactory(c *nnsclient.ServiceClient, opts ...EndpointFactoryOption) *GRPCEndpointFactory {
	cfg := newFactoryConfig(opts)
	tmpl := newBreakerTemplate(grpcMetricsKind, cfg)
	f := &GRPCEndpointFactory{
		client:        c,
		pool:          NewGRPCPool(),
		selectorCache: newSelectorCache(c, cfg.selector),
		breakers:      newBreakerCache(tmpl.settingsFor),
		metrics:       tmpl.metrics,
	}

	// 配置变更时重置选择器缓存
	c.OnConfigChange(func(serviceName string) {
		f.selectorCache.reset(serviceName)
	})

	return f
}

// ResetSelector 清除指定服务的节点选择器缓存，下次调用时按最新配置重建。
func (f *GRPCEndpointFactory) ResetSelector(serviceName string) {
	f.selectorCache.reset(serviceName)
}

// MakeEndpoint 为指定服务创建 gRPC 端点，包含负载均衡、连接池和熔断逻辑。
// balancer 与熔断器在闭包内按调用获取，ResetBalancer/配置热更对存量端点生效。
//
// 传输安全（#77）：opts 必须显式包含传输凭据——明文传
// grpc.WithTransportCredentials(insecure.NewCredentials())，TLS 传对应
// TransportCredentials。连接池不再默认注入 insecure 凭据，缺凭据的
// 调用会得到 grpc 的 "no transport security set" 错误（此前是静默明文）。
func (f *GRPCEndpointFactory) MakeEndpoint(serviceName string, opts ...grpc.DialOption) (Endpoint, error) {
	return func(ctx context.Context, request any) (any, error) {
		req, ok := request.(GRPCRequest)
		if !ok {
			return nil, fmt.Errorf("invalid request type, expected GRPCRequest")
		}

		// #71/#73：默认 proto codec 只接受 proto.Message。请求/响应的
		// 类型约束在发起调用前校验，缺 Reply 或 Request 非 proto 消息
		// 直接报配置性错误，而不是把裸 *any 塞给 Invoke 在 codec 深处
		// 报晦涩的编解码错误（响应此前永远拿不到）。
		if req.Reply == nil {
			return nil, fmt.Errorf("GRPCRequest.Reply must be a non-nil proto.Message pointer (e.g. &helloworld.HelloReply{}) to receive the response")
		}
		if _, ok := req.Request.(proto.Message); !ok {
			return nil, fmt.Errorf("invalid request type %T: gRPC request must implement proto.Message", req.Request)
		}

		instances, err := f.client.GetInstances(ctx, serviceName)
		if err != nil {
			return nil, fmt.Errorf("get instances: %w", err)
		}

		if len(instances) == 0 {
			return nil, registry.ErrNoInstances
		}

		sel, err := f.selectorCache.getOrCreate(ctx, serviceName)
		if err != nil {
			return nil, err
		}
		nodes := loadbalance.NodesFrom(instances,
			func(i registry.Instance) string { return i.Addr() },
			func(i registry.Instance) float64 { return i.Weight },
			func(i registry.Instance) map[string]string { return i.Metadata },
			func(i registry.Instance) string { return i.Service },
		)
		n, done, err := sel.Select(ctx, nodes)
		if err != nil {
			return nil, err
		}

		conn, err := f.pool.Get(ctx, n.Address(), opts...)
		if err != nil {
			done(ctx, loadbalance.DoneInfo{Err: err})
			return nil, fmt.Errorf("get grpc conn: %w", err)
		}

		// #75：两级超时均缺配（0）时兜底 defaultCallTimeout，而非
		// WithTimeout(ctx, 0) 的立即 DeadlineExceeded
		timeout := resolveCallTimeout(serviceName, req.Timeout, f.client.GetServiceBasic(serviceName).ReadTimeout)

		callCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		fullMethod := fmt.Sprintf("/%s/%s", req.GrpcServiceName, req.Method)

		result, err := f.breakers.getOrCreate(serviceName).Execute(func() (any, error) {
			// 解码目标是调用方传入的类型化 Reply 指针（#71/#73）
			invokeErr := conn.Invoke(callCtx, fullMethod, req.Request, req.Reply)
			return req.Reply, invokeErr
		})

		// done 回写与 http.go 同构（#57）：本次调用的最终结果确定后回调（含
		// 熔断出口）——此前只在 pool.Get 失败路径回调一次，Invoke 结果从不
		// 回写 ⇒ p2c 的节点级 EWMA 恒 0，故障/慢节点永不降权。
		done(ctx, loadbalance.DoneInfo{Err: err})

		if err != nil {
			// #58：half-open 态并发超过 MaxRequests 时 gobreaker 返回的是
			// ErrTooManyRequests（非 ErrOpenState）——同样是熔断拒绝，一并映射
			// ErrCircuitOpen + 计 ObserveRejected（此前裸透出，消费方的
			// errors.Is(err, ErrCircuitOpen) 判定失效）。
			if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
				if f.metrics != nil {
					f.metrics.ObserveRejected(serviceName, grpcMetricsKind)
				}
				return nil, fmt.Errorf("%w: service=%s", ErrCircuitOpen, serviceName)
			}
			return nil, err
		}

		return result, nil
	}, nil
}

// Close 关闭 gRPC 端点工厂持有的连接池资源。
func (f *GRPCEndpointFactory) Close() {
	f.pool.Close()
}
