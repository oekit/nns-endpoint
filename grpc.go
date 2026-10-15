package endpoint

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	nnsclient "github.com/yi-nology/ekit-nns/nns/client"

	"github.com/yi-nology/ekit-metrics/resilience"

	"github.com/sony/gobreaker"
	"github.com/yi-nology/ekit-httpx/loadbalance"
	"github.com/yi-nology/ekit-registry/registry"
	"google.golang.org/grpc"
)

// GRPCRequest gRPC 端点请求参数。
type GRPCRequest struct {
	ServiceName     string
	GrpcServiceName string
	Method          string
	Request         any
	Timeout         time.Duration
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
	balancerCache *balancerCache
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
	tmpl := newBreakerTemplate(grpcMetricsKind, opts)
	f := &GRPCEndpointFactory{
		client:        c,
		pool:          NewGRPCPool(),
		balancerCache: newBalancerCache(c),
		breakers:      newBreakerCache(tmpl.settingsFor),
		metrics:       tmpl.metrics,
	}

	// 配置变更时重置 balancer 缓存
	c.OnConfigChange(func(serviceName string) {
		f.balancerCache.reset(serviceName)
	})

	return f
}

// ResetBalancer 清除指定服务的负载均衡器缓存，下次调用时按最新配置重建。
func (f *GRPCEndpointFactory) ResetBalancer(serviceName string) {
	f.balancerCache.reset(serviceName)
}

// MakeEndpoint 为指定服务创建 gRPC 端点，包含负载均衡、连接池和熔断逻辑。
// balancer 与熔断器在闭包内按调用获取，ResetBalancer/配置热更对存量端点生效。
func (f *GRPCEndpointFactory) MakeEndpoint(serviceName string, opts ...grpc.DialOption) (Endpoint, error) {
	return func(ctx context.Context, request any) (any, error) {
		req, ok := request.(GRPCRequest)
		if !ok {
			return nil, fmt.Errorf("invalid request type, expected GRPCRequest")
		}

		instances, err := f.client.GetInstances(ctx, serviceName)
		if err != nil {
			return nil, fmt.Errorf("get instances: %w", err)
		}

		if len(instances) == 0 {
			return nil, registry.ErrNoInstances
		}

		idx, err := f.balancerCache.getOrCreate(ctx, serviceName).Pick(loadbalance.ToInstances(instances))
		if err != nil {
			return nil, err
		}

		instance := instances[idx]

		addr := net.JoinHostPort(instance.IP, strconv.Itoa(instance.Port))

		conn, err := f.pool.Get(ctx, addr, opts...)
		if err != nil {
			return nil, fmt.Errorf("get grpc conn: %w", err)
		}

		timeout := req.Timeout
		if timeout == 0 {
			basic := f.client.GetServiceBasic(serviceName)
			timeout = time.Duration(basic.ReadTimeout) * time.Millisecond
		}

		callCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		fullMethod := fmt.Sprintf("/%s/%s", req.GrpcServiceName, req.Method)

		result, err := f.breakers.getOrCreate(serviceName).Execute(func() (any, error) {
			var reply any
			invokeErr := conn.Invoke(callCtx, fullMethod, req.Request, &reply)
			return reply, invokeErr
		})

		if err != nil {
			if errors.Is(err, gobreaker.ErrOpenState) {
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
