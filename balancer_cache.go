package endpoint

import (
	"context"
	"sync"

	"github.com/yi-nology/ekit-httpx/loadbalance"
	nnsclient "github.com/yi-nology/ekit-nns/nns/client"
)

// balancerCache 管理服务名到负载均衡器的映射。
// 提取自 HTTPEndpointFactory/GRPCEndpointFactory 的公共逻辑，
// 避免重复代码，且修复锁跨 IO 问题。
type balancerCache struct {
	mu        sync.RWMutex
	balancers map[string]loadbalance.Balancer
	client    *nnsclient.ServiceClient
}

func newBalancerCache(client *nnsclient.ServiceClient) *balancerCache {
	return &balancerCache{
		balancers: make(map[string]loadbalance.Balancer),
		client:    client,
	}
}

// getOrCreate 获取或创建指定服务的负载均衡器。
// 快速路径（读锁命中缓存）不涉及 IO；慢路径（创建新 balancer）
// 在锁外执行网络请求，仅在写入缓存时持写锁。
//
// ctx 透传给首建的 Nacos 配置查询：慢路径发生在请求路径上
// （MakeEndpoint 闭包首建/热更重置后首次调用），必须跟随请求的
// 超时与取消，避免挂死的配置拉取阻塞调用方请求。
func (c *balancerCache) getOrCreate(ctx context.Context, serviceName string) loadbalance.Balancer {
	// 快速路径：读锁查缓存
	c.mu.RLock()
	if b, ok := c.balancers[serviceName]; ok {
		c.mu.RUnlock()
		return b
	}
	c.mu.RUnlock()

	// 慢路径：锁外执行 IO
	basic := c.client.ServiceWithName(ctx, serviceName)
	b, err := loadbalance.New(basic.Lb)
	if err != nil {
		epLog.Warn("unsupported lb strategy, fallback to round-robin", "strategy", basic.Lb, "service", serviceName)
		b, _ = loadbalance.New("") // fallback to default
	}

	// Double-check 写入
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.balancers[serviceName]; ok {
		return existing // 其他 goroutine 已创建
	}
	c.balancers[serviceName] = b
	return b
}

// reset 重置指定服务的负载均衡器缓存。
func (c *balancerCache) reset(serviceName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.balancers, serviceName)
}
