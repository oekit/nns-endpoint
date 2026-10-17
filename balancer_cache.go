package endpoint

import (
	"context"
	"sync"

	"github.com/oekit/httpx/loadbalance"
	nnsclient "github.com/oekit/nns/client"
)

// selectorCache 管理服务名到节点选择器的映射（v0.2.0 起替代 balancerCache）。
// 提取自 HTTPEndpointFactory/GRPCEndpointFactory 的公共逻辑，
// 避免重复代码，且修复锁跨 IO 问题。
type selectorCache struct {
	mu        sync.RWMutex
	selectors map[string]loadbalance.Selector
	client    *nnsclient.ServiceClient
	// injected 非空时忽略服务配置，全服务共用该选择器（WithSelector 注入）。
	injected loadbalance.Selector
}

func newSelectorCache(client *nnsclient.ServiceClient, injected loadbalance.Selector) *selectorCache {
	return &selectorCache{
		selectors: make(map[string]loadbalance.Selector),
		client:    client,
		injected:  injected,
	}
}

// getOrCreate 获取或创建指定服务的节点选择器。
// 快速路径（读锁命中缓存）不涉及 IO；慢路径（创建新 selector）
// 在锁外执行网络请求，仅在写入缓存时持写锁。
//
// ctx 透传给首建的 Nacos 配置查询：慢路径发生在请求路径上
// （MakeEndpoint 闭包首建/热更重置后首次调用），必须跟随请求的
// 超时与取消，避免挂死的配置拉取阻塞调用方请求。
// 服务配置的策略名不可识别时回退 roundrobin。
func (c *selectorCache) getOrCreate(ctx context.Context, serviceName string) (loadbalance.Selector, error) {
	// 快速路径：读锁查缓存
	c.mu.RLock()
	if s, ok := c.selectors[serviceName]; ok {
		c.mu.RUnlock()
		return s, nil
	}
	c.mu.RUnlock()

	// 慢路径：锁外执行 IO
	var s loadbalance.Selector
	if c.injected != nil {
		s = c.injected
	} else {
		basic := c.client.ServiceWithName(ctx, serviceName)
		created, err := loadbalance.NewSelector(loadbalance.NormalizeSelectorName(basic.Lb))
		if err != nil {
			epLog.Warn("unsupported lb strategy, fallback to round-robin", "strategy", basic.Lb, "service", serviceName)
			created, err = loadbalance.NewSelector(loadbalance.StrategyRoundRobin)
			if err != nil {
				return nil, err
			}
		}
		s = created
	}

	// Double-check 写入
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.selectors[serviceName]; ok {
		return existing, nil // 其他 goroutine 已创建
	}
	c.selectors[serviceName] = s
	return s, nil
}

// reset 重置指定服务的选择器缓存。
func (c *selectorCache) reset(serviceName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.selectors, serviceName)
}
