package endpoint

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/oekit/httpx/loadbalance"
	nnsclient "github.com/oekit/nns/client"
	"golang.org/x/sync/singleflight"
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
	// firstBuild 合并并发首建（#136）：快路径未命中后，同一服务的并发
	// 首建只执行一次配置拉取 + selector 构造，其余等待方共享结果——
	// 此前 N 个并发首调各拉一次配置、各建一个 selector，仅靠写锁
	// double-check 留下最后一个，其余白白丢弃。
	firstBuild singleflight.Group
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
//
// 配置读取（#98）：首建用 ServiceConfig（真实错误面）而非
// ServiceWithName（软降级）——瞬时配置拉取失败若静默取默认值，产出
// roundrobin selector 进缓存后没有失败豁免（无 TTL、无重试标记），只有
// 配置变更事件或 ResetSelector 会重建，配置的 p2c 等自适应策略在热更前
// 永不生效。失败时不写缓存直接报错，下次调用重试首建。
// 仅两类情况允许稳定回退 roundrobin 并写缓存：
//   - 配置真实存在但策略名不可识别（确定性决策，回退安全）；
//   - 未接入配置中心（ErrConfigStoreUnavailable，本就无配置可拉，
//     与 ServiceWithName 的旧行为一致）。
//
// 并发首建（#136）：慢路径经 singleflight 按服务名合并——同一服务的
// 并发首调只执行一次配置拉取与构造，等待方共享同一结果（含错误）；成功
// 后仍走写锁 double-check 写入，与既有缓存语义一致。
func (c *selectorCache) getOrCreate(ctx context.Context, serviceName string) (loadbalance.Selector, error) {
	// 快速路径：读锁查缓存
	c.mu.RLock()
	if s, ok := c.selectors[serviceName]; ok {
		c.mu.RUnlock()
		return s, nil
	}
	c.mu.RUnlock()

	// 注入式 selector 无 IO：直接构造（无并发拉配置问题，#136 不涉及）
	if c.injected != nil {
		return c.writeThrough(serviceName, c.injected), nil
	}

	// 慢路径：singleflight 合并并发首建，锁外执行 IO。缓存写入在 flight
	// 内完成（fn 返回前缓存已就位）：flight key 删除后到达的调用必然走
	// 快路径命中，不存在「flight 已结、缓存未写」的再执行窗口。
	v, err, _ := c.firstBuild.Do(serviceName, func() (any, error) {
		created, err := c.buildSelector(ctx, serviceName)
		if err != nil {
			return nil, err
		}
		return c.writeThrough(serviceName, created), nil
	})
	if err != nil {
		return nil, err
	}
	return v.(loadbalance.Selector), nil
}

// buildSelector 拉取服务配置并构造节点选择器（锁外 IO，#98 错误面语义）。
func (c *selectorCache) buildSelector(ctx context.Context, serviceName string) (loadbalance.Selector, error) {
	basic, err := c.client.ServiceConfig(ctx, serviceName)
	if err != nil && !errors.Is(err, nnsclient.ErrConfigStoreUnavailable) {
		// 配置拉取失败/JSON 损坏：不写缓存，本次调用显式失败，
		// 下次调用重试首建（配置恢复后 p2c 等策略即正常生效）
		return nil, fmt.Errorf("get service config: %w", err)
	}
	created, err := loadbalance.NewSelector(loadbalance.NormalizeSelectorName(basic.Lb))
	if err != nil {
		epLog.Warn("unsupported lb strategy, fallback to round-robin", "strategy", basic.Lb, "service", serviceName)
		created, err = loadbalance.NewSelector(loadbalance.StrategyRoundRobin)
		if err != nil {
			return nil, err
		}
	}
	return created, nil
}

// writeThrough 双检写入：缓存已有现役条目（他方并发首建/注入路径已写）
// 则复用之，否则写入并返回。
func (c *selectorCache) writeThrough(serviceName string, s loadbalance.Selector) loadbalance.Selector {
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.selectors[serviceName]; ok {
		return existing // 其他 goroutine 已创建
	}
	c.selectors[serviceName] = s
	return s
}

// reset 重置指定服务的选择器缓存。
func (c *selectorCache) reset(serviceName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.selectors, serviceName)
}
