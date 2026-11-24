package endpoint

import (
	"context"
	"errors"
	"strconv"
	"sync"

	"github.com/oekit/concurrency/singleflight"
	"github.com/oekit/httpx/loadbalance"
	nnsclient "github.com/oekit/nns/client"
	pkgerrors "github.com/oekit/pkg/errors"
)

// selectorCache 管理服务名到节点选择器的映射（v0.2.0 起替代 balancerCache）。
// 提取自 HTTPEndpointFactory/GRPCEndpointFactory 的公共逻辑，
// 避免重复代码，且修复锁跨 IO 问题。
type selectorCache struct {
	mu        sync.RWMutex
	selectors map[string]loadbalance.Selector
	// epochs 记录每个服务的缓存代数（#231）：reset 递增，slow-path 首建在
	// 开始时捕获代数、writeThrough 仅在代数未变时写入——配置变更落在
	// 「首建开始后、writeThrough 前」时，按旧配置构建的 selector 不会被写回
	// 空缓存（此前普通双检写回会把热更新静默丢失，且无 TTL 纠正）。
	epochs map[string]uint64
	client *nnsclient.ServiceClient
	// injected 非空时忽略服务配置，全服务共用该选择器（WithSelector 注入）。
	injected loadbalance.Selector
	// firstBuild 合并并发首建（#136）：快路径未命中后，同一服务的并发
	// 首建只执行一次配置拉取 + selector 构造，其余等待方共享结果——
	// 此前 N 个并发首调各拉一次配置、各建一个 selector，仅靠写锁
	// double-check 留下最后一个，其余白白丢弃。
	// #204：golang.org/x/sync/singleflight 迁移到矩阵单源
	// concurrency/singleflight.Flight——ctx 显式透传（fn 收到的 ctx 即
	// 调用方请求 ctx，慢路径跟随请求超时与取消；等待方自己的 ctx 先
	// 取消时仅该等待方退出，不影响 leader），与 getOrCreate 的
	// 「慢路径必须跟随请求超时」契约（下述 godoc）配套。
	firstBuild singleflight.Flight[loadbalance.Selector]
}

func newSelectorCache(client *nnsclient.ServiceClient, injected loadbalance.Selector) *selectorCache {
	return &selectorCache{
		selectors: make(map[string]loadbalance.Selector),
		epochs:    make(map[string]uint64),
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
	// 快速路径：读锁查缓存；未命中时同一临界区内捕获代数（#231），
	// 供 slow-path 的 writeThrough 判定「构建期间是否发生过 reset」。
	c.mu.RLock()
	if s, ok := c.selectors[serviceName]; ok {
		c.mu.RUnlock()
		return s, nil
	}
	epoch := c.epochs[serviceName]
	c.mu.RUnlock()

	// 注入式 selector 无 IO：直接构造（无并发拉配置问题，#136 不涉及）
	if c.injected != nil {
		return c.writeThrough(serviceName, c.injected, epoch), nil
	}

	// 慢路径：singleflight 合并并发首建，锁外执行 IO。缓存写入在 flight
	// 内完成（fn 返回前缓存已就位）：flight key 删除后到达的调用必然走
	// 快路径命中，不存在「flight 已结、缓存未写」的再执行窗口。
	// #204：Flight 泛型形态免类型断言；ctx 显式透传给 fn（leader 以
	// 首个调用方的请求 ctx 执行配置拉取）。
	// #231：flight key 带代数——reset 之后到达的调用不再 join reset 前
	// 已在飞的旧配置构建（否则调用方仍会拿到旧配置 selector），而是另起
	// 一次按新配置的构建；旧 flight 的结果只交给 reset 前已在等它的调用方，
	// 且其 writeThrough 因代数不匹配被拒写。
	v, err := c.firstBuild.Do(ctx, flightKey(serviceName, epoch), func(ctx context.Context) (loadbalance.Selector, error) {
		// 二次快路径（#136/#231）：本调用方在快路径未命中与进入 flight
		// 之间，前一轮 flight 可能已完成并写好缓存——flight 内先复查，
		// 命中则直接复用，避免同一服务多拉一次配置（并发首建计数仍为 1）。
		c.mu.RLock()
		if s, ok := c.selectors[serviceName]; ok {
			c.mu.RUnlock()
			return s, nil
		}
		c.mu.RUnlock()

		created, err := c.buildSelector(ctx, serviceName)
		if err != nil {
			return nil, err
		}
		return c.writeThrough(serviceName, created, epoch), nil
	})
	if err != nil {
		return nil, err
	}
	return v, nil
}

// flightKey 构造 singleflight key：服务名 + 缓存代数（#231），使 reset 前后
// 的首建互不合并。
func flightKey(serviceName string, epoch uint64) string {
	return serviceName + "#" + strconv.FormatUint(epoch, 10)
}

// buildSelector 拉取服务配置并构造节点选择器（锁外 IO，#98 错误面语义）。
func (c *selectorCache) buildSelector(ctx context.Context, serviceName string) (loadbalance.Selector, error) {
	basic, err := c.client.ServiceConfig(ctx, serviceName)
	if err != nil && !errors.Is(err, nnsclient.ErrConfigStoreUnavailable) {
		// 配置拉取失败/JSON 损坏：不写缓存，本次调用显式失败，
		// 下次调用重试首建（配置恢复后 p2c 等策略即正常生效）
		return nil, pkgerrors.Wrapf(err, "get service config")
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
// 则复用之；缓存为空时仅在**代数未变**（构建开始后没有 reset）时写入并
// 返回，否则返回本次构建的 selector 但不缓存（#231）——reset 意味着「按
// 旧配置构建的结果已失效」，写回等于把热更新静默丢失。调用方本次仍使用
// 自己已构建的 selector（无法凭空得到新配置），下次调用按新代数重建。
func (c *selectorCache) writeThrough(serviceName string, s loadbalance.Selector, epoch uint64) loadbalance.Selector {
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.selectors[serviceName]; ok {
		return existing // 其他 goroutine 已创建
	}
	if c.epochs[serviceName] != epoch {
		return s // 构建期间发生 reset：拒绝缓存旧配置产物
	}
	c.selectors[serviceName] = s
	return s
}

// reset 重置指定服务的选择器缓存：删除条目并递增代数（#231）。代数递增
// 使 reset 前已在飞的首建无法再写回缓存（writeThrough 代数校验），
// reset 后到达的调用也不会 join 旧 flight（flightKey 带代数）。
func (c *selectorCache) reset(serviceName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.epochs[serviceName]++
	delete(c.selectors, serviceName)
}
