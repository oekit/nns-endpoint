package endpoint

import (
	"sync"

	"github.com/sony/gobreaker"
)

// breakerCache 管理服务名到熔断器的映射。
// 熔断器必须按服务隔离：若全工厂共享一个熔断器，单个依赖的连续失败
// 会把其他健康依赖一并熔断（跨服务故障放大）。
type breakerCache struct {
	mu       sync.Mutex
	template func(serviceName string) gobreaker.Settings
	breakers map[string]*gobreaker.CircuitBreaker
}

func newBreakerCache(template func(serviceName string) gobreaker.Settings) *breakerCache {
	return &breakerCache{template: template, breakers: make(map[string]*gobreaker.CircuitBreaker)}
}

// getOrCreate 获取或创建指定服务的熔断器。
// 参数在工厂构造时经 WithBreakerSettings 定制，默认值见 defaultBreakerSettings。
func (c *breakerCache) getOrCreate(serviceName string) *gobreaker.CircuitBreaker {
	c.mu.Lock()
	defer c.mu.Unlock()
	if b, ok := c.breakers[serviceName]; ok {
		return b
	}
	b := gobreaker.NewCircuitBreaker(c.template(serviceName))
	c.breakers[serviceName] = b
	return b
}
