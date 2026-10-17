package endpoint

import (
	"time"

	"github.com/oekit/httpx/loadbalance"
	"github.com/oekit/metrics/resilience"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/sony/gobreaker"
)

// defaultBreakerSettings 是 per-service 熔断器的默认参数
// （v0.30.0 前为硬编码，此后作为 WithBreakerSettings 未覆盖项的缺省值）。
func defaultBreakerSettings() gobreaker.Settings {
	return gobreaker.Settings{
		MaxRequests: 3,
		Interval:    10 * time.Second,
		Timeout:     30 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures > 5
		},
	}
}

// factoryConfig 是端点工厂的可配置项。
type factoryConfig struct {
	breakerTmpl gobreaker.Settings
	metrics     *resilience.Metrics
	// selector 非空时全服务共用该节点选择器（WithSelector 注入），
	// 覆盖服务治理配置中的 lb 策略。
	selector loadbalance.Selector
}

// EndpointFactoryOption 是端点工厂（HTTP/gRPC）的配置函数类型。
type EndpointFactoryOption func(*factoryConfig)

// WithBreakerSettings 设置 per-service 熔断器的参数模板。
//
// 数值字段（MaxRequests/Interval/Timeout）整体取模板值——0 是 gobreaker
// 的合法显式语义（如 Interval 0 = 计数器跨窗口累计），不做零值回填；
// 函数字段（ReadyToTrip/OnStateChange/IsSuccessful）为 nil 时沿用工厂默认。
//
// 未传 IsSuccessful 时默认判定为"4xx 不计熔断失败"（调用方请求错误与
// 对端健康无关）；显式传入则完全接管失败判定。
func WithBreakerSettings(tmpl gobreaker.Settings) EndpointFactoryOption {
	return func(c *factoryConfig) {
		c.breakerTmpl = tmpl
	}
}

// WithSelector 注入节点选择器（loadbalance.Selector），覆盖服务治理配置
// 中的 lb 策略；nil 被忽略。未注入时按服务配置的 Lb 字段经
// loadbalance.NewSelector 构造（不可识别回退 roundrobin）。
func WithSelector(sel loadbalance.Selector) EndpointFactoryOption {
	return func(c *factoryConfig) {
		if sel != nil {
			c.selector = sel
		}
	}
}

// WithMetrics 启用弹性指标（熔断状态/迁移/拒绝 + 重试尝试），
// 形态对齐 httpx.WithMetrics。reg 为 nil 时用全局 DefaultRegisterer；
// namespace 为空时用 "nns_endpoint"。多个工厂共享同一 Registry +
// namespace 时复用已注册 collector。
func WithMetrics(reg prom.Registerer, namespace string) EndpointFactoryOption {
	return func(c *factoryConfig) {
		if namespace == "" {
			namespace = "nns_endpoint"
		}
		c.metrics = resilience.NewMetrics(reg, namespace)
	}
}

// breakerTemplate 聚合"服务名 → Settings"构造器与工厂级共享态（弹性指标）。
type breakerTemplate struct {
	settingsFor func(serviceName string) gobreaker.Settings
	metrics     *resilience.Metrics
}

// newFactoryConfig 合并默认参数与用户选项。
func newFactoryConfig(opts []EndpointFactoryOption) *factoryConfig {
	cfg := &factoryConfig{breakerTmpl: defaultBreakerSettings()}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// newBreakerTemplate 从 factoryConfig 构造带指标的熔断模板。
// Name 由工厂覆盖为 "kind:service"，保证熔断器实例可按名称观测。
func newBreakerTemplate(kind string, cfg *factoryConfig) breakerTemplate {
	def := defaultBreakerSettings()
	tmpl := cfg.breakerTmpl
	metrics := cfg.metrics

	build := func(serviceName string) gobreaker.Settings {
		s := def
		s.Name = kind + ":" + serviceName
		s.MaxRequests = tmpl.MaxRequests
		s.Interval = tmpl.Interval
		s.Timeout = tmpl.Timeout
		if tmpl.ReadyToTrip != nil {
			s.ReadyToTrip = tmpl.ReadyToTrip
		}
		userOnStateChange := tmpl.OnStateChange
		if metrics != nil {
			// 新建熔断器初始态必为 closed；显式置 0 让 gauge series 从创建即存在
			metrics.ObserveBreakerState(serviceName, kind, resilience.StateClosed)
		}
		// 状态迁移始终留痕（logx 结构化），与 httpx.WithCircuitBreaker 的默认行为对齐
		s.OnStateChange = func(name string, from, to gobreaker.State) {
			epLog.Warn("circuit breaker state change", "breaker", name, "from", from, "to", to)
			if metrics != nil {
				metrics.ObserveTransition(serviceName, kind, int(from), int(to))
			}
			if userOnStateChange != nil {
				userOnStateChange(name, from, to)
			}
		}
		if tmpl.IsSuccessful != nil {
			s.IsSuccessful = tmpl.IsSuccessful
		} else {
			s.IsSuccessful = breakerSuccessful
		}
		return s
	}
	return breakerTemplate{settingsFor: build, metrics: metrics}
}
