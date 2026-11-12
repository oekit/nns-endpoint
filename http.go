package endpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	nnsclient "github.com/oekit/nns/client"

	"github.com/oekit/metrics/resilience"

	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/oekit/httpx"
	"github.com/oekit/httpx/loadbalance"
	"github.com/oekit/logx"
	"github.com/oekit/registry"
)

var epLog = logx.Delegating("nns-endpoint")

// HTTPRequest HTTP 端点请求参数。
type HTTPRequest struct {
	Method  string
	Path    string
	Body    any
	Header  map[string]string
	Query   map[string]string
	Result  any
	Timeout time.Duration
}

// HTTPEndpointFactory HTTP 端点工厂，负责创建 HTTP 端点并管理 HTTP 客户端和熔断器。
type HTTPEndpointFactory struct {
	client        *nnsclient.ServiceClient
	http          *httpx.Client
	selectorCache *selectorCache
	breakers      *breakerCache
	metrics       *resilience.Metrics
	// configChangeHandle 是构造期在 client 上注册的配置变更回调句柄，
	// Close 时据此注销（#135）：不注销则 client 经回调闭包长持已废弃
	// 工厂（selectorCache/breakers 无法回收），配置事件继续打在死工厂上。
	configChangeHandle *nnsclient.ConfigChangeHandle
}

// metricsKind 是本工厂在弹性指标里的 kind 标签值。
const httpMetricsKind = "nns-http"

// defaultCallTimeout 是单次调用的兜底超时（#75）。
//
// 此前 req.Timeout 与服务治理 ServiceBasic.ReadTimeout 两级均为 0 时
// （治理配置漏配 readTimeout 很常见），得到 context.WithTimeout(ctx, 0)
// ——每个请求立即 DeadlineExceeded，错误形态酷似「后端超时」，排障方向
// 完全被带偏。取 30s，与 HTTP 工厂 httpx.WithTimeout(30s) 的客户端默认
// 对齐；命中兜底时记 Warn 提示补配。
const defaultCallTimeout = 30 * time.Second

// resolveCallTimeout 解析单次调用超时：显式 reqTimeout 优先，其次服务治理
// readTimeoutMS（毫秒）；两者均未配置（≤0）时回退 defaultCallTimeout。
// HTTP/gRPC 两条路径共用，保证兜底语义不漂移。
func resolveCallTimeout(serviceName string, reqTimeout time.Duration, readTimeoutMS int) time.Duration {
	timeout := reqTimeout
	if timeout == 0 {
		timeout = time.Duration(readTimeoutMS) * time.Millisecond
	}
	if timeout <= 0 {
		epLog.Warn("call timeout unresolved (req.Timeout=0 and ServiceBasic.ReadTimeout<=0), fallback to default",
			"service", serviceName, "fallback", defaultCallTimeout.String())
		return defaultCallTimeout
	}
	return timeout
}

// NewHTTPEndpointFactory 根据服务客户端创建 HTTP 端点工厂实例。
// opts 定制 per-service 熔断参数（WithBreakerSettings）、弹性指标
// （WithMetrics）等行为。
func NewHTTPEndpointFactory(c *nnsclient.ServiceClient, opts ...EndpointFactoryOption) *HTTPEndpointFactory {
	// httpx v0.8.12 起 NewClient 显式返回 error（httpx#83）；本工厂的
	// options 固定且不可能构造冲突（无 WithEndpoints/WithNNS 组合面），
	// 走 MustNewClient 逃生舱（panic 仅初始化期可达，语义同旧版）。
	httpClient := httpx.MustNewClient("nns-http",
		httpx.WithTimeout(30*time.Second),
		httpx.WithTracePropagation(),
	)
	cfg := newFactoryConfig(opts)
	tmpl := newBreakerTemplate(httpMetricsKind, cfg)

	f := &HTTPEndpointFactory{
		client:        c,
		http:          httpClient,
		selectorCache: newSelectorCache(c, cfg.selector),
		breakers:      newBreakerCache(tmpl.settingsFor),
		metrics:       tmpl.metrics,
	}

	// #135：注册即存句柄——工厂级恰好一次注册（构造期），Close 按句柄
	// 注销；OffConfigChange 幂等，重复 Close 安全。
	f.configChangeHandle = c.OnConfigChange(func(serviceName string) {
		f.selectorCache.reset(serviceName)
	})

	return f
}

// ResetSelector 清除指定服务的节点选择器缓存（配置热更后强制重建）。
func (f *HTTPEndpointFactory) ResetSelector(serviceName string) {
	f.selectorCache.reset(serviceName)
}

// MakeEndpoint 为指定服务创建 HTTP 端点。
//
// 重试语义（消费服务配置 ServiceBasic.Retry，Nacos 配置缺省 3）：
// 失败时换实例重试，最多额外 Retry 次尝试；5xx 与网络错误重试，
// 4xx 不重试（确定性请求错误）；本地确定性错误（序列化/解码/选点，
// #123）重试同一输入恒失败，同样不重试、不计熔断样本。熔断 Execute
// 包住整组尝试——一次调用计一次熔断样本（与 httpx.WithCircuitBreaker +
// WithRetryConfig 的层次结构一致），避免单次调用的多次尝试
// 撑爆连续失败计数。每次尝试独立 per-attempt 超时，最坏总耗时
// 为 (1+Retry) × ReadTimeout。
//
// 分层边界（#96/#98）：实例发现与 selector 首建在 Execute 之外——
// 「无实例」与「配置中心故障」都不是目标后端的故障，不构成熔断样本
// （与 gRPC 路径的分层一致）。
func (f *HTTPEndpointFactory) MakeEndpoint(serviceName string) (Endpoint, error) {
	return func(ctx context.Context, request any) (any, error) {
		req, ok := request.(HTTPRequest)
		if !ok {
			return nil, fmt.Errorf("invalid request type, expected HTTPRequest")
		}

		basic := f.client.GetServiceBasic(serviceName)
		retry := basic.Retry
		if retry < 0 {
			retry = 0
		}
		// #75：两级超时均缺配（0）时兜底 defaultCallTimeout，而非
		// WithTimeout(ctx, 0) 的立即 DeadlineExceeded
		timeout := resolveCallTimeout(serviceName, req.Timeout, basic.ReadTimeout)

		// #96：实例发现与 selector 首建在熔断 Execute 之外执行——熔断样本
		// 只反映「对被选节点的真实调用」。此前 ErrNoInstances（服务缩容到 0
		// 或注册中心短暂返回空）产生于 Execute 内部的 attemptOnce，无
		// statusError ⇒ 按网络错误计入熔断连续失败：第 7 次调用起被
		// ErrCircuitOpen 掩盖，调用方无法区分「无后端」与「熔断打开」，服务
		// 恢复出实例后最长 30s（breaker Timeout）内仍被拒。与 gRPC 路径
		// （发现/选点在熔断外，grpc.go）对齐。
		instances, err := f.client.GetInstances(ctx, serviceName)
		if err != nil {
			return nil, fmt.Errorf("get instances: %w", err)
		}
		if len(instances) == 0 {
			return nil, registry.ErrNoInstances
		}

		// #98：selector 首建遇配置拉取失败会显式报错（不写缓存、下次调用
		// 重试）——配置中心故障不属于目标服务的故障，同样不构成熔断样本，
		// 放在 Execute 之外。
		sel, err := f.selectorCache.getOrCreate(ctx, serviceName)
		if err != nil {
			return nil, err
		}
		nodes := httpx.SelectorNodes(instances)

		result, err := f.breakers.getOrCreate(serviceName).Execute(func() (any, error) {
			var lastErr error
			for attempt := 0; attempt <= retry; attempt++ {
				res, attemptErr := f.attemptOnce(ctx, sel, nodes, req, timeout)
				if attemptErr == nil {
					return res, nil
				}
				if !retryable(attemptErr) {
					return nil, attemptErr
				}
				lastErr = attemptErr
				if attempt < retry {
					if f.metrics != nil {
						f.metrics.ObserveRetry(serviceName, httpMetricsKind)
					}
					epLog.Debug("http endpoint retry on next instance",
						"service", serviceName, "attempt", attempt+1, logx.ErrKey, attemptErr)
				}
			}
			return nil, lastErr
		})
		if err != nil {
			// #58/#133：熔断拒绝映射收敛到 rejectIfCircuitOpen 单源——
			// ErrOpenState 与 ErrTooManyRequests（half-open 态并发超
			// MaxRequests）均映射 ErrCircuitOpen（含 ObserveRejected），
			// 非熔断拒绝原样透出
			if rejErr := rejectIfCircuitOpen(err, serviceName, httpMetricsKind, f.metrics); rejErr != nil {
				return nil, rejErr
			}
			return nil, err
		}
		return result, nil
	}, nil
}

// attemptOnce 执行一次"选实例 + 发请求"尝试。
// 实例列表与 selector 由调用方在熔断外一次性获取（#96/#98）；每次尝试
// 重新 Pick——selector 状态自然前进（RoundRobin 游标、p2c EWMA 随 done
// 回写演化），重试落在不同节点上才有意义（Random 可能撞同一节点）。
func (f *HTTPEndpointFactory) attemptOnce(ctx context.Context, sel loadbalance.Selector, nodes []loadbalance.Node, req HTTPRequest, timeout time.Duration) (any, error) {
	n, done, err := sel.Select(ctx, nodes)
	if err != nil {
		// #123：选点失败是本地确定性错误（候选集与过滤条件给定后重试
		// 同一输入恒失败，且无节点被选中、不构成后端故障样本）——包装为
		// localError：不换实例重试、不计熔断失败。
		return nil, localErrorf("select node: %w", err)
	}

	resp, callErr := f.doRequest(ctx, n.Address(), req, timeout)
	// #124：done 回写区分错误归属——调用方错误（HTTP 4xx / 本地确定性
	// 错误）归一为 nil 回写，p2c EWMA 不惩罚「碰巧被选中的节点」；
	// 节点/网络错误原样回写，故障/慢节点照常降权。与熔断样本
	// （breakerSuccessful）同一 callerFault 口径。
	done(ctx, loadbalance.DoneInfo{Err: nodeFaultErr(callErr)})
	return resp, callErr
}

// Close 关闭 HTTP 端点工厂持有的资源。
// Hertz client 连接池无显式释放 API（随进程生命周期），httpx.Client
// 的 endpointManager 在本工厂场景未启用；此处注销构造期在 client 上
// 注册的配置变更回调（#135）——否则 client 经回调闭包长持已废弃工厂，
// selectorCache/breakers 无法回收，配置事件继续打在死工厂上。
// OffConfigChange 幂等，Close 可安全重复调用。
func (f *HTTPEndpointFactory) Close() {
	f.client.OffConfigChange(f.configChangeHandle)
	f.configChangeHandle = nil
}

func (f *HTTPEndpointFactory) doRequest(ctx context.Context, addr string, req HTTPRequest, timeout time.Duration) (any, error) {
	// 只规整 addr 侧的尾斜杠用于拼接；TrimRight 整串曾把调用方声明的
	// path 尾斜杠剥掉（"/api/v1/" → "/api/v1"），隐式改写用户输入
	fullURL := strings.TrimRight(addr, "/") + req.Path

	epLog.Debug("http request", "method", req.Method, "url", fullURL)

	cancelCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if len(req.Query) > 0 {
		q := url.Values{}
		for k, v := range req.Query {
			q.Set(k, v)
		}
		sep := "?"
		if strings.Contains(fullURL, "?") {
			sep = "&"
		}
		fullURL += sep + q.Encode()
	}

	hReq, hResp := protocol.AcquireRequest(), protocol.AcquireResponse()
	defer protocol.ReleaseRequest(hReq)
	defer protocol.ReleaseResponse(hResp)

	hReq.SetMethod(req.Method)
	hReq.SetRequestURI(fullURL)
	for k, v := range req.Header {
		hReq.Header.Set(k, v)
	}
	if req.Body != nil {
		data, err := json.Marshal(req.Body)
		if err != nil {
			// #123：请求体序列化失败是本地确定性错误——重试恒失败，
			// 请求也从未到达节点，不构成后端故障样本。
			return nil, localErrorf("marshal body: %w", err)
		}
		hReq.SetBody(data)
		hReq.Header.SetContentTypeBytes([]byte("application/json"))
	}

	if err := f.http.Hertz().DoDeadline(cancelCtx, hReq, hResp, time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}

	code := hResp.StatusCode()
	if code < 200 || code >= 300 {
		msg := string(hResp.Body())
		msg = msg[:min(len(msg), 500)]
		// statusError 携带状态码：1xx/3xx/4xx 不计熔断失败、不惩罚节点、
		// 不重试（#140，与 httpx 的 >=500 失败判定同口径），5xx 反之
		return nil, &statusError{code: code, err: fmt.Errorf("http error: %d %s", code, msg)}
	}

	if req.Result != nil {
		if body := hResp.Body(); len(body) == 0 {
			// #123：2xx 空响应体（204 No Content 常见）+ Result 非 nil
			// 视同 json null——跳过解码返回保持零值的 Result。此前
			// json.Unmarshal 空切片必报 unexpected end of JSON input，
			// 且无 statusError 被判为可重试网络错误：每个坏请求白打
			// (1+Retry) 次真实 HTTP，连续 6 次调用即把熔断打开。
			return req.Result, nil
		}
		if err := json.Unmarshal(hResp.Body(), req.Result); err != nil {
			// #123：响应解码失败是本地确定性错误（调用方声明的 Result
			// 与响应契约不符）——重试恒失败，不构成后端故障样本。
			return nil, localErrorf("unmarshal response: %w", err)
		}
		return req.Result, nil
	}

	return append([]byte(nil), hResp.Body()...), nil
}
