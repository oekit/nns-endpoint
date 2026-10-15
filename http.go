package endpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	nnsclient "github.com/yi-nology/ekit-nns/nns/client"

	"github.com/yi-nology/ekit-metrics/resilience"

	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/sony/gobreaker"
	"github.com/yi-nology/ekit-httpx/httpx"
	"github.com/yi-nology/ekit-httpx/loadbalance"
	"github.com/yi-nology/ekit-logx"
	"github.com/yi-nology/ekit-registry/registry"
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
	balancerCache *balancerCache
	breakers      *breakerCache
	metrics       *resilience.Metrics
}

// metricsKind 是本工厂在弹性指标里的 kind 标签值。
const httpMetricsKind = "nns-http"

// NewHTTPEndpointFactory 根据服务客户端创建 HTTP 端点工厂实例。
// opts 定制 per-service 熔断参数（WithBreakerSettings）、弹性指标
// （WithMetrics）等行为。
func NewHTTPEndpointFactory(c *nnsclient.ServiceClient, opts ...EndpointFactoryOption) *HTTPEndpointFactory {
	httpClient := httpx.NewClient("nns-http",
		httpx.WithTimeout(30*time.Second),
		httpx.WithTracePropagation(),
	)
	tmpl := newBreakerTemplate(httpMetricsKind, opts)

	f := &HTTPEndpointFactory{
		client:        c,
		http:          httpClient,
		balancerCache: newBalancerCache(c),
		breakers:      newBreakerCache(tmpl.settingsFor),
		metrics:       tmpl.metrics,
	}

	c.OnConfigChange(func(serviceName string) {
		f.balancerCache.reset(serviceName)
	})

	return f
}

// ResetBalancer 清除指定服务的负载均衡器缓存。
func (f *HTTPEndpointFactory) ResetBalancer(serviceName string) {
	f.balancerCache.reset(serviceName)
}

// MakeEndpoint 为指定服务创建 HTTP 端点。
//
// 重试语义（消费服务配置 ServiceBasic.Retry，Nacos 配置缺省 3）：
// 失败时换实例重试，最多额外 Retry 次尝试；5xx 与网络错误重试，
// 4xx 不重试（确定性请求错误）。熔断 Execute 包住整组尝试——
// 一次调用计一次熔断样本（与 httpx.WithCircuitBreaker +
// WithRetryConfig 的层次结构一致），避免单次调用的多次尝试
// 撑爆连续失败计数。每次尝试独立 per-attempt 超时，最坏总耗时
// 为 (1+Retry) × ReadTimeout。
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
		timeout := req.Timeout
		if timeout == 0 {
			timeout = time.Duration(basic.ReadTimeout) * time.Millisecond
		}

		result, err := f.breakers.getOrCreate(serviceName).Execute(func() (any, error) {
			var lastErr error
			for attempt := 0; attempt <= retry; attempt++ {
				result, err := f.attemptOnce(ctx, serviceName, req, timeout)
				if err == nil {
					return result, nil
				}
				if !retryable(err) {
					return nil, err
				}
				lastErr = err
				if attempt < retry {
					if f.metrics != nil {
						f.metrics.ObserveRetry(serviceName, httpMetricsKind)
					}
					epLog.Debug("http endpoint retry on next instance",
						"service", serviceName, "attempt", attempt+1, "err", err)
				}
			}
			return nil, lastErr
		})
		if err != nil {
			if errors.Is(err, gobreaker.ErrOpenState) {
				if f.metrics != nil {
					f.metrics.ObserveRejected(serviceName, httpMetricsKind)
				}
				return nil, fmt.Errorf("%w: service=%s", ErrCircuitOpen, serviceName)
			}
			return nil, err
		}
		return result, nil
	}, nil
}

// attemptOnce 执行一次"选实例 + 发请求"尝试。
// 每次尝试重新获取实例列表并 Pick——重试落在不同节点上才有意义
// （RoundRobin 自然前进，Random 可能撞同一节点）。
func (f *HTTPEndpointFactory) attemptOnce(ctx context.Context, serviceName string, req HTTPRequest, timeout time.Duration) (any, error) {
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
	addr := "http://" + net.JoinHostPort(instance.IP, strconv.Itoa(instance.Port))

	return f.doRequest(ctx, addr, req, timeout)
}

// Close 关闭 HTTP 端点工厂持有的资源。
// Hertz client 连接池无显式释放 API（随进程生命周期），httpx.Client
// 的 endpointManager 在本工厂场景未启用，故此处无资源可释放；
// 保留方法以满足工厂统一 Close 契约。
func (f *HTTPEndpointFactory) Close() {}

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
			return nil, fmt.Errorf("marshal body: %w", err)
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
		if len(msg) > 500 {
			msg = msg[:500]
		}
		// statusError 携带状态码：4xx 不计熔断失败、不重试，5xx 反之
		return nil, &statusError{code: code, err: fmt.Errorf("http error: %d %s", code, msg)}
	}

	if req.Result != nil {
		if err := json.Unmarshal(hResp.Body(), req.Result); err != nil {
			return nil, fmt.Errorf("unmarshal response: %w", err)
		}
		return req.Result, nil
	}

	return append([]byte(nil), hResp.Body()...), nil
}
