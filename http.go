package endpoint

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	nnsclient "github.com/oekit/nns/client"

	"github.com/oekit/metrics/resilience"
	pkgerrors "github.com/oekit/pkg/errors"
	"github.com/oekit/pkg/pathx"
	"github.com/oekit/pkg/stringx"

	"github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/protocol"
	retryhttpx "github.com/oekit/concurrency/retry/httpx"
	"github.com/oekit/httpx"
	"github.com/oekit/httpx/loadbalance"
	"github.com/oekit/logx"
	"github.com/oekit/registry"
)

var epLog = logx.Delegating("nns-endpoint")

// HTTPRequest HTTP 端点请求参数。
type HTTPRequest struct {
	Method string
	Path   string
	Body   any
	Header map[string]string
	Query  map[string]string
	Result any
	// Timeout 是单次尝试的超时；0 时回退服务治理 ReadTimeout，两级均未
	// 配置时兜底 defaultCallTimeout（#75）。
	Timeout time.Duration
	// Idempotent 显式声明本次请求可安全重放（#231）。缺省 false：
	// 是否重试由 Method 的幂等性决定（GET/HEAD/PUT/DELETE/OPTIONS/TRACE
	// 幂等；POST/PATCH 不重试）。非幂等方法（如带幂等键的 POST）确需
	// 换实例重试时置 true——显式声明永远优先于方法门。
	Idempotent bool
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
	// closed 是工厂的终态登记（#231，与 gRPC 工厂 #148 对齐）：Close 置位
	// 后存量端点闭包短路返回 ErrFactoryClosed，不再触达底层 HTTP 客户端。
	// atomic：端点闭包读不持工厂锁，与 Close 无同一把锁的 happens-before。
	closed atomic.Bool
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

// attemptBackoff 是换实例重试的 attempt 间最小退避（#203）：短间隔即可
// 打断对持续故障后端的紧循环连击，又不过度拉长最坏总耗时；服务端返回
// Retry-After 建议且更长时取建议（#231，见 attemptBackoffFor）。
const attemptBackoff = 50 * time.Millisecond

// attemptBackoffFor 计算本次失败后的 attempt 间等待：缺省 attemptBackoff，
// 响应携带更长的 Retry-After 建议时取建议（#231）——此前全仓无 Retry-After
// 解析，上游 429 Retry-After: 3 时仍按 50ms 连打，重试全落在限流窗口内。
// 建议时长不做上限截断（服务端语义原样尊重），等待本身可被父 ctx 中断：
// 调用方需要收紧总耗时时用带 deadline 的 ctx 或 per-request Timeout 之外的
// 调用级超时表达。
func attemptBackoffFor(err error) time.Duration {
	if hint, ok := retryAfterHintOf(err); ok && hint > attemptBackoff {
		return hint
	}
	return attemptBackoff
}

// waitAttemptBackoff 可中断地等待 attempt 间退避：完整等待返回 true；
// ctx 取消/超时中断等待返回 false，调用方应停止重试并返回既有错误。
func waitAttemptBackoff(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// methodIdempotent 判定 HTTP 方法是否幂等（可安全换实例重放）：与
// httpx idempotentForRetry 的方法集一致（GET/HEAD/OPTIONS/PUT/DELETE/
// TRACE）。POST/PATCH 缺省不在集合内——副作用在方法 + 路径上，5xx 或
// 「请求已到达、响应超时」下跨实例重放会重复扣款/重复下单（#231）。
func methodIdempotent(method string) bool {
	switch strings.ToUpper(method) {
	case http.MethodGet, http.MethodHead, http.MethodOptions,
		http.MethodPut, http.MethodDelete, http.MethodTrace:
		return true
	default:
		return false
	}
}

// retrySafe 判定本次请求是否允许换实例重试：显式 Idempotent 声明优先，
// 否则按 HTTP 方法幂等性裁定（#231）。
func retrySafe(req HTTPRequest) bool {
	return req.Idempotent || methodIdempotent(req.Method)
}

// NewHTTPEndpointFactory 根据服务客户端创建 HTTP 端点工厂实例。
// opts 定制 per-service 熔断参数（WithBreakerSettings）、弹性指标
// （WithMetrics）等行为。
func NewHTTPEndpointFactory(c *nnsclient.ServiceClient, opts ...EndpointFactoryOption) *HTTPEndpointFactory {
	// httpx v0.8.12 起 NewClient 显式返回 error（httpx#83）；本工厂的
	// options 固定且不可能构造冲突（无 WithEndpoints/WithNNS 组合面），
	// 走 MustNewClient 逃生舱（panic 仅初始化期可达，语义同旧版）。
	//
	// #231：客户端级读超时关闭（WithStream，经 httpx 固化预设把 ReadTimeout
	// 置 0）——此前 WithTimeout(30s) 同时把拨号与读固定为 30s，服务治理
	// ReadTimeout 配 45s/60s 的慢接口每 30s 必被内层客户端掐断，doRequest
	// 的 DoDeadline 只兜总时长。读预算改为逐请求设置（config.WithReadTimeout
	// + DoDeadline），拨号仍保留 30s 兜底。
	httpClient := httpx.MustNewClient("nns-http",
		httpx.WithTimeout(30*time.Second),
		httpx.WithStream(),
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
// 失败时换实例重试，最多额外 Retry 次尝试；**重试前先过幂等门**（#231）：
// 仅幂等方法（GET/HEAD/PUT/DELETE/OPTIONS/TRACE）或显式置
// HTTPRequest.Idempotent 的请求可重放，POST/PATCH 缺省不重试——副作用在
// 方法 + 路径上，5xx 或「请求已到达、响应超时」下跨实例重放等于重复
// 扣款/下单。可重试判定与 httpx defaultRetryIf 同口径（#203，判定单源
// concurrency/retry/httpx）——429/5xx 与网络错误重试，其余 4xx 不重试
// （确定性请求错误）；ctx 取消/超时不重试（调用方主动放弃）；本地确定性
// 错误（序列化/解码/选点，#123）重试同一输入恒失败，同样不重试、不计
// 熔断样本。attempt 间退避缺省 attemptBackoff，响应携带更长的
// Retry-After 建议时取建议（#231），等待可被 ctx 取消中断（#203），每次
// 尝试前复查父 ctx。熔断 Execute 包住整组尝试——一次调用计一次熔断样本
// （与 httpx.WithCircuitBreaker + WithRetryConfig 的层次结构一致），避免
// 单次调用的多次尝试撑爆连续失败计数。每次尝试独立 per-attempt 超时
// （timeout = req.Timeout / 服务治理 ReadTimeout / defaultCallTimeout，
// 逐请求同时设置读预算与 DoDeadline 总预算，#231），最坏总耗时为
// (1+Retry) × timeout + Σ attempt 间退避（每次为
// max(attemptBackoff, Retry-After 建议)）。
//
// 分层边界（#96/#98）：实例发现与 selector 首建在 Execute 之外——
// 「无实例」与「配置中心故障」都不是目标后端的故障，不构成熔断样本
// （与 gRPC 路径的分层一致）。
func (f *HTTPEndpointFactory) MakeEndpoint(serviceName string) (Endpoint, error) {
	return func(ctx context.Context, request any) (any, error) {
		// #231：工厂 Close 后端点闭包短路报错（与 gRPC 工厂 #148 对齐）——
		// 存量端点在优雅停机窗口被路由表/调用方再次调用是常态场景，必须
		// 显式拒绝而不是触达已关闭的 HTTP 客户端。本地确定性错误：不计
		// 熔断样本、不做节点 EWMA 惩罚、不换实例重试。
		if f.closed.Load() {
			return nil, localErrorf("%w (service=%s)", ErrFactoryClosed, serviceName)
		}

		req, ok := request.(HTTPRequest)
		if !ok {
			return nil, pkgerrors.New("invalid request type, expected HTTPRequest")
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
			return nil, pkgerrors.Wrapf(err, "get instances")
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
				// #203：每次尝试前复查父 ctx——调用方取消/超时后不再发起
				// 新尝试（此前 ctx 取消的错误被判可重试，紧循环白烧全部
				// 重试预算）。已有 lastErr 时优先返回真实失败原因。
				if ctxErr := ctx.Err(); ctxErr != nil {
					if lastErr != nil {
						return nil, lastErr
					}
					return nil, ctxErr
				}
				res, attemptErr := f.attemptOnce(ctx, sel, nodes, req, timeout)
				if attemptErr == nil {
					return res, nil
				}
				if !retryable(attemptErr) {
					return nil, attemptErr
				}
				// #231：重试前过幂等门——POST/PATCH 缺省不重放（显式
				// Idempotent 声明除外）。方法门在 retryable 之后、真正
				// 发起下一次尝试之前生效：首试永远执行，非幂等请求的
				// 失败（含 5xx/网络错误）原样返回给调用方。
				if !retrySafe(req) {
					return nil, attemptErr
				}
				lastErr = attemptErr
				if attempt < retry {
					if f.metrics != nil {
						f.metrics.ObserveRetry(serviceName, httpMetricsKind)
					}
					epLog.Debug("http endpoint retry on next instance",
						"service", serviceName, "attempt", attempt+1, logx.ErrKey, attemptErr)
					// #203/#231：attempt 间短退避、可被 ctx 取消中断——此前
					// 零延迟紧循环对持续故障的后端连击 (1+Retry) 次；429/503
					// 携带 Retry-After 时按服务端建议等待，不再 50ms 内反复
					// 重打限流窗口。
					if !waitAttemptBackoff(ctx, attemptBackoffFor(attemptErr)) {
						return nil, lastErr
					}
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

// Close 关闭 HTTP 端点工厂持有的资源：先登记终态（此后存量端点闭包
// 短路返回 ErrFactoryClosed，#231，与 gRPC 工厂 #148 对齐），再注销构造期
// 在 client 上注册的配置变更回调（#135，不注销则 client 经回调闭包长持
// 已废弃工厂、配置事件继续打在死工厂上），最后关闭底层 httpx.Client
// （httpx v0.13.0 的 Close 释放 Hertz 空闲连接池，见 httpx#84；此前注释
// 声称「Hertz 连接池无显式释放 API」与事实不符，反复重建工厂会泄漏空闲
// 连接）。OffConfigChange 与 httpx.Client.Close 均幂等，Close 可安全重复
// 调用。
func (f *HTTPEndpointFactory) Close() {
	f.closed.Store(true)
	f.client.OffConfigChange(f.configChangeHandle)
	f.configChangeHandle = nil
	f.http.Close()
}

// joinEndpointURL 把实例地址与请求 path 拼成完整 URL，拼接前归一分隔符
// （#202，与 httpx#112 的 joinEndpoint 同语义——本仓内实现，不引 httpx
// 私有函数）：base 去尾 "/"、path 缺前导 "/" 时补齐。修复前直接相加，
// path 缺前导 "/" 时 host 被吃进 path（addr="http://h:8080" + "api/x" ⇒
// "http://h:8080api/x"，拨号报误导性的 unknown port）；path 为空时保持
// addr 原样（不追加 "/"）。path 尾斜杠不归一——调用方声明的
// "/api/v1/" 保持 "/api/v1/"（同 doRequest 处只动 addr 侧的既有约定）。
//
// 拼接主体单源下沉 pathx.JoinURL（#532）：path 非空时归一规则与 JoinURL
// 逐字一致（base 去尾 "/"、path 补前导 "/"），薄委托。path 为空保留本地
// TrimRight 分支不强行收敛：本仓钉住「空 path 时 addr 尾斜杠仍剥掉」
// （TestJoinEndpointURL 的 empty path keeps addr (trailing slash
// stripped)），而 JoinURL 对空 path 的契约是「不触碰 base、尾斜杠原样
// 保留」，两口径不同——以本仓既有行为为准。
func joinEndpointURL(addr, path string) string {
	if path == "" {
		return strings.TrimRight(addr, "/")
	}
	return pathx.JoinURL(addr, path)
}

// applyReadTimeout 把单次尝试的读预算写到请求级选项（#231）。
// 请求级配置优先于客户端级：内层 httpx 客户端的客户端级读超时已关闭
// （WithStream），读阶段的上限完全由这里与 DoDeadline 的 deadline 表达，
// 不再受固定 30s 约束。
func applyReadTimeout(hReq *protocol.Request, timeout time.Duration) {
	hReq.SetOptions(config.WithReadTimeout(timeout))
}

func (f *HTTPEndpointFactory) doRequest(ctx context.Context, addr string, req HTTPRequest, timeout time.Duration) (any, error) {
	// 只规整 addr 侧的尾斜杠用于拼接；TrimRight 整串曾把调用方声明的
	// path 尾斜杠剥掉（"/api/v1/" → "/api/v1"），隐式改写用户输入
	// （#202：path 缺前导 "/" 的归一同样只在拼接面发生，见 joinEndpointURL）
	fullURL := joinEndpointURL(addr, req.Path)

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
	// #231：读预算逐请求设置（请求级选项优先于客户端级配置）——内层
	// httpx 客户端的客户端级读超时已关闭（WithStream），服务治理
	// ReadTimeout 配 45s/60s 时不再被固定 30s 掐断。DoDeadline 随后设置
	// 同一时长的整体请求预算，两者共同保证单次尝试不超过 timeout。
	applyReadTimeout(hReq, timeout)
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
		return nil, pkgerrors.Wrapf(err, "http request")
	}

	code := hResp.StatusCode()
	if code < 200 || code >= 300 {
		// #204：错误体摘要经 pkg/stringx.FirstN 按 rune 截断——此前字节
		// 切片 msg[:min(len(msg),500)] 会把落在边界上的多字节字符腰斩，
		// 产出非法 UTF-8（日志/存储层拒收或乱码）
		msg := stringx.FirstN(string(hResp.Body()), 500)
		// statusError 携带状态码：1xx/3xx/4xx 不计熔断失败、不惩罚节点
		// （#140，与 httpx 的 >=500 失败判定同口径），5xx 反之；重试判定
		// 由 retryable 单源裁定（#203：429 与 5xx 可重试，其余 4xx 不重试）
		se := &statusError{code: code, err: pkgerrors.Newf("http error: %d %s", code, msg)}
		// #231：保留本次响应的 Retry-After 建议（delay-seconds 与
		// HTTP-date 双形态，解析单源 concurrency/retry/httpx.ParseRetryAfter）——
		// 重试等待据此退避；头缺失/不可解析时保持 hasRetryAfter=false，
		// 回退固定 attemptBackoff。
		if hint, ok := retryhttpx.ParseRetryAfter(hResp.Header.Get("Retry-After"), time.Now()); ok {
			se.retryAfter, se.hasRetryAfter = hint, true
		}
		return nil, se
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
