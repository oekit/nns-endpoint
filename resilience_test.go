package endpoint

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	nnsclient "github.com/oekit/nns/client"
	"github.com/oekit/nns/metadata"
	"github.com/oekit/registry"
	"github.com/sony/gobreaker"
)

// newFactoryWithServers 构造带自定义服务配置与多个静态实例的 HTTP 工厂。
// 显式 Init 使 localServiceConfig 进入 depService（GetServiceBasic 才能读到），
// 与生产路径一致；mock 模式下 Init 无网络依赖。
func newFactoryWithServers(t *testing.T, cfg *metadata.ServiceBasic, factories map[string][]func() (string, int)) *HTTPEndpointFactory {
	t.Helper()
	opts := []nnsclient.ClientOption{nnsclient.WithMockMode(true)}
	for svc, makers := range factories {
		instances := make([]registry.Instance, 0, len(makers))
		for _, mk := range makers {
			host, port := mk()
			instances = append(instances, registry.Instance{IP: host, Port: port})
		}
		opts = append(opts, nnsclient.WithStaticInstances(svc, instances))
	}
	if cfg != nil {
		opts = append(opts, nnsclient.WithLocalServiceConfig("svc-a", cfg))
	}
	c := nnsclient.New(opts...)
	if err := c.LoadDepService(context.Background(), []string{"svc-a"}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	return NewHTTPEndpointFactory(c)
}

func localServiceConfig(retry int) *metadata.ServiceBasic {
	return &metadata.ServiceBasic{
		ServiceName: metadata.ServiceName{FullName: "svc-a"},
		ConnectionConfig: metadata.ConnectionConfig{
			ReadTimeout: 3000,
			Lb:          metadata.LbRoundRobin,
			Retry:       retry,
		},
	}
}

// TestHTTPEndpoint_ClientErrorNotTripBreaker 钉住"4xx 不计熔断失败"语义：
// 调用方请求错误与对端健康无关，连续 4xx 不得触发熔断。
// 修复前 4xx 与 5xx 同样计入连续失败，单调用方的坏请求会惩罚所有调用方。
func TestHTTPEndpoint_ClientErrorNotTripBreaker(t *testing.T) {
	var hits atomic.Int64
	factory := newHTTPFactoryWithTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	})
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	// 超过默认 ReadyToTrip 阈值（连续失败 > 5）的 4xx 调用
	for i := 0; i < 10; i++ {
		if _, err := ep(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/x"}); err == nil {
			t.Fatal("expected 400 error from endpoint")
		}
	}
	if got := hits.Load(); got != 10 {
		t.Fatalf("expected exactly 10 hits (no retry on 4xx), got %d", got)
	}
	// 熔断器必须仍是 closed
	if state := factory.breakers.getOrCreate("svc-a").State(); state != gobreaker.StateClosed {
		t.Fatalf("expected breaker closed after 10 consecutive 4xx, got %v", state)
	}
}

// TestHTTPEndpoint_RetryOnNextInstance 钉住换实例重试语义：
// ServiceBasic.Retry=2 时，首个实例 5xx 后换下一实例重试，第三次成功即整体成功。
func TestHTTPEndpoint_RetryOnNextInstance(t *testing.T) {
	bad1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad1.Close()
	bad2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer bad2.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer good.Close()

	bad1Host, bad1Port := parseHostPort(bad1.URL)
	bad2Host, bad2Port := parseHostPort(bad2.URL)
	goodHost, goodPort := parseHostPort(good.URL)

	// roundrobin：三个实例按序轮转，第 1 次调用依次落在 bad1/bad2/good
	factory := newFactoryWithServers(t, localServiceConfig(2), map[string][]func() (string, int){
		"svc-a": {
			func() (string, int) { return bad1Host, bad1Port },
			func() (string, int) { return bad2Host, bad2Port },
			func() (string, int) { return goodHost, goodPort },
		},
	})
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	var result struct {
		OK bool `json:"ok"`
	}
	if _, err := ep(context.Background(), HTTPRequest{
		Method: http.MethodGet, Path: "/x", Result: &result,
	}); err != nil {
		t.Fatalf("expected success via instance retry, got: %v", err)
	}
	if !result.OK {
		t.Fatal("expected response body to be decoded")
	}
}

// TestHTTPEndpoint_RetryExhaustedReturnsLastError 钉住重试耗尽语义：
// 全部实例 5xx 且重试次数用尽时返回最后一次错误，且每次尝试都真实发出。
func TestHTTPEndpoint_RetryExhaustedReturnsLastError(t *testing.T) {
	var hits atomic.Int64
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()
	host, port := parseHostPort(bad.URL)

	factory := newFactoryWithServers(t, localServiceConfig(2), map[string][]func() (string, int){
		"svc-a": {func() (string, int) { return host, port }},
	})
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}
	_, err = ep(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/x"})
	if err == nil {
		t.Fatal("expected error after retry exhausted")
	}
	if code, ok := httpStatus(err); !ok || code != http.StatusInternalServerError {
		t.Fatalf("expected last error to carry 500, got: %v", err)
	}
	// 1 次首试 + 2 次重试（单实例场景每次重试仍落在同一实例）
	if got := hits.Load(); got != 3 {
		t.Fatalf("expected 3 attempts (1+Retry), got %d", got)
	}
}

// TestWithBreakerSettings_CustomReadyToTrip 钉住熔断参数可配置：
// 定制 ReadyToTrip（单次失败即 trip）+ Timeout 后行为随之变化。
func TestWithBreakerSettings_CustomReadyToTrip(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()
	host, port := parseHostPort(bad.URL)

	c := nnsclient.New(
		nnsclient.WithMockMode(true),
		nnsclient.WithStaticInstances("svc-a", []registry.Instance{{IP: host, Port: port}}),
		nnsclient.WithLocalServiceConfig("svc-a", localServiceConfig(0)),
	)
	if err := c.LoadDepService(context.Background(), []string{"svc-a"}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	factory := NewHTTPEndpointFactory(c, WithBreakerSettings(gobreaker.Settings{
		ReadyToTrip: func(counts gobreaker.Counts) bool { return counts.ConsecutiveFailures >= 1 },
		Timeout:     50 * time.Millisecond,
	}))
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	// 单次失败即 trip：第一次调用失败（500），第二次必须是 ErrCircuitOpen
	if _, callErr := ep(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/x"}); callErr == nil {
		t.Fatal("expected first call to fail with 500")
	}
	_, err = ep(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/x"})
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen after single failure, got: %v", err)
	}
}

// TestHTTPEndpoint_DecodeErrorNotRetried 是 #123 的回归测试：2xx 响应体
// 无法解码进 Result（非 JSON 契约）是本地确定性错误——不换实例重试。
// 修复前解码错误无 statusError，retryable 判真：Retry=2 时每个坏请求
// 白打 3 次真实 HTTP（1 次首试 + 2 次换实例重试），重试结果必然同样失败。
func TestHTTPEndpoint_DecodeErrorNotRetried(t *testing.T) {
	var hits atomic.Int64
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `<html>service moved</html>`)
	}))
	defer bad.Close()
	host, port := parseHostPort(bad.URL)

	factory := newFactoryWithServers(t, localServiceConfig(2), map[string][]func() (string, int){
		"svc-a": {func() (string, int) { return host, port }},
	})
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	var result map[string]any
	_, err = ep(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/x", Result: &result})
	if err == nil {
		t.Fatal("expected decode error to propagate")
	}
	if !containsAny(err.Error(), "unmarshal response") {
		t.Errorf("expected decode error shape preserved, got: %v", err)
	}
	// 解码失败必须恰好 1 次真实 HTTP（修复前 1+Retry=3 次）
	if got := hits.Load(); got != 1 {
		t.Fatalf("expected exactly 1 attempt (decode error must not retry), got %d", got)
	}
}

// TestHTTPEndpoint_DecodeErrorDoesNotTripBreaker 是 #123 的回归测试：
// 连续解码失败不计熔断连续失败——解码错误是调用方契约错误，不是被选
// 节点的故障。修复前每次调用给熔断器记 1 个连续失败，默认
// ReadyToTrip（ConsecutiveFailures > 5）下第 7 次调用起被 ErrCircuitOpen
// 拒绝，之后所有正常调用（含不带 Result 的）一并被拒 30s。
func TestHTTPEndpoint_DecodeErrorDoesNotTripBreaker(t *testing.T) {
	factory := newHTTPFactoryWithTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `not-json`)
	})
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	// 远超默认 ReadyToTrip 阈值（连续失败 > 5）的解码失败调用，
	// 每次都必须保持解码错误形态（不得被 ErrCircuitOpen 掩盖）
	for range 10 {
		var result map[string]any
		if _, callErr := ep(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/x", Result: &result}); callErr == nil {
			t.Fatal("expected decode error")
		} else if errors.Is(callErr, ErrCircuitOpen) {
			t.Fatalf("decode error must not trip the breaker, got: %v", callErr)
		}
	}
	if state := factory.breakers.getOrCreate("svc-a").State(); state != gobreaker.StateClosed {
		t.Fatalf("expected breaker closed after 10 consecutive decode errors, got %v", state)
	}
}

// TestHTTPEndpoint_EmptyBodyWithResultSucceeds 是 #123 的回归测试：
// 2xx 空响应体（204 No Content 常见）+ Result 非 nil 视同 json null——
// 跳过解码返回保持零值的 Result。修复前 json.Unmarshal([]byte{}) 必报
// unexpected end of JSON input，且被当可重试网络错误白打 1+Retry 次。
func TestHTTPEndpoint_EmptyBodyWithResultSucceeds(t *testing.T) {
	var hits atomic.Int64
	factory := newHTTPFactoryWithTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	})
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	result := map[string]any{"preset": true}
	resp, err := ep(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/x", Result: &result})
	if err != nil {
		t.Fatalf("expected empty 2xx body to succeed (treated as json null), got: %v", err)
	}
	// 返回值必须是调用方传入的 Result 指针，且解码被跳过（预置数据保持原样）
	if resp != any(&result) {
		t.Errorf("expected caller's Result pointer back, got %T", resp)
	}
	if result["preset"] != true {
		t.Errorf("Result must be preserved untouched on empty body, got %v", result)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("expected exactly 1 attempt, got %d", got)
	}
}

// TestHTTPEndpoint_MarshalErrorDoesNotTripBreaker 是 #123 的回归测试：
// 请求体序列化失败是本地确定性错误（请求根本不会到达节点）——不计
// 熔断失败。修复前无 statusError 按网络错误计入连续失败，连续坏请求
// 会把熔断打开，殃及所有正常调用方。
func TestHTTPEndpoint_MarshalErrorDoesNotTripBreaker(t *testing.T) {
	var hits atomic.Int64
	factory := newHTTPFactoryWithTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	// chan 无法 JSON 序列化：每次调用都在本地 marshal 失败，
	// 0 次真实 HTTP，且远超熔断阈值后仍必须保持 closed
	for range 10 {
		if _, callErr := ep(context.Background(), HTTPRequest{
			Method: http.MethodPost,
			Path:   "/x",
			Body:   make(chan int),
		}); callErr == nil {
			t.Fatal("expected marshal error")
		} else if containsAny(callErr.Error(), "circuit breaker") {
			t.Fatalf("marshal error must not trip the breaker, got: %v", callErr)
		} else if !containsAny(callErr.Error(), "marshal body") {
			t.Fatalf("expected marshal body error shape, got: %v", callErr)
		}
	}
	if got := hits.Load(); got != 0 {
		t.Fatalf("marshal failure never reaches the backend, got %d hits", got)
	}
	if state := factory.breakers.getOrCreate("svc-a").State(); state != gobreaker.StateClosed {
		t.Fatalf("expected breaker closed after 10 consecutive marshal errors, got %v", state)
	}
}

// TestHTTPEndpoint_ClientErrorNotChargedToNode 是 #124 的回归测试：
// done 回写区分错误归属——HTTP 4xx 是调用方请求错误，「碰巧被选中的
// 节点」不得被记 EWMA 故障惩罚（修复前 4xx 的非 nil Err 全量回写，
// p2c 把流量推离完全健康的节点）；节点/网络错误（5xx）仍原样回写，
// EWMA 惩罚语义不变（#57/#74）。
func TestHTTPEndpoint_ClientErrorNotChargedToNode(t *testing.T) {
	var nodeFault atomic.Bool // false=4xx（调用方错误）；true=5xx（节点错误）
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if nodeFault.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)
	host, port := parseHostPort(srv.URL)

	rec := &recordingSelector{}
	c := nnsclient.New(
		nnsclient.WithMockMode(true),
		nnsclient.WithStaticInstances("svc-a", []registry.Instance{{IP: host, Port: port}}),
		// Retry=0：本测试钉 done 错误归属，5xx 阶段不换实例重试
		//（否则 500 一次调用产生 1+Retry 个 done 样本）
		nnsclient.WithLocalServiceConfig("svc-a", localServiceConfig(0)),
	)
	if err := c.LoadDepService(context.Background(), []string{"svc-a"}); err != nil {
		t.Fatalf("LoadDepService: %v", err)
	}
	factory := NewHTTPEndpointFactory(c, WithSelector(rec))
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}
	call := func() error {
		_, callErr := ep(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/x"})
		return callErr
	}

	// 4xx 阶段：调用报错，但节点不得被记故障惩罚
	if err := call(); err == nil {
		t.Fatal("expected 400 error from endpoint")
	}
	if errs := rec.doneErrs(); len(errs) != 1 || errs[0] != nil {
		t.Fatalf("4xx must not charge the selected node, done samples = %v", errs)
	}

	// 5xx 阶段：节点/网络错误原样回写，EWMA 惩罚语义不变
	nodeFault.Store(true)
	if err := call(); err == nil {
		t.Fatal("expected 500 error from endpoint")
	}
	errs := rec.doneErrs()
	if len(errs) != 2 {
		t.Fatalf("expected exactly 2 done samples, got %d", len(errs))
	}
	if errs[1] == nil {
		t.Fatal("5xx must keep charging the node (EWMA penalty semantics unchanged)")
	}
	if code, ok := httpStatus(errs[1]); !ok || code != http.StatusInternalServerError {
		t.Fatalf("5xx done sample must carry the status error, got: %v", errs[1])
	}
}
