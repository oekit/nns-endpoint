package endpoint

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sony/gobreaker"
	nnsclient "github.com/yi-nology/ekit-nns/nns/client"
	"github.com/yi-nology/ekit-nns/nns/metadata"
	"github.com/yi-nology/ekit-registry/registry"
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
