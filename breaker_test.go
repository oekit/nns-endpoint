package endpoint

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sony/gobreaker"
	nnsclient "github.com/yi-nology/ekit-nns/nns/client"
	"github.com/yi-nology/ekit-registry/registry"
)

// TestBreakerCache_PerServiceIsolation 验证熔断器按服务分片：
// 同一服务复用同一熔断器，不同服务互相隔离。
func TestBreakerCache_PerServiceIsolation(t *testing.T) {
	bc := newBreakerCache(newBreakerTemplate("test", nil).settingsFor)

	a1 := bc.getOrCreate("svc-a")
	a2 := bc.getOrCreate("svc-a")
	b := bc.getOrCreate("svc-b")

	if a1 != a2 {
		t.Fatal("expected same breaker instance for the same service")
	}
	if a1 == b {
		t.Fatal("expected different breaker instances for different services")
	}

	// 连续失败触发 svc-a 熔断
	for i := 0; i < 10; i++ {
		_, _ = a1.Execute(func() (any, error) { return nil, errors.New("boom") })
	}
	if a1.State() != gobreaker.StateOpen {
		t.Fatalf("expected svc-a breaker open, got %v", a1.State())
	}
	// svc-b 不受影响
	if b.State() != gobreaker.StateClosed {
		t.Fatalf("expected svc-b breaker closed, got %v", b.State())
	}
	if _, err := b.Execute(func() (any, error) { return "ok", nil }); err != nil {
		t.Fatalf("svc-b should still execute, got: %v", err)
	}
}

// TestHTTPEndpoint_CircuitBreakerIsolatedPerService 端到端验证：
// svc-a 连续失败熔断后，svc-b（健康实例）的调用不受影响。
// 修复前工厂共享单个熔断器，svc-a 熔断会连带拒绝 svc-b。
func TestHTTPEndpoint_CircuitBreakerIsolatedPerService(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer good.Close()

	badHost, badPort := parseHostPort(bad.URL)
	goodHost, goodPort := parseHostPort(good.URL)

	c := nnsclient.New(
		nnsclient.WithMockMode(true),
		nnsclient.WithStaticInstances("svc-a", []registry.Instance{{IP: badHost, Port: badPort}}),
		nnsclient.WithStaticInstances("svc-b", []registry.Instance{{IP: goodHost, Port: goodPort}}),
	)
	factory := NewHTTPEndpointFactory(c)
	defer factory.Close()

	epA, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint svc-a: %v", err)
	}
	epB, err := factory.MakeEndpoint("svc-b")
	if err != nil {
		t.Fatalf("MakeEndpoint svc-b: %v", err)
	}

	// ReadyToTrip: ConsecutiveFailures > 5，6 次失败后熔断。
	// mock 模式下服务配置取默认 ServiceBasic（Retry=3），每次调用在熔断
	// 内换实例重试 3 次，但熔断只计一次样本（与 httpx breaker-outside-retry 对齐）。
	var openErr error
	for i := 0; i < 7; i++ {
		_, openErr = epA(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/x"})
	}
	if openErr == nil || !errors.Is(openErr, ErrCircuitOpen) {
		t.Fatalf("expected svc-a circuit open error (errors.Is ErrCircuitOpen), got: %v", openErr)
	}

	// svc-b 必须正常
	if _, err := epB(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/x"}); err != nil {
		t.Fatalf("svc-b should not be affected by svc-a breaker: %v", err)
	}
}

// TestResetBalancer_AppliesToExistingEndpoints 回归测试（W1-T2）：
// balancer 必须在端点闭包内按调用获取，ResetBalancer 后存量端点
// 下一次调用即使用重建的 balancer（修复前闭包持有旧 balancer 引用）。
func TestResetBalancer_AppliesToExistingEndpoints(t *testing.T) {
	factory := newHTTPFactoryWithTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	// 触发 balancer 创建并缓存
	if _, err := ep(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/x"}); err != nil {
		t.Fatalf("first call: %v", err)
	}
	factory.balancerCache.mu.RLock()
	old := factory.balancerCache.balancers["svc-a"]
	factory.balancerCache.mu.RUnlock()
	if old == nil {
		t.Fatal("expected balancer to be cached after first call")
	}

	factory.ResetBalancer("svc-a")

	// 同一端点（存量闭包）再次调用，必须按新配置重建 balancer
	if _, err := ep(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/x"}); err != nil {
		t.Fatalf("call after reset: %v", err)
	}
	factory.balancerCache.mu.RLock()
	fresh := factory.balancerCache.balancers["svc-a"]
	factory.balancerCache.mu.RUnlock()

	if fresh == nil {
		t.Fatal("expected balancer to be recreated after reset")
	}
	if fresh == old {
		t.Fatal("existing endpoint still uses the old balancer after ResetBalancer")
	}
}
