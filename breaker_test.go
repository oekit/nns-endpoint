package endpoint

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	nnsclient "github.com/oekit/nns/client"
	"github.com/oekit/registry"
	"github.com/sony/gobreaker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// TestBreakerCache_PerServiceIsolation 验证熔断器按服务分片：
// 同一服务复用同一熔断器，不同服务互相隔离。
func TestBreakerCache_PerServiceIsolation(t *testing.T) {
	bc := newBreakerCache(newBreakerTemplate("test", newFactoryConfig(nil)).settingsFor)

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

// TestResetSelector_AppliesToExistingEndpoints 回归测试（W1-T2）：
// selector 必须在端点闭包内按调用获取，ResetSelector 后存量端点
// 下一次调用即使用重建的 selector（修复前闭包持有旧 selector 引用）。
func TestResetSelector_AppliesToExistingEndpoints(t *testing.T) {
	factory := newHTTPFactoryWithTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	// 触发 selector 创建并缓存
	if _, err := ep(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/x"}); err != nil {
		t.Fatalf("first call: %v", err)
	}
	factory.selectorCache.mu.RLock()
	old := factory.selectorCache.selectors["svc-a"]
	factory.selectorCache.mu.RUnlock()
	if old == nil {
		t.Fatal("expected selector to be cached after first call")
	}

	factory.ResetSelector("svc-a")

	// 同一端点（存量闭包）再次调用，必须按新配置重建 selector
	if _, err := ep(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/x"}); err != nil {
		t.Fatalf("call after reset: %v", err)
	}
	factory.selectorCache.mu.RLock()
	fresh := factory.selectorCache.selectors["svc-a"]
	factory.selectorCache.mu.RUnlock()

	if fresh == nil {
		t.Fatal("expected selector to be recreated after reset")
	}
	if fresh == old {
		t.Fatal("existing endpoint still uses the old selector after ResetSelector")
	}
}

// TestRejectIfCircuitOpen 钉住 #133 收敛后的单源映射行为（HTTP/gRPC 两工厂
// 共用，规则演进只改这一处）：
//   - 非熔断拒绝（普通错误）→ nil，调用方继续原错误路径；
//   - ErrOpenState → ErrCircuitOpen（errors.Is 可判定，带 service 上下文）；
//   - ErrTooManyRequests（half-open 态并发超 MaxRequests）→ 同映射。
func TestRejectIfCircuitOpen(t *testing.T) {
	if got := rejectIfCircuitOpen(errors.New("boom"), "svc-a", httpMetricsKind, nil); got != nil {
		t.Fatalf("non-breaker rejection must return nil, got: %v", got)
	}

	err := rejectIfCircuitOpen(gobreaker.ErrOpenState, "svc-a", httpMetricsKind, nil)
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("ErrOpenState must map to ErrCircuitOpen, got: %v", err)
	}
	if !containsAny(err.Error(), "service=svc-a") {
		t.Errorf("mapped error must carry service context, got: %v", err)
	}

	err = rejectIfCircuitOpen(gobreaker.ErrTooManyRequests, "svc-b", grpcMetricsKind, nil)
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("ErrTooManyRequests must map to ErrCircuitOpen, got: %v", err)
	}
}

// TestGRPCEndpoint_CircuitBreakerOpenMapsToErrCircuitOpen 端到端验证 gRPC
// 路径的熔断拒绝映射（#133，与 HTTP 侧
// TestHTTPEndpoint_CircuitBreakerIsolatedPerService 对称）：svc-a 连续失败
// 熔断后，下一次调用必须得到 ErrCircuitOpen（而非 gobreaker 的裸
// ErrOpenState），且熔断拒绝发生在 done 回写之前（#97 语义不回退）。
func TestGRPCEndpoint_CircuitBreakerOpenMapsToErrCircuitOpen(t *testing.T) {
	dialOpts := newBufconnGRPCServer(t, "nns.test.EchoService", map[string]func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error){
		"Echo": func(_ context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return nil, status.Error(codes.Internal, "boom")
		},
	})

	c := nnsclient.New(
		nnsclient.WithMockMode(true),
		nnsclient.WithStaticInstances("svc-a", []registry.Instance{{IP: "10.7.0.1", Port: 1}}),
	)
	factory := NewGRPCEndpointFactory(c)
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a", dialOpts...)
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	// ReadyToTrip: ConsecutiveFailures > 5，6 次失败（Internal 非调用方
	// 错误，计熔断样本）后熔断打开；第 7 次调用被拒绝。
	req := GRPCRequest{Request: wrapperspb.String("x"), Reply: &wrapperspb.StringValue{}}
	var lastErr error
	for i := 0; i < 7; i++ {
		_, lastErr = ep(context.Background(), req)
	}
	if lastErr == nil || !errors.Is(lastErr, ErrCircuitOpen) {
		t.Fatalf("expected gRPC circuit open error (errors.Is ErrCircuitOpen), got: %v", lastErr)
	}
}
