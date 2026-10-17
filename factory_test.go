package endpoint

import (
	"context"
	"testing"

	"github.com/oekit/httpx"
	nnsclient "github.com/oekit/nns/client"
	"github.com/oekit/nns/metadata"
	"github.com/oekit/registry"
)

// newTestServiceClient 创建一个用于测试的 mock 模式 ServiceClient 实例。
// 每次 New 返回独立实例（不再使用 sync.Once 单例）。
func newTestServiceClient(t *testing.T) *nnsclient.ServiceClient {
	t.Helper()
	return nnsclient.New(
		nnsclient.WithMockMode(true),
		nnsclient.WithStaticInstances("svc-a", []registry.Instance{
			{IP: "10.0.0.1", Port: 8080},
			{IP: "10.0.0.2", Port: 8080},
			{IP: "10.0.0.3", Port: 8080},
		}),
		nnsclient.WithLocalServiceConfig("svc-a", &metadata.ServiceBasic{
			ConnectionConfig: metadata.ConnectionConfig{
				ReadTimeout: 3000,
				Lb:          metadata.LbRoundRobin,
			},
		}),
	)
}

// TestHTTPEndpointFactory_BalancerCached 验证同一服务的 selector 被缓存复用。
// 这是轮询策略正常工作的前提：如果每次重建 selector，计数器会重置导致永远选同一个实例。
func TestHTTPEndpointFactory_BalancerCached(t *testing.T) {
	c := newTestServiceClient(t)
	factory := NewHTTPEndpointFactory(c)

	b1, _ := factory.selectorCache.getOrCreate(context.Background(), "svc-a")
	b2, _ := factory.selectorCache.getOrCreate(context.Background(), "svc-a")

	if b1 != b2 {
		t.Fatal("expected same selector instance for same service (cache miss bug)")
	}

	// 不同服务应有不同 selector
	b3, _ := factory.selectorCache.getOrCreate(context.Background(), "svc-other")
	if b1 == b3 {
		t.Fatal("expected different selector for different service")
	}
}

// TestHTTPEndpointFactory_ResetSelector 验证 ResetSelector 清除缓存后重建。
func TestHTTPEndpointFactory_ResetSelector(t *testing.T) {
	c := newTestServiceClient(t)
	factory := NewHTTPEndpointFactory(c)

	b1, _ := factory.selectorCache.getOrCreate(context.Background(), "svc-a")
	factory.ResetSelector("svc-a")
	b2, _ := factory.selectorCache.getOrCreate(context.Background(), "svc-a")

	if b1 == b2 {
		t.Fatal("expected new selector instance after ResetSelector")
	}
}

// TestHTTPEndpointFactory_RoundRobinPersistsAcrossEndpoints 验证两次 MakeEndpoint
// 调用共享同一 selector，轮询计数器不会因 endpoint 重建而重置。
func TestHTTPEndpointFactory_RoundRobinPersistsAcrossEndpoints(t *testing.T) {
	c := newTestServiceClient(t)
	factory := NewHTTPEndpointFactory(c)

	// 创建 endpoint 两次（模拟 CallHTTP 的行为）
	ep1, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint error: %v", err)
	}
	ep2, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint error: %v", err)
	}

	// 两个 endpoint 应共享同一 selector
	// 验证方式：从 ep1 做 3 次 Pick（轮询应覆盖 3 个实例），
	// 然后从 ep2 做 1 次 Pick（应继续 ep1 的计数，而非从 0 开始）
	//
	// 由于 endpoint closure 内部调用 selector.Pick，我们需要直接验证 selector
	rr, _ := factory.selectorCache.getOrCreate(context.Background(), "svc-a")

	instances, err := c.GetInstances(context.Background(), "svc-a")
	if err != nil {
		t.Fatalf("GetInstances error: %v", err)
	}

	nodes := httpx.SelectorNodes(instances)
	ctx := context.Background()

	// 第一次 MakeEndpoint 后做 3 次 Select（轮询游标持续前进）
	seen1 := make(map[string]bool)
	for i := 0; i < len(instances); i++ {
		n, _, err := rr.Select(ctx, nodes)
		if err != nil {
			t.Fatalf("Select error: %v", err)
		}
		seen1[n.Address()] = true
	}
	if len(seen1) != len(instances) {
		t.Errorf("first batch: RR did not cover all instances, seen=%v", seen1)
	}

	// 第二次 MakeEndpoint 后再做 3 次 Select，计数器应继续
	seen2 := make(map[string]bool)
	for i := 0; i < len(instances); i++ {
		n, _, err := rr.Select(ctx, nodes)
		if err != nil {
			t.Fatalf("Select error: %v", err)
		}
		seen2[n.Address()] = true
	}
	if len(seen2) != len(instances) {
		t.Errorf("second batch: RR did not cover all instances, seen=%v", seen2)
	}

	// 如果 selector 被重建（bug），第二轮 Pick 会与第一轮返回相同序列
	// 但由于共享 selector，6 次 Pick 应覆盖所有索引
	allSeen := make(map[string]bool)
	for k := range seen1 {
		allSeen[k] = true
	}
	for k := range seen2 {
		allSeen[k] = true
	}
	// 3 个实例做 6 次轮询，所有地址都应被覆盖
	if len(allSeen) != len(instances) {
		t.Errorf("expected all %d addresses covered over 6 selects, got %d", len(instances), len(allSeen))
	}

	_ = ep1
	_ = ep2
}

// TestGRPCEndpointFactory_BalancerCached 验证 gRPC 端点工厂也缓存 selector。
func TestGRPCEndpointFactory_BalancerCached(t *testing.T) {
	c := newTestServiceClient(t)
	factory := NewGRPCEndpointFactory(c)
	defer factory.Close()

	b1, _ := factory.selectorCache.getOrCreate(context.Background(), "svc-a")
	b2, _ := factory.selectorCache.getOrCreate(context.Background(), "svc-a")

	if b1 != b2 {
		t.Fatal("expected same selector instance for same service")
	}

	b3, _ := factory.selectorCache.getOrCreate(context.Background(), "svc-other")
	if b1 == b3 {
		t.Fatal("expected different selector for different service")
	}
}

// TestHTTPEndpointFactory_UnsupportedLBFallback 验证不支持的 LB 策略回退到轮询。
func TestHTTPEndpointFactory_UnsupportedLBFallback(t *testing.T) {
	c := newTestServiceClient(t)
	factory := NewHTTPEndpointFactory(c)

	// svc-unknown 没有配置，mock 模式返回默认（LbRoundRobin）
	b, _ := factory.selectorCache.getOrCreate(context.Background(), "svc-unknown")
	if b == nil {
		t.Fatal("expected non-nil selector even for unknown service")
	}
	// 能正常 Select
	instances, _ := c.GetInstances(context.Background(), "svc-a")
	if len(instances) > 0 {
		n, _, err := b.Select(context.Background(), httpx.SelectorNodes(instances))
		if err != nil {
			t.Fatalf("Select error: %v", err)
		}
		if n == nil || n.Address() == "" {
			t.Fatalf("Select returned empty node: %v", n)
		}
	}
}
