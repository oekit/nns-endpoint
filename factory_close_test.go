package endpoint

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
	"weak"

	nnsclient "github.com/oekit/nns/client"

	"github.com/oekit/httpx/loadbalance"
)

// capturingConfigStore 记录 ListenConfig 注册的监听器并支持手动触发配置
// 变更事件（#135 行为面测试用）：fire 同步执行 nns 的分发闭包，返回即
// 代表回调已全部执行完。
type capturingConfigStore struct {
	mu        sync.Mutex
	content   string
	listeners map[string]func(string)
}

func newCapturingConfigStore(content string) *capturingConfigStore {
	return &capturingConfigStore{content: content, listeners: make(map[string]func(string))}
}

func (s *capturingConfigStore) GetConfig(_ context.Context, _, _ string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.content, nil
}

func (s *capturingConfigStore) ListenConfig(_ context.Context, service, _ string, fn func(string)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listeners[service] = fn
	return nil
}

func (s *capturingConfigStore) StopListenConfig(_ context.Context, service, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.listeners, service)
	return nil
}

// setContent 更新配置中心内容（GetConfig 的返回值）。
func (s *capturingConfigStore) setContent(content string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.content = content
}

// fire 手动触发指定服务的配置变更事件：等 ListenConfig 注册到位后调用
// 分发闭包（WatchService 注册的是 nns 侧的事件处理，回调在其内同步分发）。
func (s *capturingConfigStore) fire(t *testing.T, service string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		s.mu.Lock()
		fn, ok := s.listeners[service]
		s.mu.Unlock()
		if ok {
			fn("")
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("listener for %s not registered in time", service)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestFactoryClose_UnregistersConfigChangeCallback_LeakRegression 是 #135 的
// 泄漏回归测试：工厂 Close 必须按句柄注销 client 上的配置变更回调——
// 否则长生命周期的 ServiceClient 经回调闭包长持已废弃工厂，selectorCache/
// breakers/pool 全部不可回收。断言手段：弱引用 + 双轮 GC——Close 并解除
// 本地引用后，工厂必须可回收（弱引用取不到值）。
func TestFactoryClose_UnregistersConfigChangeCallback_LeakRegression(t *testing.T) {
	c := newTestServiceClient(t)

	httpFactory := NewHTTPEndpointFactory(c)
	grpcFactory := NewGRPCEndpointFactory(c)
	httpWeak := weak.Make(httpFactory)
	grpcWeak := weak.Make(grpcFactory)

	// 前置条件：Close 前工厂可达（本地强引用 + client 的回调闭包）
	runtime.GC()
	if httpWeak.Value() == nil || grpcWeak.Value() == nil {
		t.Fatal("precondition: factories must be reachable before Close")
	}

	httpFactory.Close()
	grpcFactory.Close()
	httpFactory, grpcFactory = nil, nil

	// 双轮 GC：第一轮发现不可达并清除弱引用，第二轮兜底 finalizer/cleanup 时序
	runtime.GC()
	runtime.GC()

	if httpWeak.Value() != nil {
		t.Fatal("HTTP factory leaked: client still holds its config-change callback after Close (#135)")
	}
	if grpcWeak.Value() != nil {
		t.Fatal("gRPC factory leaked: client still holds its config-change callback after Close (#135)")
	}
}

// TestHTTPEndpointFactoryClose_ConfigEventNoLongerResetsCache 是 #135 的行为
// 回归测试：Close 前，client 的配置变更事件送达工厂回调、selector 缓存被
// 重置（重建时按新配置出新 selector）；Close 后回调已注销——配置事件不再
// 打在死工厂上，缓存保持稳定。
func TestHTTPEndpointFactoryClose_ConfigEventNoLongerResetsCache(t *testing.T) {
	store := newCapturingConfigStore(`{"lb":"roundrobin"}`)
	c := nnsclient.New(nnsclient.WithConfigStore(store))
	factory := NewHTTPEndpointFactory(c)

	ctx := context.Background()
	c.WatchService(ctx, "svc-a")

	sel1, err := factory.selectorCache.getOrCreate(ctx, "svc-a")
	if err != nil {
		t.Fatalf("getOrCreate first: %v", err)
	}
	if got := selectorKind(sel1); got != selectorKind(mustRoundRobin(t)) {
		t.Fatalf("precondition: expected roundrobin selector, got %s", got)
	}

	// Close 前：配置事件 → 回调重置缓存 → 重建按新配置 p2c 出新实例
	store.setContent(`{"lb":"p2c"}`)
	store.fire(t, "svc-a")
	sel2, err := factory.selectorCache.getOrCreate(ctx, "svc-a")
	if err != nil {
		t.Fatalf("getOrCreate after config event: %v", err)
	}
	if sel2 == sel1 {
		t.Fatal("config event must reset the selector cache while the factory is open")
	}
	if got := selectorKind(sel2); got != selectorKind(mustP2C(t)) {
		t.Fatalf("expected p2c selector rebuilt from new config after event, got %s", got)
	}

	// Close 后：回调已注销，配置事件不再重置缓存（死工厂不被事件打扰）
	factory.Close()
	store.setContent(`{"lb":"roundrobin"}`)
	store.fire(t, "svc-a")
	sel3, err := factory.selectorCache.getOrCreate(ctx, "svc-a")
	if err != nil {
		t.Fatalf("getOrCreate after close: %v", err)
	}
	if sel3 != sel2 {
		t.Fatal("factory closed: config event must no longer reset the selector cache (#135)")
	}
}

// TestGRPCEndpointFactoryClose_ConfigEventNoLongerResetsCache 与 HTTP 侧对称：
// gRPC 工厂 Close 后配置事件同样不再重置其 selector 缓存（#135 两工厂同修）。
func TestGRPCEndpointFactoryClose_ConfigEventNoLongerResetsCache(t *testing.T) {
	store := newCapturingConfigStore(`{"lb":"roundrobin"}`)
	c := nnsclient.New(nnsclient.WithConfigStore(store))
	factory := NewGRPCEndpointFactory(c)

	ctx := context.Background()
	c.WatchService(ctx, "svc-a")

	sel1, err := factory.selectorCache.getOrCreate(ctx, "svc-a")
	if err != nil {
		t.Fatalf("getOrCreate first: %v", err)
	}

	store.setContent(`{"lb":"p2c"}`)
	store.fire(t, "svc-a")
	sel2, err := factory.selectorCache.getOrCreate(ctx, "svc-a")
	if err != nil {
		t.Fatalf("getOrCreate after config event: %v", err)
	}
	if sel2 == sel1 {
		t.Fatal("config event must reset the selector cache while the factory is open")
	}

	factory.Close()
	store.setContent(`{"lb":"roundrobin"}`)
	store.fire(t, "svc-a")
	sel3, err := factory.selectorCache.getOrCreate(ctx, "svc-a")
	if err != nil {
		t.Fatalf("getOrCreate after close: %v", err)
	}
	if sel3 != sel2 {
		t.Fatal("factory closed: config event must no longer reset the selector cache (#135)")
	}
}

// TestFactoryClose_Idempotent 钉住重复 Close 边界：句柄注销幂等，两工厂
// 的 Close 可安全重复调用（不 panic、不影响 client 其他回调）。
func TestFactoryClose_Idempotent(t *testing.T) {
	c := newTestServiceClient(t)

	httpFactory := NewHTTPEndpointFactory(c)
	httpFactory.Close()
	httpFactory.Close()

	grpcFactory := NewGRPCEndpointFactory(c)
	grpcFactory.Close()
	grpcFactory.Close()
}

// mustRoundRobin 构造 roundrobin selector（仅用于类型名比对）。
func mustRoundRobin(t *testing.T) loadbalance.Selector {
	t.Helper()
	sel, err := loadbalance.NewSelector(loadbalance.StrategyRoundRobin)
	if err != nil {
		t.Fatalf("NewSelector(roundrobin): %v", err)
	}
	return sel
}

// mustP2C 构造 p2c selector（仅用于类型名比对）。
func mustP2C(t *testing.T) loadbalance.Selector {
	t.Helper()
	sel, err := loadbalance.NewSelector(loadbalance.StrategyP2C)
	if err != nil {
		t.Fatalf("NewSelector(p2c): %v", err)
	}
	return sel
}
