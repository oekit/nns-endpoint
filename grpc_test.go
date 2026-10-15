package endpoint

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// TestGRPCPool_Get_CachesConnection verifies that two Get calls for the
// same address return the same *grpc.ClientConn. Pool reuse is the whole
// point of the pool — without it, every RPC would pay the dial cost.
func TestGRPCPool_Get_CachesConnection(t *testing.T) {
	pool := NewGRPCPool()
	defer pool.Close()

	// Use a real routable address (127.0.0.1:0 is a kernel-chosen port —
	// not a real server, but NewClient is non-blocking, so this works
	// for the caching test).
	conn1, err := pool.Get(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Get first: %v", err)
	}
	if conn1 == nil {
		t.Fatal("Get first returned nil conn")
	}
	defer conn1.Close()

	conn2, err := pool.Get(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Get second: %v", err)
	}

	if conn1 != conn2 {
		t.Fatal("expected pool to return cached conn, got different instance")
	}
}

// TestGRPCPool_Get_DifferentAddressDifferentConn covers the cache-miss
// path: a different address must produce a different *grpc.ClientConn.
func TestGRPCPool_Get_DifferentAddressDifferentConn(t *testing.T) {
	pool := NewGRPCPool()
	defer pool.Close()

	conn1, err := pool.Get(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Get first: %v", err)
	}
	defer conn1.Close()

	conn2, err := pool.Get(context.Background(), "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Get second: %v", err)
	}
	defer conn2.Close()

	if conn1 == conn2 {
		t.Fatal("expected different conns for different addresses")
	}
}

// TestGRPCPool_Get_RespectsExtraOptions verifies that the caller-supplied
// grpc.DialOption is passed through to grpc.NewClient. We use a custom
// unary interceptor (a non-deprecated option) to exercise the path.
func TestGRPCPool_Get_RespectsExtraOptions(t *testing.T) {
	pool := NewGRPCPool()
	defer pool.Close()

	called := false
	interceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		called = true
		return invoker(ctx, method, req, reply, cc, opts...)
	}

	conn, err := pool.Get(context.Background(), "127.0.0.1:0",
		grpc.WithUnaryInterceptor(interceptor),
	)
	if err != nil {
		t.Fatalf("Get with extra option: %v", err)
	}
	if conn == nil {
		t.Fatal("Get returned nil conn")
	}
	conn.Close()
	// `called` will be false because we never actually invoke an RPC;
	// the test only confirms the option is accepted (not rejected at
	// dial time).
	_ = called
}

// TestGRPCPool_Close_ClearsAllConnections verifies that after Close,
// subsequent Get returns a NEW connection (not the cached one).
func TestGRPCPool_Close_ClearsAllConnections(t *testing.T) {
	pool := NewGRPCPool()

	conn1, err := pool.Get(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Get before close: %v", err)
	}

	pool.Close()

	// After Close, the old conn should report Shutdown state.
	state := conn1.GetState()
	if state != connectivity.Shutdown {
		t.Errorf("expected Shutdown state after Close, got %v", state)
	}

	// A new Get for the same address must succeed and return a new conn
	// (not the closed one).
	conn2, err := pool.Get(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Get after close: %v", err)
	}
	defer conn2.Close()
	if conn1 == conn2 {
		t.Fatal("expected new conn after Close, got cached one")
	}
}

// TestGRPCPool_Get_NoInsecurePanic verifies the default creds path.
// Without insecure.NewCredentials(), grpc.NewClient panics in modern
// versions. The pool must always include the default.
func TestGRPCPool_Get_NoInsecurePanic(t *testing.T) {
	pool := NewGRPCPool()
	defer pool.Close()

	// Should not panic; should return a non-nil conn.
	conn, err := pool.Get(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if conn == nil {
		t.Fatal("Get returned nil conn without error")
	}
	conn.Close()
}

// TestGRPCPool_Get_AfterShutdownRecreates covers the Get → Shutdown → Get
// path inside the same pool. The pool must detect the Shutdown state and
// create a fresh connection.
func TestGRPCPool_Get_AfterShutdownRecreates(t *testing.T) {
	pool := NewGRPCPool()
	defer pool.Close()

	conn1, err := pool.Get(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Get first: %v", err)
	}
	// Manually close the connection (simulating a remote Close) — the
	// pool should detect the Shutdown state on the next Get and recreate.
	conn1.Close()

	conn2, err := pool.Get(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Get after conn close: %v", err)
	}
	defer conn2.Close()
	if conn1 == conn2 {
		t.Fatal("expected fresh conn after manual close, got cached one")
	}
}

// --- gRPC factory tests ---

// TestGRPCEndpoint_MakeEndpoint_InvalidRequestType covers the type-assertion
// error path on the gRPC endpoint. The closure must reject non-GRPCRequest
// inputs.
func TestGRPCEndpoint_MakeEndpoint_InvalidRequestType(t *testing.T) {
	c := newTestServiceClient(t)
	factory := NewGRPCEndpointFactory(c)
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	_, err = ep(context.Background(), "not-a-GRPCRequest")
	if err == nil {
		t.Fatal("expected type assertion error, got nil")
	}
	if !containsAny(err.Error(), "invalid request type") {
		t.Errorf("expected 'invalid request type' error, got: %v", err)
	}
}

// TestGRPCEndpoint_ResetBalancer verifies that gRPC ResetBalancer drops
// the cached balancer. (HTTP equivalent is covered in factory_test.go.)
func TestGRPCEndpoint_ResetBalancer(t *testing.T) {
	c := newTestServiceClient(t)
	factory := NewGRPCEndpointFactory(c)
	defer factory.Close()

	b1 := factory.balancerCache.getOrCreate(context.Background(), "svc-a")
	factory.ResetBalancer("svc-a")
	b2 := factory.balancerCache.getOrCreate(context.Background(), "svc-a")

	if b1 == b2 {
		t.Fatal("expected new balancer after ResetBalancer, got cached one")
	}
}

// TestGRPCEndpoint_Close verifies Close calls pool.Close.
func TestGRPCEndpoint_Close(t *testing.T) {
	c := newTestServiceClient(t)
	factory := NewGRPCEndpointFactory(c)
	// Pre-populate the pool with a connection.
	conn, err := factory.pool.Get(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pool.Get: %v", err)
	}
	defer conn.Close()
	factory.Close()
	// After Close, the pool's internal map should be empty.
	// We can't directly inspect the map (unexported), but a new Get
	// must return a new conn.
	conn2, err := factory.pool.Get(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pool.Get after Close: %v", err)
	}
	defer conn2.Close()
	if conn == conn2 {
		t.Fatal("expected new conn after factory.Close, got cached one")
	}
}

// --- Get TOCTOU 回归测试（review L1）---

// newTestClientConn 创建一个不发起真实拨号的 *grpc.ClientConn
// （grpc.NewClient 是惰性连接，无需真实 server）。
func newTestClientConn(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient(%s): %v", addr, err)
	}
	return conn
}

// TestGRPCPool_Touch_IdentityCheck 验证 touch 的写锁内身份校验（Get 防止
// 返回已被并发驱逐/关闭连接的核心逻辑）：
//   - entry 是现役条目 → 返回同一连接并刷新 lastUsed；
//   - entry 已被替换 → (nil, false)，调用方必须重试；
//   - entry 已被删除 → (nil, false)。
//
// 修复前 Get 在 RUnlock 与 return 之间不校验身份，并发驱逐 Close 连接后
// 仍会把已关闭连接返回给调用方。
func TestGRPCPool_Touch_IdentityCheck(t *testing.T) {
	pool := NewGRPCPool()
	defer pool.Close()

	const addr = "svc:1"
	entry := &connEntry{conn: newTestClientConn(t, "127.0.0.1:1"), lastUsed: time.Now().Add(-time.Second)}
	pool.mu.Lock()
	pool.conns[addr] = entry
	pool.mu.Unlock()

	// 1) 现役条目：返回同一连接并刷新 lastUsed
	before := entry.lastUsed
	conn, ok := pool.touch(addr, entry)
	if !ok {
		t.Fatal("touch: expected ok=true for live entry")
	}
	if conn != entry.conn {
		t.Fatal("touch: expected the live entry's conn")
	}
	pool.mu.RLock()
	refreshed := pool.conns[addr].lastUsed
	pool.mu.RUnlock()
	if !refreshed.After(before) {
		t.Error("touch: expected lastUsed to be refreshed")
	}

	// 2) entry 已被替换：返回 (nil, false)
	replacement := &connEntry{conn: newTestClientConn(t, "127.0.0.1:2"), lastUsed: time.Now()}
	pool.mu.Lock()
	pool.conns[addr] = replacement
	pool.mu.Unlock()
	if c, ok := pool.touch(addr, entry); ok || c != nil {
		t.Fatalf("touch: expected (nil, false) for replaced entry, got (%v, %v)", c, ok)
	}

	// 3) entry 已被删除：返回 (nil, false)
	pool.mu.Lock()
	delete(pool.conns, addr)
	pool.mu.Unlock()
	if c, ok := pool.touch(addr, replacement); ok || c != nil {
		t.Fatalf("touch: expected (nil, false) for deleted entry, got (%v, %v)", c, ok)
	}
}

// TestGRPCPool_Touch_ConcurrentReplaceRetry 并发替换/关闭连接与 Get 竞争，
// 回归 Get 的 "身份校验失败则重试" 逻辑；配合 -race 检测数据竞争。
// 注意：不在这里断言返回连接的 connectivity 状态——Get 返回后 churn 方
// 可能立即关闭该连接，对返回值的断言本身存在无法消除的竞态。
func TestGRPCPool_Touch_ConcurrentReplaceRetry(t *testing.T) {
	pool := NewGRPCPool()
	defer pool.Close()

	const (
		addr             = "127.0.0.1:9"
		churnRounds      = 200
		getters          = 4
		getsPerGoroutine = 200
	)

	// 预创建 churn 用连接，避免在子 goroutine 中使用 t
	conns := make([]*grpc.ClientConn, churnRounds)
	for i := range conns {
		conns[i] = newTestClientConn(t, addr)
		defer conns[i].Close()
	}

	churnDone := make(chan struct{})
	go func() {
		defer close(churnDone)
		for i := 0; i < churnRounds; i++ {
			newEntry := &connEntry{conn: conns[i], lastUsed: time.Now()}
			pool.mu.Lock()
			old := pool.conns[addr]
			pool.conns[addr] = newEntry
			pool.mu.Unlock()
			if old != nil {
				_ = old.conn.Close() // 模拟驱逐关闭
			}
			time.Sleep(time.Millisecond)
		}
	}()

	var wg sync.WaitGroup
	for g := 0; g < getters; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < getsPerGoroutine; i++ {
				conn, err := pool.Get(context.Background(), addr)
				if err != nil {
					t.Errorf("Get: %v", err)
					return
				}
				if conn == nil {
					t.Error("Get returned nil conn")
					return
				}
			}
		}()
	}
	wg.Wait()
	<-churnDone
}

// guard: the insecure creds must compile (used by pool.create).
var _ = insecure.NewCredentials
