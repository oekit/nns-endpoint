package endpoint

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/wrapperspb"
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
	conn1, err := pool.Get(context.Background(), "127.0.0.1:0", withTestInsecureCreds())
	if err != nil {
		t.Fatalf("Get first: %v", err)
	}
	if conn1 == nil {
		t.Fatal("Get first returned nil conn")
	}
	defer conn1.Close()

	conn2, err := pool.Get(context.Background(), "127.0.0.1:0", withTestInsecureCreds())
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

	conn1, err := pool.Get(context.Background(), "127.0.0.1:0", withTestInsecureCreds())
	if err != nil {
		t.Fatalf("Get first: %v", err)
	}
	defer conn1.Close()

	conn2, err := pool.Get(context.Background(), "127.0.0.1:1", withTestInsecureCreds())
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
		withTestInsecureCreds(),
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

	conn1, err := pool.Get(context.Background(), "127.0.0.1:0", withTestInsecureCreds())
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
	conn2, err := pool.Get(context.Background(), "127.0.0.1:0", withTestInsecureCreds())
	if err != nil {
		t.Fatalf("Get after close: %v", err)
	}
	defer conn2.Close()
	if conn1 == conn2 {
		t.Fatal("expected new conn after Close, got cached one")
	}
}

// TestGRPCPool_Get_RequiresExplicitCredentials 是 #77 的回归测试：
// 池不再默认注入 insecure 凭据——不传传输凭据必须得到 grpc 的
// "no transport security set" 错误（fail-closed，静默明文才是缺陷），
// 显式传入 insecure 凭据则正常建连。
func TestGRPCPool_Get_RequiresExplicitCredentials(t *testing.T) {
	pool := NewGRPCPool()
	defer pool.Close()

	// 不传凭据：显式报错，而非静默明文
	_, err := pool.Get(context.Background(), "127.0.0.1:0")
	if err == nil {
		t.Fatal("expected no-transport-security error, got nil")
	}
	if !containsAny(err.Error(), "no transport security set") {
		t.Errorf("expected 'no transport security set' error, got: %v", err)
	}

	// 显式明文凭据：正常返回连接
	conn, err := pool.Get(context.Background(), "127.0.0.1:0", withTestInsecureCreds())
	if err != nil {
		t.Fatalf("Get with explicit insecure creds: %v", err)
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

	conn1, err := pool.Get(context.Background(), "127.0.0.1:0", withTestInsecureCreds())
	if err != nil {
		t.Fatalf("Get first: %v", err)
	}
	// Manually close the connection (simulating a remote Close) — the
	// pool should detect the Shutdown state on the next Get and recreate.
	conn1.Close()

	conn2, err := pool.Get(context.Background(), "127.0.0.1:0", withTestInsecureCreds())
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

// TestGRPCEndpoint_ResetSelector verifies that gRPC ResetSelector drops
// the cached selector. (HTTP equivalent is covered in factory_test.go.)
func TestGRPCEndpoint_ResetSelector(t *testing.T) {
	c := newTestServiceClient(t)
	factory := NewGRPCEndpointFactory(c)
	defer factory.Close()

	b1, _ := factory.selectorCache.getOrCreate(context.Background(), "svc-a")
	factory.ResetSelector("svc-a")
	b2, _ := factory.selectorCache.getOrCreate(context.Background(), "svc-a")

	if b1 == b2 {
		t.Fatal("expected new selector after ResetSelector, got cached one")
	}
}

// TestGRPCEndpoint_Close verifies Close calls pool.Close.
func TestGRPCEndpoint_Close(t *testing.T) {
	c := newTestServiceClient(t)
	factory := NewGRPCEndpointFactory(c)
	// Pre-populate the pool with a connection.
	conn, err := factory.pool.Get(context.Background(), "127.0.0.1:0", withTestInsecureCreds())
	if err != nil {
		t.Fatalf("pool.Get: %v", err)
	}
	defer conn.Close()
	factory.Close()
	// After Close, the pool's internal map should be empty.
	// We can't directly inspect the map (unexported), but a new Get
	// must return a new conn.
	conn2, err := factory.pool.Get(context.Background(), "127.0.0.1:0", withTestInsecureCreds())
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
//   - ref=true 时在途引用加一；
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

	// 1) 现役条目：返回同一连接并刷新 lastUsed（无引用借出，inFlight 不变）
	before := entry.lastUsed
	conn, ok := pool.touch(addr, entry, false)
	if !ok {
		t.Fatal("touch: expected ok=true for live entry")
	}
	if conn != entry.conn {
		t.Fatal("touch: expected the live entry's conn")
	}
	pool.mu.RLock()
	refreshed := pool.conns[addr].lastUsed
	inFlight := pool.conns[addr].inFlight
	pool.mu.RUnlock()
	if !refreshed.After(before) {
		t.Error("touch: expected lastUsed to be refreshed")
	}
	if inFlight != 0 {
		t.Errorf("touch(ref=false): expected inFlight unchanged, got %d", inFlight)
	}

	// 2) ref=true：在途引用加一（Acquire 借出口径）
	if _, ok := pool.touch(addr, entry, true); !ok {
		t.Fatal("touch(ref=true): expected ok=true for live entry")
	}
	pool.mu.RLock()
	inFlight = pool.conns[addr].inFlight
	pool.mu.RUnlock()
	if inFlight != 1 {
		t.Errorf("touch(ref=true): expected inFlight=1, got %d", inFlight)
	}

	// 3) entry 已被替换：返回 (nil, false)
	replacement := &connEntry{conn: newTestClientConn(t, "127.0.0.1:2"), lastUsed: time.Now()}
	pool.mu.Lock()
	pool.conns[addr] = replacement
	pool.mu.Unlock()
	if c, ok := pool.touch(addr, entry, false); ok || c != nil {
		t.Fatalf("touch: expected (nil, false) for replaced entry, got (%v, %v)", c, ok)
	}

	// 4) entry 已被删除：返回 (nil, false)
	pool.mu.Lock()
	delete(pool.conns, addr)
	pool.mu.Unlock()
	if c, ok := pool.touch(addr, replacement, false); ok || c != nil {
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
				conn, err := pool.Get(context.Background(), addr, withTestInsecureCreds())
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

// withTestInsecureCreds 显式传入明文传输凭据（池不再默认注入，见 #77）。
func withTestInsecureCreds() grpc.DialOption {
	return grpc.WithTransportCredentials(insecure.NewCredentials())
}

// inFlightOf 读取指定地址条目的在途引用计数（条目不存在返回 -1）。
// 仅供测试观察池内部记账。
func inFlightOf(p *GRPCPool, addr string) int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	e, ok := p.conns[addr]
	if !ok {
		return -1
	}
	return e.inFlight
}

// TestGRPCPool_Acquire_SparesInFlightFromIdleEvict 是 #99 的回归测试（忠实
// 复刻 issue 复现件）：长生命周期调用（此处以阻塞一元调用模拟流式占用）
// 跨越空闲驱逐时刻——借出口径下条目早已「空闲超龄」，修复前第二次 Get
// 触发同地址驱逐、Close 掉在途连接（在途 RPC 立即报 Unavailable）；修复后
// 在途引用保护连接，RPC 正常完成。
func TestGRPCPool_Acquire_SparesInFlightFromIdleEvict(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	dialOpts := newBufconnGRPCServer(t, "nns.test.EchoService", map[string]func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error){
		"Slow": func(_ context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			close(entered)
			<-release
			return wrapperspb.String("done:" + req.GetValue()), nil
		},
	})

	pool := NewGRPCPool()
	pool.idleTTL = 250 * time.Millisecond
	defer pool.Close()

	const addr = "127.0.0.1:1"
	conn, err := pool.Acquire(context.Background(), addr, dialOpts...)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if got := inFlightOf(pool, addr); got != 1 {
		t.Fatalf("expected inFlight=1 after Acquire, got %d", got)
	}

	invokeErr := make(chan error, 1)
	go func() {
		reply := &wrapperspb.StringValue{}
		invokeErr <- conn.Invoke(context.Background(), "/nns.test.EchoService/Slow", wrapperspb.String("x"), reply)
	}()
	<-entered // RPC 已到达服务端、正占用连接

	// 超过 idleTTL：借出口径（lastUsed=借出时刻）下条目已「空闲超龄」
	time.Sleep(400 * time.Millisecond)

	// 同地址 Get 触发空闲驱逐路径：修复前这里 Close 掉在途连接
	reused, err := pool.Get(context.Background(), addr, dialOpts...)
	if err != nil {
		t.Fatalf("Get during in-flight RPC: %v", err)
	}
	if reused != conn {
		t.Fatal("expected the in-flight conn to be reused, not evicted and recreated")
	}

	// 在途 RPC 必须正常完成
	close(release)
	select {
	case callErr := <-invokeErr:
		if callErr != nil {
			t.Fatalf("in-flight RPC killed by idle eviction: %v", callErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for in-flight RPC")
	}

	// 归还并过期后：驱逐恢复（inFlight==0 判据解除）
	pool.Release(addr, conn)
	if got := inFlightOf(pool, addr); got != 0 {
		t.Fatalf("expected inFlight=0 after Release, got %d", got)
	}
	pool.mu.Lock()
	pool.conns[addr].lastUsed = time.Now().Add(-time.Hour)
	pool.mu.Unlock()
	fresh, err := pool.Get(context.Background(), addr, dialOpts...)
	if err != nil {
		t.Fatalf("Get after release+expiry: %v", err)
	}
	if fresh == conn {
		t.Fatal("expected expired conn to be evicted and recreated after Release")
	}
}

// TestGRPCPool_Acquire_FullPoolEvictSkipsInFlight 覆盖满池驱逐路径的 #99
// 语义：evictIdleLocked 必须跳过有在途引用的条目——满池时宁可拒绝创建
// （#72/#76 的既有拒绝语义），也不 Close 在途连接。
func TestGRPCPool_Acquire_FullPoolEvictSkipsInFlight(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	dialOpts := newBufconnGRPCServer(t, "nns.test.EchoService", map[string]func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error){
		"Slow": func(_ context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			close(entered)
			<-release
			return wrapperspb.String("done:" + req.GetValue()), nil
		},
	})

	pool := NewGRPCPoolWithSize(1)
	pool.idleTTL = 250 * time.Millisecond
	defer pool.Close()

	const (
		addrA = "10.9.0.1:1"
		addrB = "10.9.0.2:2"
	)

	connA, err := pool.Acquire(context.Background(), addrA, dialOpts...)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	invokeErr := make(chan error, 1)
	go func() {
		reply := &wrapperspb.StringValue{}
		invokeErr <- connA.Invoke(context.Background(), "/nns.test.EchoService/Slow", wrapperspb.String("a"), reply)
	}()
	<-entered
	time.Sleep(400 * time.Millisecond) // A 超过 idleTTL（借出口径），但在途引用持有中

	// 满池创建新地址：驱逐跳过在用的 A → 拒绝创建
	_, err = pool.Get(context.Background(), addrB, dialOpts...)
	if err == nil || !containsAny(err.Error(), "max size") {
		t.Fatalf("expected max size reached while in-flight conn occupies pool, got: %v", err)
	}

	// 在途 RPC 不受驱逐路径影响
	close(release)
	select {
	case callErr := <-invokeErr:
		if callErr != nil {
			t.Fatalf("in-flight RPC killed by full-pool eviction: %v", callErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for in-flight RPC")
	}

	// 归还并过期后：A 正常让位，B 创建成功
	pool.Release(addrA, connA)
	pool.mu.Lock()
	pool.conns[addrA].lastUsed = time.Now().Add(-time.Hour)
	pool.mu.Unlock()

	connB, err := pool.Get(context.Background(), addrB, dialOpts...)
	if err != nil {
		t.Fatalf("Get new addr after release+expiry: %v", err)
	}
	if connB == nil {
		t.Fatal("Get new addr returned nil conn")
	}
	if state := connA.GetState(); state != connectivity.Shutdown {
		t.Errorf("expected released+expired conn evicted (Shutdown), got %v", state)
	}
}

// TestGRPCPool_Acquire_ReusesCachedConnection 验证 Acquire 与 Get 共享同一
// 连接缓存，且在途引用随配对 Acquire/Release 增减、Get（无引用借出）不动账。
func TestGRPCPool_Acquire_ReusesCachedConnection(t *testing.T) {
	pool := NewGRPCPool()
	defer pool.Close()

	const addr = "10.9.2.1:1"
	conn1, err := pool.Acquire(context.Background(), addr, withTestInsecureCreds())
	if err != nil {
		t.Fatalf("Acquire first: %v", err)
	}
	conn2, err := pool.Acquire(context.Background(), addr, withTestInsecureCreds())
	if err != nil {
		t.Fatalf("Acquire second: %v", err)
	}
	if conn1 != conn2 {
		t.Fatal("expected Acquire to reuse the cached conn")
	}
	if got := inFlightOf(pool, addr); got != 2 {
		t.Fatalf("expected inFlight=2 after two Acquires, got %d", got)
	}

	// Get 是无引用借出：不改在途引用
	if _, err := pool.Get(context.Background(), addr, withTestInsecureCreds()); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := inFlightOf(pool, addr); got != 2 {
		t.Fatalf("expected Get to leave inFlight unchanged, got %d", got)
	}

	pool.Release(addr, conn1)
	pool.Release(addr, conn2)
	if got := inFlightOf(pool, addr); got != 0 {
		t.Fatalf("expected inFlight=0 after paired Releases, got %d", got)
	}
}

// TestGRPCPool_Release_IdentityCheck 验证 Release 的配对语义（#99）：
//   - 归还与借出不配对（连接已被替换/驱逐）→ no-op；
//   - 配对归还 → 引用减一、lastUsed 刷新（空闲计时从归还时刻起算）；
//   - 归还后过期 → 连接重新参与空闲驱逐。
func TestGRPCPool_Release_IdentityCheck(t *testing.T) {
	pool := NewGRPCPool()
	defer pool.Close()

	const addr = "10.9.1.1:1"
	conn, err := pool.Acquire(context.Background(), addr, withTestInsecureCreds())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	// 1) 不配对的归还：no-op，在途引用不变
	stranger := newTestClientConn(t, "127.0.0.1:9")
	defer stranger.Close()
	pool.Release(addr, stranger)
	if got := inFlightOf(pool, addr); got != 1 {
		t.Fatalf("Release with foreign conn must be a no-op, inFlight=%d", got)
	}

	// 2) 配对归还：引用减一，lastUsed 刷新
	pool.mu.Lock()
	pool.conns[addr].lastUsed = time.Now().Add(-time.Hour)
	pool.mu.Unlock()
	pool.Release(addr, conn)
	if got := inFlightOf(pool, addr); got != 0 {
		t.Fatalf("expected inFlight=0 after paired Release, got %d", got)
	}
	pool.mu.RLock()
	age := time.Since(pool.conns[addr].lastUsed)
	pool.mu.RUnlock()
	if age > time.Minute {
		t.Fatalf("expected lastUsed refreshed on Release, age=%v", age)
	}

	// 3) 归还后过期：重新可驱逐
	pool.mu.Lock()
	pool.conns[addr].lastUsed = time.Now().Add(-time.Hour)
	pool.mu.Unlock()
	fresh, err := pool.Get(context.Background(), addr, withTestInsecureCreds())
	if err != nil {
		t.Fatalf("Get after release+expiry: %v", err)
	}
	if fresh == conn {
		t.Fatal("expected expired conn to be evicted and recreated after Release")
	}
	if state := conn.GetState(); state != connectivity.Shutdown {
		t.Errorf("expected old conn Shutdown after eviction, got %v", state)
	}
}

// TestGRPCPool_Create_FullPoolRejectsWhenNoIdle 覆盖满池拒绝路径的边界：
// 池满且没有空闲超龄条目可驱逐时，仍直接拒绝（max size reached）。
func TestGRPCPool_Create_FullPoolRejectsWhenNoIdle(t *testing.T) {
	pool := NewGRPCPoolWithSize(1)
	defer pool.Close()

	conn1, err := pool.Get(context.Background(), "10.0.0.1:1", withTestInsecureCreds())
	if err != nil {
		t.Fatalf("Get first: %v", err)
	}
	// 归还借出引用（#99）：让驱逐判据只看 lastUsed 新鲜度
	pool.Release("10.0.0.1:1", conn1)

	_, err = pool.Get(context.Background(), "10.0.0.2:2", withTestInsecureCreds())
	if err == nil || !containsAny(err.Error(), "max size") {
		t.Fatalf("expected max size reached (fresh pool, nothing idle), got: %v", err)
	}
}

// TestGRPCPool_Create_FullPoolEvictsIdleOtherAddr 是 #72/#76 的回归测试：
// 满池创建新地址连接前，必须先全局驱逐**其他地址**的空闲超龄条目。
// 场景：滚动发布换 IP 后旧地址连接无人再 Get——修复前空闲驱逐只在
// Get(同 addr) 路径生效，池被陈旧连接占满时新地址一律 max size reached。
func TestGRPCPool_Create_FullPoolEvictsIdleOtherAddr(t *testing.T) {
	pool := NewGRPCPoolWithSize(1)
	defer pool.Close()

	const (
		addrA = "10.0.0.1:1" // 旧实例（滚动发布后无人再访问）
		addrB = "10.0.0.2:2" // 服务发现返回的新实例
	)
	connA, err := pool.Get(context.Background(), addrA, withTestInsecureCreds())
	if err != nil {
		t.Fatalf("Get old addr: %v", err)
	}
	// 归还借出引用（#99）：让旧地址条目重新回到「可按空闲判据驱逐」状态
	pool.Release(addrA, connA)

	// A 仍在活跃窗口内：无可驱逐条目，新地址被拒（拒绝语义保持）
	if _, err = pool.Get(context.Background(), addrB, withTestInsecureCreds()); err == nil || !containsAny(err.Error(), "max size") {
		t.Fatalf("expected max size reached while old entry fresh, got: %v", err)
	}

	// 将 A 置为空闲超龄（模拟旧地址长期无人 Get）
	pool.mu.Lock()
	pool.conns[addrA].lastUsed = time.Now().Add(-(pool.idleTTL + time.Second))
	pool.mu.Unlock()

	// 新地址 Get：满池驱逐 A 后创建成功（修复前此处直接 max size reached）
	connB, err := pool.Get(context.Background(), addrB, withTestInsecureCreds())
	if err != nil {
		t.Fatalf("Get new addr after idle expiry: %v", err)
	}
	if connB == nil {
		t.Fatal("Get new addr returned nil conn")
	}

	// A 已被驱逐关闭且条目删除，池内只剩新地址
	pool.mu.RLock()
	_, hasA := pool.conns[addrA]
	_, hasB := pool.conns[addrB]
	pool.mu.RUnlock()
	if hasA {
		t.Error("expected idle old-addr entry to be evicted, still in pool")
	}
	if !hasB {
		t.Error("expected new-addr entry in pool")
	}
	if state := connA.GetState(); state != connectivity.Shutdown {
		t.Errorf("expected evicted old conn Shutdown, got %v", state)
	}
}
