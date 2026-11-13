package endpoint

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
)

// TestGRPCPool_Close_TerminalGate 回归 #148：Close 是终结操作——Close 后
// Get/Acquire 一律返回 ErrPoolClosed（errors.Is 可判定），不再静默重建连接。
// 此前 Close 只清理现存连接、不登记终态，此后的 Get/Acquire 走 create()
// 照常 grpc.NewClient 写回池——池在「终结」后照常复活。
func TestGRPCPool_Close_TerminalGate(t *testing.T) {
	pool := NewGRPCPool()

	// 前置：Close 前正常建连（NewClient 非阻塞，无需真实 server）
	conn, err := pool.Get(context.Background(), "127.0.0.1:0", withTestInsecureCreds())
	if err != nil {
		t.Fatalf("Get before close: %v", err)
	}
	_ = conn.Close()

	pool.Close()

	if _, err := pool.Get(context.Background(), "127.0.0.1:0", withTestInsecureCreds()); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("#148 回归：Close 后 Get = %v, want ErrPoolClosed（终态闸未生效）", err)
	}
	if _, err := pool.Acquire(context.Background(), "127.0.0.1:0", withTestInsecureCreds()); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("#148 回归：Close 后 Acquire = %v, want ErrPoolClosed（终态闸未生效）", err)
	}

	// 幂等：重复 Close 安全（终态已登记，不 panic）
	pool.Close()
}

// TestGRPCPool_Close_NoConnRevival_LeakRegression 是 #148 的泄漏回归测试：
// Close 后 N 次 Acquire 连接数不涨。旧实现每次调用新建一条连接写回池，
// 直至 maxSize 后靠 idleTTL 驱逐勉强自净——但驱逐只关旧连接、新调用继续
// 新建，长期停机尾流量下无上界循环。
func TestGRPCPool_Close_NoConnRevival_LeakRegression(t *testing.T) {
	pool := NewGRPCPool()

	if _, err := pool.Acquire(context.Background(), "127.0.0.1:0", withTestInsecureCreds()); err != nil {
		t.Fatalf("Acquire before close: %v", err)
	}
	pool.Close()

	for i := 0; i < 50; i++ {
		if _, err := pool.Acquire(context.Background(), "127.0.0.1:0", withTestInsecureCreds()); !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("Acquire #%d after close = %v, want ErrPoolClosed", i, err)
		}
	}

	pool.mu.Lock()
	size := len(pool.conns)
	pool.mu.Unlock()
	if size != 0 {
		t.Fatalf("#148 泄漏回归：Close 后 50 次 Acquire 连接数 = %d, want 0（池在终结后被静默重建）", size)
	}
}

// TestGRPCPool_ConcurrentCloseAndGet 终态闸的并发形态钉：Close 与 Get/Acquire
// 竞态下，Get/Acquire 要么成功（Close 前已建连）要么返回 ErrPoolClosed，
// 绝不出现「Close 后新建连接」（-race 下回归钉）。
func TestGRPCPool_ConcurrentCloseAndGet(t *testing.T) {
	pool := NewGRPCPool()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			conn, err := pool.Get(context.Background(), "127.0.0.1:0", withTestInsecureCreds())
			if err != nil {
				if !errors.Is(err, ErrPoolClosed) {
					t.Errorf("Get = %v, want nil or ErrPoolClosed", err)
				}
				return
			}
			_ = conn.Close()
		}
	}()
	pool.Close()
	<-done

	if _, err := pool.Acquire(context.Background(), "127.0.0.1:0", withTestInsecureCreds()); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("Acquire after close = %v, want ErrPoolClosed", err)
	}
}

// TestGRPCEndpointFactoryClose_EndpointShortCircuits 回归 #148：工厂 Close
// 后存量端点闭包短路返回 ErrFactoryClosed（localError 归类），不再触达
// 连接池——优雅停机窗口的在途请求、双路关停竞态下未同步的消费方拿到
// 明确错误。
func TestGRPCEndpointFactoryClose_EndpointShortCircuits(t *testing.T) {
	c := newTestServiceClient(t)
	factory := NewGRPCEndpointFactory(c)

	ep, err := factory.MakeEndpoint("svc-a", withTestInsecureCreds())
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	factory.Close()

	// 闭包首段即短路：请求参数尚未校验就返回终态错误
	_, err = ep(context.Background(), GRPCRequest{})
	if !errors.Is(err, ErrFactoryClosed) {
		t.Fatalf("#148 回归：工厂 Close 后端点调用 = %v, want ErrFactoryClosed", err)
	}
	if !localFault(err) {
		t.Fatalf("#148 回归：终态错误应归入 localError（不计熔断样本/节点 EWMA）, got %v", err)
	}

	// 重复 Close 幂等，端点仍短路
	factory.Close()
	if _, err := ep(context.Background(), GRPCRequest{}); !errors.Is(err, ErrFactoryClosed) {
		t.Fatalf("endpoint after repeated Close = %v, want ErrFactoryClosed", err)
	}
}

// 编译期守卫：grpc.DialOption 参数形态与工厂签名一致（防止 opts 形参漂移）。
var _ = func() grpc.DialOption { return withTestInsecureCreds() }
