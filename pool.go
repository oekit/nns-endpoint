package endpoint

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// defaultMaxPoolSize 是连接池默认最大连接数。
const defaultMaxPoolSize = 128

// defaultIdleTTL 是连接默认空闲超时时间。
const defaultIdleTTL = 10 * time.Minute

// connEntry 包装 gRPC 连接、最后使用时间与在途引用计数。
//
// inFlight 是「借出且尚未归还」的调用数（Acquire 加、Release 减），
// 由池写锁保护、无锁外访问。lastUsed 的口径是「最近一次借出/归还/
// 创建时刻」，覆盖不了「借出后仍在用」——长生命周期流式 RPC 可以
// 远超 idleTTL，在途引用计数才是驱逐的安全判据（#99）：空闲驱逐只
// 处理 inFlight==0 的条目。
type connEntry struct {
	conn     *grpc.ClientConn
	lastUsed time.Time
	inFlight int
}

// GRPCPool gRPC 连接池，复用 gRPC 客户端连接。
// 连接数超过 maxSize 时先全局驱逐空闲超龄条目，仍满则拒绝创建新连接。
// 空闲超过 idleTTL 的连接在 Get 时自动驱逐（同地址命中与满池创建两条
// 路径都会驱逐）；有在途引用（Acquire 后未 Release）的连接不参与驱逐
// （#99）——conn.Close 会终止其上全部活跃 RPC。
//
// 借还口径：Get 是无引用借出（与 v0.2.x 行为一致，适合短平快的一元
// 调用）；长生命周期流式 RPC 必须用 Acquire/Release 配对——在途期间
// 连接不会被任何驱逐路径关闭。
//
// 传输凭据（#77）：池**不注入**默认凭据——调用方必须经 opts 显式传入
// grpc.WithTransportCredentials(insecure.NewCredentials())（明文）或 TLS
// 凭据。缺凭据时 grpc.NewClient 返回 "no transport security set" 错误
// （fail-closed），不再静默降级明文。
type GRPCPool struct {
	mu      sync.RWMutex
	conns   map[string]*connEntry
	maxSize int
	idleTTL time.Duration
}

// NewGRPCPool 创建一个新的 gRPC 连接池实例。
//
// maxSize 可省略（缺省 defaultMaxPoolSize）；显式传入 ≤0 的值同样回退
// 默认值（原 NewGRPCPoolWithSize 的守卫语义）。两个构造器除 maxSize 外
// 逐行相同、且带尺寸形态全工作区仅测试使用（#134）——合并为单一构造器，
// 公开面少一个入口。
func NewGRPCPool(maxSize ...int) *GRPCPool {
	if len(maxSize) > 1 {
		// 构造期非法值 fail-fast（AGENTS.md Option 总则）：变参误用即编程错误
		panic("nns-endpoint: NewGRPCPool accepts at most one maxSize argument")
	}
	size := defaultMaxPoolSize
	if len(maxSize) == 1 && maxSize[0] > 0 {
		size = maxSize[0]
	}
	return &GRPCPool{
		conns:   make(map[string]*connEntry),
		maxSize: size,
		idleTTL: defaultIdleTTL,
	}
}

// Get 从连接池获取指定地址的 gRPC 连接，不存在、已关闭或空闲超时时创建新连接。
// opts 透传给 grpc.NewClient；必须包含传输凭据（明文 insecure / TLS），
// 缺凭据时返回 grpc 的 no transport security set 错误（#77）。
//
// Get 是**无引用借出**：不记在途引用，适合调用时长远小于 idleTTL 的
// 一元调用（端点工厂内部即此形态，超时上限有界）。调用时长可能超过
// idleTTL 的长生命周期流式 RPC 必须改用 Acquire/Release（#99）——否则
// 借出后连接仍可能因空闲判据（按「借出时刻」计）被驱逐关闭，终止
// 在途 RPC。
//
// 并发安全说明：读锁读取 entry 与写锁校验之间存在窗口，期间该 entry 可能被
// 并发驱逐（Close 后删除）或替换。因此返回前必须在写锁内校验 p.conns[addr]
// 仍等于该 entry，否则重试——绝不把一个可能已被 Close 的连接返回给调用方。
func (p *GRPCPool) Get(ctx context.Context, addr string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return p.get(ctx, addr, false, opts...)
}

// Acquire 从连接池借出指定地址的 gRPC 连接并记在途引用（#99）。
// 与 Get 的差别只在引用记账：借出期间（直到 Release）该连接不会被任何
// 空闲驱逐路径关闭——lastUsed 的「借出时刻」口径覆盖不了「借出后仍在
// 用」的长生命周期流式 RPC。调用方用毕必须 Release(addr, conn) 配对归还；
// 未归还的连接永不被空闲驱逐（至多被 Shutdown 替换或池 Close 终结）。
func (p *GRPCPool) Acquire(ctx context.Context, addr string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return p.get(ctx, addr, true, opts...)
}

// Release 归还 Acquire 借出的连接（在途引用减一，#99）。
// (addr, conn) 必须与 Acquire 的返回值配对：连接对应的条目已被替换/驱逐时
// 本次归还为 no-op（老条目的引用计数随其消亡，老连接必然已关闭或濒死，
// 无需保护）。归还即刷新 lastUsed——空闲计时从归还时刻起算。
func (p *GRPCPool) Release(addr string, conn *grpc.ClientConn) {
	if conn == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	entry, ok := p.conns[addr]
	if !ok || entry.conn != conn {
		return
	}
	if entry.inFlight > 0 {
		entry.inFlight--
	}
	entry.lastUsed = time.Now()
}

// get 是 Get/Acquire 的公共实现；ref=true 时借出记在途引用。
func (p *GRPCPool) get(ctx context.Context, addr string, ref bool, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	for {
		p.mu.RLock()
		entry, ok := p.conns[addr]
		var lastUsed time.Time
		if ok {
			// 锁内取副本：lastUsed 在别处由写锁保护更新，解锁后裸读是数据竞争
			lastUsed = entry.lastUsed
		}
		p.mu.RUnlock()

		if !ok {
			break // 不存在，走 create
		}

		if time.Since(lastUsed) > p.idleTTL {
			p.mu.Lock()
			// 二次检查：指针同一性之外还须复核 lastUsed 新鲜度——读锁快照
			// 过期后，touch 可能已在写锁内刷新 lastUsed 并把连接交给在途
			// 请求；只查指针会关掉"刚被取用"的活连接（在途 RPC 报
			// client connection is closing，偶发且难排查）。
			// #99：之外还须复核在途引用——lastUsed 只说明「自借出/归还
			// 以来无人再来取」，不说明「借出去的没用着」；inFlight>0 的
			// 连接上有活跃 RPC，Close 会终止它们，跳过驱逐。
			if cur, exists := p.conns[addr]; exists && cur == entry &&
				time.Since(cur.lastUsed) > p.idleTTL && cur.inFlight == 0 {
				if err := cur.conn.Close(); err != nil {
					epLog.Warn("grpc conn close failed on evict", "addr", addr, "err", err)
				}
				delete(p.conns, addr)
			}
			p.mu.Unlock()
			break // 空闲驱逐后走 create（未驱逐时 create 的二次检查会复用现役连接）
		}

		if entry.conn.GetState() != connectivity.Shutdown {
			if conn, ok := p.touch(addr, entry, ref); ok {
				return conn, nil
			}
			// entry 已被并发驱逐/替换（连接可能已被 Close），重试获取
			continue
		}
		break // 已 Shutdown，走 create 替换
	}

	return p.create(ctx, addr, ref, opts...)
}

// touch 在写锁内校验 entry 仍是 addr 的现役连接，是则刷新 lastUsed（ref=true
// 时同时在途引用加一）并返回连接；否则返回 ok=false，表示 entry 已被并发
// 驱逐或替换，调用方必须重试。
func (p *GRPCPool) touch(addr string, entry *connEntry, ref bool) (_ *grpc.ClientConn, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cur, exists := p.conns[addr]
	if !exists || cur != entry {
		return nil, false
	}
	cur.lastUsed = time.Now()
	if ref {
		cur.inFlight++
	}
	return cur.conn, true
}

func (p *GRPCPool) create(_ context.Context, addr string, ref bool, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, ok := p.conns[addr]
	if ok && entry.conn.GetState() != connectivity.Shutdown {
		entry.lastUsed = time.Now()
		if ref {
			entry.inFlight++
		}
		return entry.conn, nil
	}

	if ok {
		if err := entry.conn.Close(); err != nil {
			epLog.Warn("grpc conn close failed on replace", "addr", addr, "err", err)
		}
		delete(p.conns, addr)
	} else if len(p.conns) >= p.maxSize {
		// 满池先全局驱逐空闲超龄条目再拒绝（#72/#76）：空闲驱逐原本只在
		// Get(同 addr) 路径生效——滚动发布换 IP 后旧地址连接无人再 Get，
		// 池被陈旧连接占满时新地址一律 max size reached，扩容实例直接被拒。
		p.evictIdleLocked()
		if len(p.conns) >= p.maxSize {
			return nil, fmt.Errorf("grpc pool: max size %d reached, cannot create new connection to %s", p.maxSize, addr)
		}
	}

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		// 典型触发是未显式传传输凭据（#77）：grpc 的 errNoTransportSecurity
		// 已自带修复指引（显式 WithTransportCredentials），这里补地址上下文。
		return nil, fmt.Errorf("grpc pool: create conn to %s: %w", addr, err)
	}

	entry = &connEntry{conn: conn, lastUsed: time.Now()}
	if ref {
		entry.inFlight = 1
	}
	p.conns[addr] = entry
	return conn, nil
}

// evictIdleLocked 关闭并删除全部空闲超过 idleTTL 且无在途引用的条目（不区分
// 地址，调用方须持写锁）。与 Get 路径的同地址驱逐语义一致：只看 lastUsed
// 新鲜度 + inFlight==0（#99），连接状态由 Get/create 自行复核。
func (p *GRPCPool) evictIdleLocked() {
	now := time.Now()
	for addr, entry := range p.conns {
		if now.Sub(entry.lastUsed) > p.idleTTL && entry.inFlight == 0 {
			if err := entry.conn.Close(); err != nil {
				epLog.Warn("grpc conn close failed on evict", "addr", addr, "err", err)
			}
			delete(p.conns, addr)
		}
	}
}

// Close 关闭连接池中的所有 gRPC 连接。
// Close 是终结操作：有在途引用的连接同样关闭（其上活跃 RPC 会终止），
// 与「显式关停」语义一致。
func (p *GRPCPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr, entry := range p.conns {
		if err := entry.conn.Close(); err != nil {
			epLog.Warn("grpc conn close failed on pool close", "addr", addr, "err", err)
		}
	}
	p.conns = make(map[string]*connEntry)
}
