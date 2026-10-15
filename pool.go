package endpoint

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// defaultMaxPoolSize 是连接池默认最大连接数。
const defaultMaxPoolSize = 128

// defaultIdleTTL 是连接默认空闲超时时间。
const defaultIdleTTL = 10 * time.Minute

// connEntry 包装 gRPC 连接和最后使用时间。
type connEntry struct {
	conn     *grpc.ClientConn
	lastUsed time.Time
}

// GRPCPool gRPC 连接池，复用 gRPC 客户端连接。
// 连接数超过 maxSize 时拒绝创建新连接。
// 空闲超过 idleTTL 的连接在 Get 时自动驱逐。
type GRPCPool struct {
	mu      sync.RWMutex
	conns   map[string]*connEntry
	maxSize int
	idleTTL time.Duration
}

// NewGRPCPool 创建一个新的 gRPC 连接池实例。
func NewGRPCPool() *GRPCPool {
	return &GRPCPool{
		conns:   make(map[string]*connEntry),
		maxSize: defaultMaxPoolSize,
		idleTTL: defaultIdleTTL,
	}
}

// NewGRPCPoolWithSize 创建指定最大连接数的 gRPC 连接池。
func NewGRPCPoolWithSize(maxSize int) *GRPCPool {
	if maxSize <= 0 {
		maxSize = defaultMaxPoolSize
	}
	return &GRPCPool{
		conns:   make(map[string]*connEntry),
		maxSize: maxSize,
		idleTTL: defaultIdleTTL,
	}
}

// Get 从连接池获取指定地址的 gRPC 连接，不存在、已关闭或空闲超时时创建新连接。
//
// 并发安全说明：读锁读取 entry 与写锁校验之间存在窗口，期间该 entry 可能被
// 并发驱逐（Close 后删除）或替换。因此返回前必须在写锁内校验 p.conns[addr]
// 仍等于该 entry，否则重试——绝不把一个可能已被 Close 的连接返回给调用方。
func (p *GRPCPool) Get(ctx context.Context, addr string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
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
			if cur, exists := p.conns[addr]; exists && cur == entry && time.Since(cur.lastUsed) > p.idleTTL {
				if err := cur.conn.Close(); err != nil {
					epLog.Warn("grpc conn close failed on evict", "addr", addr, "err", err)
				}
				delete(p.conns, addr)
			}
			p.mu.Unlock()
			break // 空闲驱逐后走 create
		}

		if entry.conn.GetState() != connectivity.Shutdown {
			if conn, ok := p.touch(addr, entry); ok {
				return conn, nil
			}
			// entry 已被并发驱逐/替换（连接可能已被 Close），重试获取
			continue
		}
		break // 已 Shutdown，走 create 替换
	}

	return p.create(ctx, addr, opts...)
}

// touch 在写锁内校验 entry 仍是 addr 的现役连接，是则刷新 lastUsed 并返回连接；
// 否则返回 ok=false，表示 entry 已被并发驱逐或替换，调用方必须重试。
func (p *GRPCPool) touch(addr string, entry *connEntry) (_ *grpc.ClientConn, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cur, exists := p.conns[addr]
	if !exists || cur != entry {
		return nil, false
	}
	cur.lastUsed = time.Now()
	return cur.conn, true
}

func (p *GRPCPool) create(_ context.Context, addr string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, ok := p.conns[addr]
	if ok && entry.conn.GetState() != connectivity.Shutdown {
		entry.lastUsed = time.Now()
		return entry.conn, nil
	}

	if ok {
		if err := entry.conn.Close(); err != nil {
			epLog.Warn("grpc conn close failed on replace", "addr", addr, "err", err)
		}
		delete(p.conns, addr)
	} else if len(p.conns) >= p.maxSize {
		return nil, fmt.Errorf("grpc pool: max size %d reached, cannot create new connection to %s", p.maxSize, addr)
	}

	defaultOpts := make([]grpc.DialOption, 0, 1+len(opts))
	defaultOpts = append(defaultOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defaultOpts = append(defaultOpts, opts...)

	conn, err := grpc.NewClient(addr, defaultOpts...)
	if err != nil {
		return nil, err
	}

	p.conns[addr] = &connEntry{conn: conn, lastUsed: time.Now()}
	return conn, nil
}

// Close 关闭连接池中的所有 gRPC 连接。
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
