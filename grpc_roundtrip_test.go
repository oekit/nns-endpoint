package endpoint

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	nnsclient "github.com/oekit/nns/client"
	"github.com/oekit/nns/metadata"
	"github.com/oekit/registry"

	"github.com/oekit/httpx/loadbalance"
	"github.com/sony/gobreaker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// newBufconnGRPCServer 启动一个 bufconn gRPC 服务器：手工注册
// ServiceDesc（免 codegen），每个方法是「StringValue → StringValue」的
// 一元处理器。返回可直接传给 MakeEndpoint 的拨号选项（bufconn dialer +
// 显式明文凭据）——实例地址被 dialer 接管，不会真实拨号。
func newBufconnGRPCServer(t *testing.T, serviceName string, methods map[string]func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error)) []grpc.DialOption {
	t.Helper()

	srv := grpc.NewServer()
	desc := &grpc.ServiceDesc{ServiceName: serviceName}
	for name, fn := range methods {
		handler := fn
		desc.Methods = append(desc.Methods, grpc.MethodDesc{
			MethodName: name,
			Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				in := new(wrapperspb.StringValue)
				if err := dec(in); err != nil {
					return nil, err
				}
				return handler(ctx, in)
			},
		})
	}
	srv.RegisterService(desc, nil)

	lis := bufconn.Listen(64 * 1024)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})

	return []grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
}

// recordingSelector 记录每次调用的 DoneFunc 回写结果（#74 回归测试用），
// 选点固定为候选首个节点。
type recordingSelector struct {
	mu    sync.Mutex
	errs  []error
	addrs []string
}

func (s *recordingSelector) Select(_ context.Context, nodes []loadbalance.Node, _ ...loadbalance.SelectOption) (loadbalance.Node, loadbalance.DoneFunc, error) {
	if len(nodes) == 0 {
		return nil, nil, loadbalance.ErrNoAvailable
	}
	n := nodes[0]
	return n, func(_ context.Context, di loadbalance.DoneInfo) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.errs = append(s.errs, di.Err)
		s.addrs = append(s.addrs, n.Address())
	}, nil
}

func (s *recordingSelector) doneErrs() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.errs...)
}

// newGRPCFactoryWithBufconn 构造指向 bufconn 服务器的 gRPC 端点工厂。
// ReadTimeout 故意配 0：配合不设 req.Timeout，让往返测试同时覆盖 #75
// 的超时兜底路径（修复前 context.WithTimeout(ctx, 0) 立即
// DeadlineExceeded，往返根本走不完）。
func newGRPCFactoryWithBufconn(t *testing.T, dialOpts []grpc.DialOption, factoryOpts ...EndpointFactoryOption) *GRPCEndpointFactory {
	t.Helper()
	c := nnsclient.New(
		nnsclient.WithMockMode(true),
		nnsclient.WithStaticInstances("svc-a", []registry.Instance{
			{IP: "127.0.0.1", Port: 1},
		}),
		nnsclient.WithLocalServiceConfig("svc-a", &metadata.ServiceBasic{
			ConnectionConfig: metadata.ConnectionConfig{
				ReadTimeout: 0,
				Lb:          metadata.LbRoundRobin,
			},
		}),
	)
	return NewGRPCEndpointFactory(c, factoryOpts...)
}

// TestGRPCEndpoint_InvokeRoundTrip 是 gRPC happy path 的首个真实 RPC 往返
// 测试（#71/#73）：修复前解码目标是 var reply any（*any），默认 proto
// codec 要求 proto.Message，类型化响应永远拿不到——本测试在修复前会以
// codec 解码错误失败。同时验证：
//   - 端点返回值就是调用方传入的 Reply 指针
//   - 两级超时缺配（ReadTimeout=0 且不设 req.Timeout）时走 #75 兜底，
//     往返正常完成而非立即 DeadlineExceeded
//   - 成功路径 selector 的 done 得到回写（Err=nil，#74）
func TestGRPCEndpoint_InvokeRoundTrip(t *testing.T) {
	dialOpts := newBufconnGRPCServer(t, "nns.test.EchoService", map[string]func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error){
		"Echo": func(_ context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return wrapperspb.String("echo: " + req.GetValue()), nil
		},
	})

	rec := &recordingSelector{}
	factory := newGRPCFactoryWithBufconn(t, dialOpts, WithSelector(rec))
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a", dialOpts...)
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	reply := &wrapperspb.StringValue{}
	result, err := ep(context.Background(), GRPCRequest{
		GrpcServiceName: "nns.test.EchoService",
		Method:          "Echo",
		Request:         wrapperspb.String("hello"),
		Reply:           reply,
	})
	if err != nil {
		t.Fatalf("endpoint call: %v", err)
	}
	if result != any(reply) {
		t.Errorf("endpoint must return the caller's Reply pointer, got %T", result)
	}
	if got := reply.GetValue(); got != "echo: hello" {
		t.Errorf("expected reply %q, got %q", "echo: hello", got)
	}

	// 成功路径 done 恰好回写一次、Err=nil（#74）
	errs := rec.doneErrs()
	if len(errs) != 1 {
		t.Fatalf("expected exactly 1 done callback, got %d", len(errs))
	}
	if errs[0] != nil {
		t.Errorf("expected done with nil err on success, got %v", errs[0])
	}
}

// TestGRPCEndpoint_InvokeFailureReportsDone 锁定 #74 的行为：RPC 失败
// （conn.Invoke 返回错误）时 selector 的 done 必须带错误回写——p2c 的
// EWMA 故障惩罚依赖该样本，失败节点才会降权。修复已随 #57（becc834）
// 落地 main（此前 Invoke 失败直接 return、从不回调 done），本测试补齐
// gRPC 失败路径的回归覆盖。
func TestGRPCEndpoint_InvokeFailureReportsDone(t *testing.T) {
	dialOpts := newBufconnGRPCServer(t, "nns.test.EchoService", map[string]func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error){
		"Boom": func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return nil, status.Error(codes.Internal, "boom")
		},
	})

	rec := &recordingSelector{}
	factory := newGRPCFactoryWithBufconn(t, dialOpts, WithSelector(rec))
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a", dialOpts...)
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	_, err = ep(context.Background(), GRPCRequest{
		GrpcServiceName: "nns.test.EchoService",
		Method:          "Boom",
		Request:         wrapperspb.String("x"),
		Reply:           &wrapperspb.StringValue{},
	})
	if err == nil {
		t.Fatal("expected Invoke failure to propagate, got nil")
	}
	if !containsAny(err.Error(), "boom") {
		t.Errorf("expected server error to surface, got: %v", err)
	}

	// 失败样本必须恰好回写一次且携带错误（#74）
	errs := rec.doneErrs()
	if len(errs) != 1 {
		t.Fatalf("expected exactly 1 done callback on failure, got %d", len(errs))
	}
	if errs[0] == nil {
		t.Error("expected done callback to receive the Invoke error, got nil")
	}
}

// TestGRPCEndpoint_BreakerRejectionSkipsNodeDone 是 #97 的回归测试：
// 服务级熔断拒绝（ErrOpenState/ErrTooManyRequests）时调用从未到达节点，
// 不得回写本次 Select 选中的节点的 done——此前熔断打开期间每次调用仍
// GetInstances→Select→Execute 即刻拒绝→done(Err)，p2c 对被反复选中的
// 健康节点持续记故障 EWMA（makeDone 对非 nil Err 追加惩罚），真实肇事
// 节点反而可能 EWMA 最低，熔断恢复后流量分布被反向扭曲。
func TestGRPCEndpoint_BreakerRejectionSkipsNodeDone(t *testing.T) {
	dialOpts := newBufconnGRPCServer(t, "nns.test.EchoService", map[string]func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error){
		"Boom": func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return nil, status.Error(codes.Internal, "boom")
		},
	})

	rec := &recordingSelector{}
	factory := newGRPCFactoryWithBufconn(t, dialOpts,
		WithSelector(rec),
		// 单次真实失败即 trip，且测试窗口内保持 open
		WithBreakerSettings(gobreaker.Settings{
			ReadyToTrip: func(counts gobreaker.Counts) bool { return counts.ConsecutiveFailures >= 1 },
			Timeout:     time.Minute,
		}),
	)
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a", dialOpts...)
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}
	call := func() error {
		_, err := ep(context.Background(), GRPCRequest{
			GrpcServiceName: "nns.test.EchoService",
			Method:          "Boom",
			Request:         wrapperspb.String("x"),
			Reply:           &wrapperspb.StringValue{},
		})
		return err
	}

	// call 1：真实到达节点的 Invoke 失败——done 必须带错误回写（#74 语义）
	if err := call(); err == nil {
		t.Fatal("expected first call to fail with Invoke error")
	}
	errs := rec.doneErrs()
	if len(errs) != 1 || errs[0] == nil {
		t.Fatalf("expected exactly 1 real-failure done sample, got %v", errs)
	}

	// call 2：熔断已打开——调用从未到达节点，done 不得再回写
	if err := call(); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen after trip, got: %v", err)
	}
	if errs := rec.doneErrs(); len(errs) != 1 {
		t.Fatalf("breaker rejection must not charge the selected node: got %d done samples, want 1", len(errs))
	}

	// call 3：熔断持续打开，同样不回写
	if err := call(); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen while breaker stays open, got: %v", err)
	}
	if errs := rec.doneErrs(); len(errs) != 1 {
		t.Fatalf("repeated breaker rejections must not charge nodes: got %d done samples, want 1", len(errs))
	}
}

// TestGRPCEndpoint_CallerErrorNotChargedToNode 是 #124 的回归测试：
// done 回写区分错误归属——gRPC 调用方错误码（InvalidArgument 等，HTTP
// 4xx 同构语义）不回写节点惩罚：调用方 bug 打出的错误风暴不得把 p2c
// 流量推离只是碰上坏请求的健康节点；节点/网络错误（Internal）仍原样
// 回写，EWMA 惩罚语义不变（#74）。
func TestGRPCEndpoint_CallerErrorNotChargedToNode(t *testing.T) {
	dialOpts := newBufconnGRPCServer(t, "nns.test.EchoService", map[string]func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error){
		"BadArg": func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return nil, status.Error(codes.InvalidArgument, "bad arg")
		},
		"Boom": func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return nil, status.Error(codes.Internal, "boom")
		},
	})

	rec := &recordingSelector{}
	factory := newGRPCFactoryWithBufconn(t, dialOpts, WithSelector(rec))
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a", dialOpts...)
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}
	call := func(method string) error {
		_, callErr := ep(context.Background(), GRPCRequest{
			GrpcServiceName: "nns.test.EchoService",
			Method:          method,
			Request:         wrapperspb.String("x"),
			Reply:           &wrapperspb.StringValue{},
		})
		return callErr
	}

	// 调用方错误码：调用报错，但节点不得被记故障惩罚
	if err := call("BadArg"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument to propagate, got: %v", err)
	}
	if errs := rec.doneErrs(); len(errs) != 1 || errs[0] != nil {
		t.Fatalf("gRPC caller error must not charge the selected node, done samples = %v", errs)
	}

	// 节点/网络错误：原样回写，EWMA 惩罚语义不变
	if err := call("Boom"); status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal to propagate, got: %v", err)
	}
	errs := rec.doneErrs()
	if len(errs) != 2 {
		t.Fatalf("expected exactly 2 done samples, got %d", len(errs))
	}
	if errs[1] == nil {
		t.Fatal("gRPC node error must keep charging the node (EWMA penalty semantics unchanged)")
	}
	if status.Code(errs[1]) != codes.Internal {
		t.Fatalf("node fault done sample must carry the Invoke error, got: %v", errs[1])
	}
}

// TestGRPCEndpoint_CallerErrorDoesNotTripBreaker 是 #124 的回归测试：
// 连续 gRPC 调用方错误（InvalidArgument 风暴）不计熔断失败——
// callerFault 归属口径统一进 breakerSuccessful 后，熔断样本与均衡器
// 样本对「错误归谁」的判定一致（修复前无 statusError ⇒ 连续失败，
// 第 7 次调用起被 ErrCircuitOpen 掩盖真实错误形态）。
func TestGRPCEndpoint_CallerErrorDoesNotTripBreaker(t *testing.T) {
	dialOpts := newBufconnGRPCServer(t, "nns.test.EchoService", map[string]func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error){
		"BadArg": func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return nil, status.Error(codes.InvalidArgument, "bad arg")
		},
	})

	factory := newGRPCFactoryWithBufconn(t, dialOpts)
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a", dialOpts...)
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	// 远超默认 ReadyToTrip 阈值（连续失败 > 5）的调用方错误调用，
	// 每次都必须保持 InvalidArgument 错误形态（不得被 ErrCircuitOpen 掩盖）
	for range 10 {
		_, callErr := ep(context.Background(), GRPCRequest{
			GrpcServiceName: "nns.test.EchoService",
			Method:          "BadArg",
			Request:         wrapperspb.String("x"),
			Reply:           &wrapperspb.StringValue{},
		})
		if status.Code(callErr) != codes.InvalidArgument {
			t.Fatalf("caller error must not trip the breaker, got: %v", callErr)
		}
	}
	if state := factory.breakers.getOrCreate("svc-a").State(); state != gobreaker.StateClosed {
		t.Fatalf("expected breaker closed after 10 consecutive caller errors, got %v", state)
	}
}

// TestGRPCEndpoint_ReplyAndRequestValidation 覆盖 #71/#73 的前置校验：
// 缺 Reply 或 Request 非 proto.Message 时，端点必须在发起调用前报
// 配置性错误（无需真实服务器——校验先于实例选择）。
func TestGRPCEndpoint_ReplyAndRequestValidation(t *testing.T) {
	c := nnsclient.New(
		nnsclient.WithMockMode(true),
		nnsclient.WithStaticInstances("svc-a", []registry.Instance{{IP: "127.0.0.1", Port: 1}}),
	)
	factory := NewGRPCEndpointFactory(c)
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	_, err = ep(context.Background(), GRPCRequest{
		GrpcServiceName: "nns.test.EchoService",
		Method:          "Echo",
		Request:         wrapperspb.String("x"),
		// Reply 缺失
	})
	if err == nil || !containsAny(err.Error(), "Reply must be a non-nil proto.Message pointer") {
		t.Errorf("expected missing-Reply error, got: %v", err)
	}

	_, err = ep(context.Background(), GRPCRequest{
		GrpcServiceName: "nns.test.EchoService",
		Method:          "Echo",
		Request:         "not-a-proto-message", // 非 proto.Message
		Reply:           &wrapperspb.StringValue{},
	})
	if err == nil || !containsAny(err.Error(), "must implement proto.Message") {
		t.Errorf("expected non-proto request error, got: %v", err)
	}
}
