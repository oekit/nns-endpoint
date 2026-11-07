package endpoint

import (
	"context"
	"net"
	"sync"
	"testing"

	nnsclient "github.com/oekit/nns/client"
	"github.com/oekit/nns/metadata"
	"github.com/oekit/registry"

	"github.com/oekit/httpx/loadbalance"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
