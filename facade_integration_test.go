package endpoint

import (
	"testing"

	nnsfacade "github.com/oekit/nns"
	"github.com/oekit/registry"
	nacospkg "github.com/oekit/registry/nacos"
)

// TestFacadeServiceClientMakesHTTPEndpoint 验证 NNS 门面暴露的 ServiceClient
// 可直接构造端点工厂（原 nns 门面验收测试，Round-4 分家后随 endpoint 落位本仓）。
func TestFacadeServiceClientMakesHTTPEndpoint(t *testing.T) {
	c := nnsfacade.New(nacospkg.Bootstrap{ServiceName: "http-svc"}, nnsfacade.WithMockMode(true),
		nnsfacade.WithStaticInstance("http-svc", []registry.Instance{
			{IP: "10.0.0.1", Port: 8080},
		}))

	ep, err := NewHTTPEndpointFactory(c.ServiceClient()).MakeEndpoint("http-svc")
	if err != nil {
		t.Fatalf("MakeEndpoint error: %v", err)
	}
	if ep == nil {
		t.Fatal("expected non-nil endpoint")
	}
}
