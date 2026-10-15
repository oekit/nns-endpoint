package endpoint

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/sony/gobreaker"
	"github.com/yi-nology/ekit-metrics/resilience"
	nnsclient "github.com/yi-nology/ekit-nns/nns/client"
	"github.com/yi-nology/ekit-registry/registry"
)

// metricValue 从 Registry 黑盒采集指定 metric + labels 的值（单值 gauge/counter）。
func metricValue(t *testing.T, reg *prom.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			match := true
			for k, v := range labels {
				found := false
				for _, lp := range m.GetLabel() {
					if lp.GetName() == k && lp.GetValue() == v {
						found = true
						break
					}
				}
				if !found {
					match = false
					break
				}
			}
			if match {
				if m.GetGauge() != nil {
					return m.GetGauge().GetValue()
				}
				return m.GetCounter().GetValue()
			}
		}
	}
	t.Fatalf("metric %s with labels %v not found", name, labels)
	return 0
}

// TestHTTPEndpoint_MetricsRetryAndRejected 钉住弹性指标语义：
// 重试计数 = 额外尝试次数；熔断打开后的拒绝计数递增；
// 状态 gauge 与迁移计数同步。
func TestHTTPEndpoint_MetricsRetryAndRejected(t *testing.T) {
	var hits atomic.Int64
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()
	host, port := parseHostPort(bad.URL)

	reg := prom.NewRegistry()
	c := nnsclient.New(
		nnsclient.WithMockMode(true),
		nnsclient.WithStaticInstances("svc-a", []registry.Instance{{IP: host, Port: port}}),
		nnsclient.WithLocalServiceConfig("svc-a", localServiceConfig(2)),
	)
	if err := c.LoadDepService(context.Background(), []string{"svc-a"}); err != nil {
		t.Fatalf("LoadDepService: %v", err)
	}
	factory := NewHTTPEndpointFactory(c,
		WithMetrics(reg, ""),
		WithBreakerSettings(gobreaker.Settings{
			ReadyToTrip: func(counts gobreaker.Counts) bool { return counts.ConsecutiveFailures >= 1 },
			Timeout:     time.Hour, // 保持 open，不回 half-open
		}),
	)
	defer factory.Close()

	ep, err := factory.MakeEndpoint("svc-a")
	if err != nil {
		t.Fatalf("MakeEndpoint: %v", err)
	}

	// 第一次调用：500 → 2 次重试（共 3 次尝试）→ 失败且熔断 trip
	_, err = ep(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/x"})
	if err == nil {
		t.Fatal("expected failure")
	}
	if got := hits.Load(); got != 3 {
		t.Fatalf("expected 3 attempts, got %d", got)
	}
	if got := metricValue(t, reg, "nns_endpoint_retry_attempts_total", map[string]string{"service": "svc-a", "kind": "nns-http"}); got != 2 {
		t.Fatalf("expected 2 retry observations, got %v", got)
	}
	// closed → open 迁移一次，gauge=2
	if got := metricValue(t, reg, "nns_endpoint_breaker_state", map[string]string{"service": "svc-a", "kind": "nns-http"}); got != resilience.StateOpen {
		t.Fatalf("expected open state gauge, got %v", got)
	}
	if got := metricValue(t, reg, "nns_endpoint_breaker_transitions_total",
		map[string]string{"service": "svc-a", "kind": "nns-http", "from": "closed", "to": "open"}); got != 1 {
		t.Fatalf("expected 1 closed→open transition, got %v", got)
	}

	// 第二次调用：熔断拒绝（不发请求）
	_, err = ep(context.Background(), HTTPRequest{Method: http.MethodGet, Path: "/x"})
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
	if got := hits.Load(); got != 3 {
		t.Fatalf("expected no additional hits while open, got %d", got)
	}
	if got := metricValue(t, reg, "nns_endpoint_breaker_rejected_total", map[string]string{"service": "svc-a", "kind": "nns-http"}); got != 1 {
		t.Fatalf("expected 1 rejection observation, got %v", got)
	}
}

// TestGRPCEndpoint_MetricsNotEnabled 默认无指标（零值路径不 panic）。
func TestGRPCEndpoint_MetricsNotEnabled(t *testing.T) {
	c := nnsclient.New(nnsclient.WithMockMode(true))
	f := NewGRPCEndpointFactory(c)
	defer f.Close()
	if f.metrics != nil {
		t.Fatal("expected nil metrics when WithMetrics not set")
	}
}
