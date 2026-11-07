package endpoint

import (
	"testing"
	"time"
)

// TestResolveCallTimeout 覆盖 #75 的超时解析矩阵：
//   - 显式 req.Timeout 优先（正数原样返回）
//   - 其次服务治理 ReadTimeout（毫秒 → Duration）
//   - 两级均未配置（0/负值）回退 defaultCallTimeout——修复前此处得到 0，
//     context.WithTimeout(ctx, 0) 让每个请求立即 DeadlineExceeded
func TestResolveCallTimeout(t *testing.T) {
	tests := []struct {
		name          string
		reqTimeout    time.Duration
		readTimeoutMS int
		want          time.Duration
	}{
		{"explicit request timeout wins", 500 * time.Millisecond, 3000, 500 * time.Millisecond},
		{"service read timeout used", 0, 3000, 3 * time.Second},
		{"both unset falls back to default", 0, 0, defaultCallTimeout},
		{"negative request timeout falls back", -1, 0, defaultCallTimeout},
		{"negative read timeout falls back", 0, -5, defaultCallTimeout},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveCallTimeout("svc-a", tt.reqTimeout, tt.readTimeoutMS); got != tt.want {
				t.Errorf("resolveCallTimeout(%v, %d) = %v, want %v", tt.reqTimeout, tt.readTimeoutMS, got, tt.want)
			}
		})
	}
}
