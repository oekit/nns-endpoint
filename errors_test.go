package endpoint

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// statusErrOf 构造携带指定状态码的 statusError（测试表驱动用）。
func statusErrOf(code int) error {
	return &statusError{code: code, err: fmt.Errorf("http error: %d", code)}
}

// TestCallerFault_Classification 钉住错误归属分类面（#123/#124）：
// callerFault 是熔断（breakerSuccessful）、换实例重试（retryable）与
// 均衡器 done 回写（nodeFaultErr）三个消费点的同一口径——调用方错误
// （HTTP 4xx / gRPC 调用方错误码 / 本地确定性错误）不构成对节点的指控。
func TestCallerFault_Classification(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		caller bool // true = 归属调用方（不惩罚节点/不计熔断失败/不重试）
	}{
		{"nil", nil, false},
		{"http 400", statusErrOf(400), true},
		{"http 422", statusErrOf(422), true},
		{"http 499", statusErrOf(499), true},
		{"http 500", statusErrOf(500), false},
		{"http 503", statusErrOf(503), false},
		{"network error", errors.New("connection reset by peer"), false},
		{"local marshal", localErrorf("marshal body: %w", errors.New("unsupported type")), true},
		{"local unmarshal", localErrorf("unmarshal response: %w", errors.New("unexpected end of JSON input")), true},
		{"local select wrapped", fmt.Errorf("attempt: %w", localErrorf("select node: %w", errors.New("no available"))), true},
		{"grpc InvalidArgument", status.Error(codes.InvalidArgument, "bad arg"), true},
		{"grpc NotFound", status.Error(codes.NotFound, "no route"), true},
		{"grpc PermissionDenied", status.Error(codes.PermissionDenied, "denied"), true},
		{"grpc Unauthenticated", status.Error(codes.Unauthenticated, "no auth"), true},
		{"grpc Canceled", status.Error(codes.Canceled, "caller canceled"), true},
		{"grpc Internal", status.Error(codes.Internal, "boom"), false},
		{"grpc Unavailable", status.Error(codes.Unavailable, "down"), false},
		{"grpc DeadlineExceeded", status.Error(codes.DeadlineExceeded, "timeout"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := callerFault(tc.err); got != tc.caller {
				t.Errorf("callerFault = %v, want %v", got, tc.caller)
			}
			// 熔断口径：无错误或调用方错误都算成功
			if got := breakerSuccessful(tc.err); got != (tc.err == nil || tc.caller) {
				t.Errorf("breakerSuccessful = %v, want %v", got, tc.err == nil || tc.caller)
			}
			// done 回写口径：调用方错误归一为 nil，节点/网络错误原样透传
			got := nodeFaultErr(tc.err)
			wantNil := tc.err == nil || tc.caller
			if (got == nil) != wantNil {
				t.Errorf("nodeFaultErr nil = %v, want %v (err=%v)", got == nil, wantNil, got)
			}
		})
	}
}

// TestRetryable_Classification 钉住重试口径（#123）：本地确定性错误
// 不可重试，5xx/网络错误可重试，4xx 不可重试。
func TestRetryable_Classification(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		{"http 500", statusErrOf(500), true},
		{"network error", errors.New("dial tcp: i/o timeout"), true},
		{"http 400", statusErrOf(400), false},
		{"local marshal", localErrorf("marshal body: %w", errors.New("unsupported type")), false},
		{"local unmarshal", localErrorf("unmarshal response: %w", errors.New("invalid character")), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := retryable(tc.err); got != tc.retryable {
				t.Errorf("retryable = %v, want %v", got, tc.retryable)
			}
		})
	}
}

// TestLocalError_Unwrap 钉住 localError 的包装契约：%w 底层错误可被
// errors.Is / errors.As 穿透判定（消费方仍能识别 json.SyntaxError 等
// 具体错误形态）。
func TestLocalError_Unwrap(t *testing.T) {
	syntaxErr := errors.New("unexpected end of JSON input")
	err := fmt.Errorf("do: %w", localErrorf("unmarshal response: %w", syntaxErr))

	if !errors.Is(err, syntaxErr) {
		t.Fatal("errors.Is must unwrap through localError to the wrapped cause")
	}
	var le *localError
	if !errors.As(err, &le) {
		t.Fatal("errors.As must find localError through wrapping")
	}
	if want := "unmarshal response: unexpected end of JSON input"; le.Error() != want {
		t.Errorf("localError message = %q, want %q", le.Error(), want)
	}
}
