package endpoint

import (
	"errors"

	"github.com/yi-nology/ekit-httpx/httpx"
)

// ErrCircuitOpen 表示熔断器处于打开状态、请求被拒绝。
//
// 与 httpx.ErrCircuitOpen 是同一 sentinel：endpoint 工厂与 httpx.Client 的
// 熔断行为共用一个可判定错误，消费方 errors.Is 任一别名均可识别，
// 不必同时 import 两个包。
var ErrCircuitOpen = httpx.ErrCircuitOpen

// statusError 携带 HTTP 状态码的响应错误，供熔断/重试按状态码分类。
type statusError struct {
	code int
	err  error
}

func (e *statusError) Error() string { return e.err.Error() }
func (e *statusError) Unwrap() error { return e.err }

// httpStatus 从错误链中提取 HTTP 状态码；ok=false 表示非 HTTP 状态错误
// （网络层错误，无状态码）。
func httpStatus(err error) (code int, ok bool) {
	var se *statusError
	if errors.As(err, &se) {
		return se.code, true
	}
	return 0, false
}

// breakerSuccessful 是熔断器的默认 IsSuccessful 判定。
//
// 4xx 不计熔断失败：4xx 是调用方的请求错误（参数非法、未授权等），
// 与对端服务健康无关；若计入，单个调用方的 bug 请求足以把依赖熔断，
// 惩罚所有调用方。网络错误与 5xx 仍计失败。
// （对齐 httpx.WithCircuitBreaker 的 >=500 失败判定。）
func breakerSuccessful(err error) bool {
	if err == nil {
		return true
	}
	if code, ok := httpStatus(err); ok && code >= 400 && code < 500 {
		return true
	}
	return false
}

// retryable 判定错误是否值得换实例重试。
//
// 5xx 与网络错误可重试（对端故障或瞬态网络问题，换节点有意义）；
// 4xx 是确定性请求错误，重试必然同样失败。
func retryable(err error) bool {
	if code, ok := httpStatus(err); ok {
		return code >= 500
	}
	return true
}
