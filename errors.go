package endpoint

import (
	"errors"
	"fmt"

	"github.com/oekit/httpx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// localError 本地确定性错误（请求体序列化、响应解码、选点失败等，#123）。
// 这类错误在本地即可判定失败、重试同一输入恒失败，且不是被选节点的
// 故障——不换实例重试、不计熔断样本、不做节点 EWMA 惩罚。
type localError struct {
	err error
}

func (e *localError) Error() string { return e.err.Error() }
func (e *localError) Unwrap() error { return e.err }

// localErrorf 构造本地确定性错误，包装风格与 fmt.Errorf 一致（%w 保留
// 底层错误，errors.Is 可穿透判定）。
func localErrorf(format string, args ...any) error {
	return &localError{err: fmt.Errorf(format, args...)}
}

// localFault 判定错误链中是否含本地确定性错误（errors.As 穿透包装层）。
func localFault(err error) bool {
	var le *localError
	return errors.As(err, &le)
}

// grpcCallerCode 判定 gRPC 错误码是否为调用方错误码：按 grpc-go 官方
// HTTP 状态映射落在 4xx 区间的码（#124）。这些码与 HTTP 4xx 同构——
// 错误归属调用方的请求本身（参数非法、目标不存在、未授权、调用方
// 主动取消等），与被选节点的健康无关；服务端/网络侧错误码
// （Internal/Unavailable/DeadlineExceeded 等 5xx 语义）不在此列。
func grpcCallerCode(c codes.Code) bool {
	switch c {
	case codes.Canceled, codes.InvalidArgument, codes.NotFound, codes.AlreadyExists,
		codes.PermissionDenied, codes.ResourceExhausted, codes.FailedPrecondition,
		codes.Aborted, codes.OutOfRange, codes.Unauthenticated:
		return true
	default:
		return false
	}
}

// callerFault 判定错误是否归属调用方（错误归属分类面，#123/#124）。
//
// 三类调用方错误：
//   - 本地确定性错误（localError）：序列化/解码/选点在本地即失败，
//     请求可能根本未到达节点；
//   - HTTP 4xx（statusError）：请求错误与对端健康无关；
//   - gRPC 调用方错误码：HTTP 4xx 的同构语义（grpcCallerCode）。
//
// 该判定是熔断（breakerSuccessful）、换实例重试（retryable）与均衡器
// done 回写（nodeFaultErr）三个消费点的同一口径——错误归属决定弹性
// 信号归谁，调用方的坏请求不构成对节点的指控。
func callerFault(err error) bool {
	if err == nil {
		return false
	}
	if localFault(err) {
		return true
	}
	if code, ok := httpStatus(err); ok {
		return code >= 400 && code < 500
	}
	// 非 gRPC status 错误经 status.Code 归一为 Unknown（5xx 语义）：
	// 未知错误按节点/网络故障处理，与既有行为一致。
	return grpcCallerCode(status.Code(err))
}

// nodeFaultErr 是均衡器 done 回写的错误归属面（#124）：返回值非 nil
// 才构成 p2c 的节点 EWMA 故障惩罚。调用方错误（callerFault）归一为
// nil——「碰巧被选中的节点」不因调用方的坏请求降权，流量分布不被
// 调用方错误扭曲；节点/网络错误原样透传，故障/慢节点照常降权（#74）。
func nodeFaultErr(err error) error {
	if callerFault(err) {
		return nil
	}
	return err
}

// breakerSuccessful 是熔断器的默认 IsSuccessful 判定。
//
// 判定维度是错误归属（#123/#124）：调用方错误（HTTP 4xx / gRPC 调用方
// 错误码 / 本地确定性编解码错误，见 callerFault）不计熔断失败——与对端
// 服务健康无关，若计入，单个调用方的 bug 请求足以把依赖熔断，惩罚
// 所有调用方。网络错误与 5xx 仍计失败。
// （对齐 httpx.WithCircuitBreaker 的 >=500 失败判定。）
func breakerSuccessful(err error) bool {
	return err == nil || callerFault(err)
}

// retryable 判定错误是否值得换实例重试。
//
// 5xx 与网络错误可重试（对端故障或瞬态网络问题，换节点有意义）；
// 4xx 是确定性请求错误，重试必然同样失败；本地确定性错误
// （序列化/解码/选点，#123）重试同一输入恒失败，同样不可重试——
// 此前它们无 statusError 被判为可重试网络错误，每个坏请求白打
// (1+Retry) 次真实 HTTP。
func retryable(err error) bool {
	if localFault(err) {
		return false
	}
	if code, ok := httpStatus(err); ok {
		return code >= 500
	}
	return true
}
