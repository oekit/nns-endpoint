package endpoint

import (
	"context"
)

// Endpoint 端点函数类型，接收上下文和请求，返回响应和错误。
type Endpoint func(ctx context.Context, request any) (any, error)
