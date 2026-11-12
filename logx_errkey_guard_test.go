package endpoint

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestNoHandwrittenErrKeyLiteral 钉住本仓 AGENTS.md 日志红线的防回潮护栏
// （#143）：错误字段名统一用 logx.ErrKey 常量（值 "err"），禁止在日志调用
// 里手写 "err"/"error" 字面量——两矩阵落同一 Loki/ES，手写字面量会在常量
// 改名时（或打错字时）静默产生字段分裂。
//
// 实现：扫描本仓全部非测试 Go 文件，禁止出现 `"err",`（结构化日志调用里
// "err" 作为字段名后跟值参数的唯一形态）。行首注释行跳过；测试文件跳过
// （测试断言日志字段名时需要字面量）。修复前本仓 5 处（http.go ×1、
// pool.go ×4），修复后 0 处。
func TestNoHandwrittenErrKeyLiteral(t *testing.T) {
	var violations []string
	err := filepath.WalkDir(".", func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			switch filepath.Base(path) {
			case ".git", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		for i, line := range strings.Split(string(data), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue // 行首注释：文档里提及 "err" 不构成日志字段
			}
			if strings.Contains(line, `"err",`) {
				violations = append(violations, filepath.Join(path, name)+":"+strconv.Itoa(i+1))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("handwritten \"err\" log field found (use logx.ErrKey constant, AGENTS.md hard rule / #143):\n%s",
			strings.Join(violations, "\n"))
	}
}
