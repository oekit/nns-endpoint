.PHONY: test test-race test-coverage lint fmt vet ci tidy

## test: 运行所有单元测试
test:
	go test ./...

## test-race: 运行竞态检测测试
test-race:
	go test -race ./...

## test-coverage: 生成测试覆盖率报告
test-coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

## lint: 运行 golangci-lint（需要先安装）
lint:
	golangci-lint run ./...

## fmt: 格式化所有 Go 文件
fmt:
	gofmt -s -w .
	goimports -w .

## vet: 运行 go vet
vet:
	go vet ./...

## ci: 模拟 CI 流程（fmt 检查 + vet + test-race）
ci:
	@test -z "$$(gofmt -l . | grep -v vendor | tee /dev/stderr)" || (echo "请运行 make fmt" && exit 1)
	go vet ./...
	go test -race -count=1 ./...

## tidy: 整理 go.mod
tidy:
	go mod tidy

## help: 显示帮助
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## //'
