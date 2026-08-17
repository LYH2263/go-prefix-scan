# go-prefix-scan 评测说明

LSM-KV：SSTable Writer 在 flush 路径跳过 tombstone（详见题面 / `_BUG_PATCH.txt`）。

## 环境

- 镜像：benzhi.Dockerfile 基于 golang:1.22（官方多架构）
- go.mod 语言版本：go 1.22
- 容器内使用镜像自带工具链即可

## 标准命令

```bash
go build ./...
go test ./... -count=1
go vet ./...
```

## 构建评测镜像（须双架构）

验证请用 `bash -c`（勿用 `bash -lc`）。

```bash
chmod +x build_benzhi_docker.sh
./build_benzhi_docker.sh go-prefix-scan linux/amd64
docker run --platform linux/amd64 --rm go-prefix-scan:latest bash -c 'go build ./... && go test ./... -count=1'

./build_benzhi_docker.sh go-prefix-scan linux/arm64
docker run --platform linux/arm64 --rm go-prefix-scan:latest bash -c 'go build ./... && go test ./... -count=1'
```

构建阶段已处理依赖下载；容器内不应再出现 downloading ...。
