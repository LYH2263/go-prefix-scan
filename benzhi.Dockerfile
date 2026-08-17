# 评测专用：保留完整 Go 工具链（勿改成多阶段/只留二进制）
# 官方多架构基础镜像；须分别验证 linux/amd64 与 linux/arm64
FROM golang:1.22

WORKDIR /app

# 本批题无外部依赖、无 go.sum
COPY go.mod ./
RUN go mod download

COPY . .

# 预编译缓存；不要在构建期跑会必红的 go test
RUN go build ./...

CMD ["bash"]
