.PHONY: build build-server build-ctl start-server start-ctl clean

# 默认目标：构建服务器和客户端
build: build-server build-ctl

# 构建服务器
build-server:
	go build -o bin/hmetcd cmd/server/main.go

# 构建交互式客户端
build-ctl:
	go build -o bin/hmctl cmd/ctl/main.go

# 一键启动 3 节点集群（后台运行，日志写入 bin/ 目录）
cluster: build
	@mkdir -p bin
	@echo "启动 3 节点集群..."
	@nohup bin/hmetcd -config configs/node0.yaml > bin/node0.log 2>&1 &
	@nohup bin/hmetcd -config configs/node1.yaml > bin/node1.log 2>&1 &
	@nohup bin/hmetcd -config configs/node2.yaml > bin/node2.log 2>&1 &
	@sleep 2
	@echo "集群已启动。日志: bin/node*.log"
	@echo "客户端: bin/hmctl -config configs/node0.yaml"

# 停止集群
stop-cluster:
	@pkill -f "bin/hmetcd" || true
	@echo "集群已停止"

# 启动交互式客户端
start-ctl:
	go run cmd/ctl/main.go -config configs/node0.yaml

# 清理构建产物
clean:
	rm -rf bin/
