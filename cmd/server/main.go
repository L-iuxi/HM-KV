package main

import (
	"TicketX/internal/config"
	"TicketX/internal/kv"
	"TicketX/proto"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
)

// 启动服务器
func main() {
	configPath := flag.String("config", "hmetcd.yaml", "配置文件路径")
	flag.Parse()

	// 加载配置
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	peers := cfg.Peers()
	if cfg.Node.ID < 0 || cfg.Node.ID >= len(peers) {
		log.Fatalf("节点 ID %d 不在 peers 范围 [0, %d)", cfg.Node.ID, len(peers))
	}

	// 创建 KV 服务（Raft + MVCC + Watch 全部初始化）
	kvServer := kv.MakeKVServer(cfg)

	// 启动 gRPC
	lis, err := net.Listen("tcp", cfg.Server.Address)
	if err != nil {
		log.Fatalf("监听 %s 失败: %v", cfg.Server.Address, err)
	}

	srv := grpc.NewServer()
	proto.RegisterKvServer(srv, kvServer)
	proto.RegisterRaftServer(srv, kvServer.GetRaft())

	// 优雅关闭
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		fmt.Printf("\n收到信号 %v，正在关闭...\n", sig)
		srv.GracefulStop()
	}()

	fmt.Printf("节点 %d 启动，监听 %s，peers: %v\n", cfg.Node.ID, cfg.Server.Address, peers)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("gRPC 服务异常退出: %v", err)
	}
}
