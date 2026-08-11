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

	// ---- TLS 配置 ----
	// NewServerTLS 根据配置文件中的 tls 段决定是否启用 TLS：
	//   - 如果 certFile 为空 → 返回 nil → 沿用 insecure 模式（grpc.NewServer() 无参）
	//   - 如果 certFile 不为空 → 返回 grpc.Creds(...) → gRPC 走加密通道
	//   - 如果 mtls=true → 服务器要求客户端也出示证书（双向认证）
	//
	// TLS 工作原理（写在注释里方便理解）：
	//   客户端连接服务器时 → TLS 握手 →
	//   服务器出示自己的证书（cfg.TLS.Cert）→
	//   客户端用 CA 证书（cfg.TLS.CA）验证服务器证书签名 →
	//   （mTLS 模式下）服务器要求客户端出示证书 →
	//   客户端出示证书 → 服务器用 CA 证书验证客户端证书签名 →
	//   双向验证通过 → 建立加密通道
	tlsOpt, err := config.NewServerTLS(cfg.TLS.CA, cfg.TLS.Cert, cfg.TLS.Key, cfg.TLS.MTLS)
	if err != nil {
		log.Fatalf("加载 TLS 配置失败: %v", err)
	}

	// grpc.NewServer 接收不定长参数 ...grpc.ServerOption
	// 有 TLS 时传 tlsOpt，没有时 tlsOpt 为 nil → 不传任何参数 → 默认 insecure
	var srv *grpc.Server
	if tlsOpt != nil {
		srv = grpc.NewServer(tlsOpt)
		fmt.Printf("TLS 已启用")
		if cfg.TLS.MTLS {
			fmt.Printf("（双向认证 mTLS）")
		}
		fmt.Println()
	} else {
		srv = grpc.NewServer()
		fmt.Println("TLS 未启用（insecure 模式）")
	}

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
