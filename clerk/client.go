// Package clerk 提供 HMETCD 分布式 KV 存储的客户端 SDK。
//
// # 使用方式
//
//	// 普通连接（无 TLS）
//	client, err := clerk.New([]string{"localhost:50051"})
//
//	// TLS 加密连接
//	client, err := clerk.New([]string{"localhost:50051"}, clerk.WithTLS("certs/ca.pem", "", ""))
//
//	// mTLS 双向认证
//	client, err := clerk.New([]string{"localhost:50051"}, clerk.WithTLS("certs/ca.pem", "certs/client.pem", "certs/client-key.pem"))
//
// # 连接管理
//
// Clerk 内部维护到所有节点的 gRPC 连接，通过 leaderIdx 跟踪当前 Leader。
// 遇到 WRONG_LEADER 错误时自动切换到 knownLeader 或顺序探测下一个节点。
package clerk

import (
	"TicketX/internal/config"
	"fmt"
	"math/rand"

	"TicketX/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Option 是 Client 的可选配置函数（函数式选项模式）。
// 用于在不改变 New 签名的前提下扩展配置项。
type Option func(*Client) error

// WithTLS 配置客户端使用 TLS 连接。
//
// 参数：
//   caFile   — CA 证书路径，用于验证服务器证书是否由可信 CA 签发
//   certFile — 客户端自己的证书路径（mTLS 时需要），普通 TLS 传空字符串
//   keyFile  — 客户端私钥路径（mTLS 时需要）
//
// 普通 TLS（只验证服务器）：
//
//	clerk.New(addrs, clerk.WithTLS("ca.pem", "", ""))
//
// mTLS（双向验证）：
//
//	clerk.New(addrs, clerk.WithTLS("ca.pem", "client.pem", "client-key.pem"))
func WithTLS(caFile, certFile, keyFile string) Option {
	return func(c *Client) error {
		// 用 config.NewClientTLS 构建 gRPC DialOption
		// serverName 留空 → 不校验服务器证书 SAN（跨机器部署 IP 变化时更灵活）
		tlsOpt, err := config.NewClientTLS(caFile, certFile, keyFile, "")
		if err != nil {
			return fmt.Errorf("clerk: TLS 配置失败: %w", err)
		}

		// 关闭之前用 insecure 建立的连接，用 TLS 重新建连
		for _, conn := range c.conns {
			if conn != nil {
				conn.Close()
			}
		}
		c.conns = make([]*grpc.ClientConn, len(c.addrs))
		c.kvcs = make([]proto.KvClient, len(c.addrs))

		for i, addr := range c.addrs {
			// 有 TLS 时用 tlsOpt，没有时回退 insecure（应该不会走这里，防御性代码）
			var conn *grpc.ClientConn
			if tlsOpt != nil {
				conn, err = grpc.NewClient(addr, tlsOpt)
			} else {
				conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			}
			if err != nil {
				c.Close()
				return fmt.Errorf("clerk: 连接 %s 失败: %w", addr, err)
			}
			c.conns[i] = conn
			c.kvcs[i] = proto.NewKvClient(conn)
		}
		return nil
	}
}

// New 创建一个 Clerk 客户端。
//
// addrs 是所有节点的 gRPC 地址列表，如 []string{"192.168.1.10:50051", "192.168.1.11:50051"}。
// 可以传入 Option 来配置 TLS 等选项。
func New(addrs []string, opts ...Option) (*Client, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("clerk: 至少需要一个节点地址")
	}

	c := &Client{
		addrs:    addrs,
		conns:    make([]*grpc.ClientConn, len(addrs)),
		kvcs:     make([]proto.KvClient, len(addrs)),
		clientID: rand.Int63(),
	}

	// 先用 insecure 建立连接（TLS option 会覆盖）
	for i, addr := range addrs {
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("clerk: 连接 %s 失败: %w", addr, err)
		}
		c.conns[i] = conn
		c.kvcs[i] = proto.NewKvClient(conn)
	}

	// 应用 Option（WithTLS 会用 TLS 连接覆盖 insecure 连接）
	for _, opt := range opts {
		if err := opt(c); err != nil {
			c.Close()
			return nil, err
		}
	}

	c.leaderIdx.Store(0)
	c.knownLeader.Store(-1)
	c.requestID.Store(1)
	return c, nil
}

// Close 关闭所有 gRPC 连接。
func (c *Client) Close() error {
	var errs []error
	for _, conn := range c.conns {
		if conn != nil {
			if err := conn.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("clerk: 关闭连接时出错: %v", errs)
	}
	return nil
}

// nextID 生成下一个请求 ID，线程安全（原子递增）。
func (c *Client) nextID() int64 {
	return c.requestID.Add(1)
}

// tryNextLeader 切换到下一个可能的 Leader 节点。
//
// 策略：
//   1. 优先使用 knownLeader（上一次成功通信的 Leader）
//   2. 如果 knownLeader 不可用或就是当前节点，顺序尝试下一个（轮询）
//
// 并发安全：mu 锁保证只有一个 goroutine 在修改 leaderIdx。
func (c *Client) tryNextLeader(current int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if int(c.leaderIdx.Load()) != current {
		return // 已被其他请求切换，无需操作
	}

	// 优先尝试已知 leader
	known := c.knownLeader.Load()
	if known >= 0 && int(known) != current && int(known) < len(c.addrs) {
		c.leaderIdx.Store(int32(known))
		return
	}

	// 已知 leader 不可用或就是当前节点，顺序探测下一个
	next := (current + 1) % len(c.addrs)
	c.leaderIdx.Store(int32(next))
}
