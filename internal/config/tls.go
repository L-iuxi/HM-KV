package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// ================================================================
// TLS 证书加载工具
// ================================================================
//
// 本文件提供从 PEM 文件加载 TLS 证书的方法，
// 用于 gRPC server 端和 client 端建立加密连接。
//
// # TLS 为什么需要这些证书
//
// TLS 的核心目标有两个：
//   1. 加密通信——防止中间人窃听
//   2. 身份认证——确认通信双方的身份
//
// 加密通信通过"证书 + 私钥"实现：
//   - 证书（公钥）→ 加密数据，只有持有私钥的人能解密
//   - 私钥 → 解密数据 + 签名（证明自己对私钥的所有权）
//
// 身份认证通过"CA 信任链"实现：
//   - CA 用自己的私钥给节点证书签名（"我担保这个证书的持有者是 XXX"）
//   - 任何人都可以用 CA 公钥验证签名（"这个证书确实是 CA 签的，没被篡改"）
//   - 如果你信任 CA，你就信任所有它签发的证书
//
// # 双向认证（mTLS）和单向认证的区别
//
//   普通 TLS：客户端验证服务器（"我没连错网站"）
//   mTLS：双方互相验证（"你是谁"→"我是 xxx，有 CA 的证明"）
//
// mTLS 常用于服务间通信的零信任安全模型：
// 不依赖防火墙或网络隔离，每个服务都要求对端出示证书。

// NewServerTLS 创建 gRPC Server 的 TLS 配置选项。
//
// 参数：
//   caFile   — CA 证书路径。mTLS 时用于验证客户端的证书签名
//   certFile — 服务器自己的证书路径（在 TLS 握手时出示给客户端）
//   keyFile  — 服务器的私钥路径（对应 certFile，绝密，不传输）
//   mtls     — true 表示要求客户端也出示证书（双向认证）
//
// 返回值是 grpc.ServerOption，直接传给 grpc.NewServer()。
// 如果 certFile 为空字符串，表示不启用 TLS，返回 nil。
func NewServerTLS(caFile, certFile, keyFile string, mtls bool) (grpc.ServerOption, error) {
	if certFile == "" {
		// 没配证书 → 不启用 TLS，沿用 insecure 模式
		return nil, nil
	}

	// 第一步：加载服务器自己的证书和私钥
	//
	// tls.LoadX509KeyPair 做了两件事：
	//   1. 读取 PEM 文件，解析出 X.509 证书（ASN.1 DER 结构体）
	//   2. 读取 PEM 文件，解析出私钥（ECDSA 或 RSA）
	//   3. 校验证书和私钥是否匹配（用公钥加密一段数据，私钥能解开才算）
	//
	// 返回的 tls.Certificate 包含：
	//   Certificate[0] — 叶子证书（服务器的公钥证书，DER 格式二进制）
	//   Certificate[1:] — 中间 CA 证书链（如果有的话，这里不需要）
	//   PrivateKey — 私钥（*ecdsa.PrivateKey，不发送给客户端）
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("加载服务器证书失败 %s/%s: %w", certFile, keyFile, err)
	}

	tlsCfg := &tls.Config{
		// Certificates：TLS 握手时，服务器从列表中选一个证书出示给客户端
		// 服务器根据客户端 ClientHello 中的 SNI 来选，只有一个时直接用它
		Certificates: []tls.Certificate{cert},

		// MinVersion：强制最低 TLS 1.2
		// TLS 1.0/1.1 有已知漏洞（BEAST/POODLE），已被 RFC 8996 废弃
		MinVersion: tls.VersionTLS12,
	}

	if mtls {
		// mTLS 模式：服务器在 TLS 握手时发送 CertificateRequest 消息，
		// 要求客户端出示证书。客户端必须回应，否则握手失败。

		// ClientAuth 的几种取值：
		//   NoClientCert               — 不要求客户端证书（普通 TLS）
		//   RequestClientCert          — 请求但不强制，没证书也放行
		//   RequireAnyClientCert       — 必须有证书，但不验证内容
		//   VerifyClientCertIfGiven    — 有就验，没有也放行
		//   RequireAndVerifyClientCert — 必须有且必须通过验证（最严）
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert

		// ClientCAs：服务器信任的 CA 证书池
		// 客户端证书的签名必须能被池中至少一个 CA 公钥验证通过
		// 验证不通过 → 握手失败 → 连接建立不了
		pool, err := loadCertPool(caFile)
		if err != nil {
			return nil, err
		}
		tlsCfg.ClientCAs = pool
	}

	// credentials.NewTLS(tlsCfg) → Go 标准 tls.Config → gRPC 的 TransportCredentials 接口
	// grpc.Creds(...) → TransportCredentials → grpc.ServerOption
	return grpc.Creds(credentials.NewTLS(tlsCfg)), nil
}

// NewClientTLS 创建 gRPC Client 的 TLS 配置选项。
//
// 参数：
//   caFile     — CA 证书路径，用于验证服务器证书是否由可信 CA 签发
//   certFile   — 客户端自己的证书路径（mTLS 时需要，普通 TLS 可留空）
//   keyFile    — 客户端私钥路径（mTLS 时需要）
//   serverName — 期望的服务器名称，会跟服务器证书 SAN 做比对。空表示不校验
//
// 返回值是 grpc.DialOption，传给 grpc.Dial() / grpc.NewClient()。
// 如果 caFile 为空，返回 nil 表示不启用 TLS。
func NewClientTLS(caFile, certFile, keyFile, serverName string) (grpc.DialOption, error) {
	if caFile == "" {
		return nil, nil
	}

	// RootCAs：客户端信任的根 CA 证书池
	// TLS 握手时，服务器出示证书，客户端用池中的 CA 公钥逐一尝试验证签名。
	// 只要有一个 CA 验证通过，服务器身份就是合法的。
	// 如果服务器证书的签名无法被任何 CA 验证 → 握手失败。
	pool, err := loadCertPool(caFile)
	if err != nil {
		return nil, fmt.Errorf("加载 CA 证书失败 %s: %w", caFile, err)
	}

	tlsCfg := &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
	}

	// ServerName：SNI（Server Name Indication）校验
	// 客户端对服务器证书上的 SAN（Subject Alternative Name）做严格比对。
	// 例如：serverName = "node-0"，服务器证书必须包含 "node-0" 在 DNS SAN 中。
	// 这个机制防止一种特定的中间人攻击：
	//   攻击者持有同一 CA 签发的另一个合法证书（比如攻击者自己的节点证书），
	//   但证书 SAN 里写的是 "attacker"，不是 "node-0"。
	//   客户端校验 serverName → 不匹配 → 拒连。
	if serverName != "" {
		tlsCfg.ServerName = serverName
	}

	// mTLS 场景：客户端也出示自己的证书
	// 加载自己的证书和私钥，TLS 握手时服务器要求证书就出示
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("加载客户端证书失败 %s/%s: %w", certFile, keyFile, err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)), nil
}

// loadCertPool 从 PEM 文件加载 CA 证书，构建可信证书池。
//
// PEM 文件内容示例：
//
//	-----BEGIN CERTIFICATE-----
//	MIIBoDCCAUegAwIBAgI...（Base64 编码的 DER 证书数据）
//	-----END CERTIFICATE-----
//
// AppendCertsFromPEM 会跳过 PEM 头尾标记找到中间的 Base64 数据，
// 解码为 DER 二进制，再解析为 X.509 证书结构体，加入池中。
func loadCertPool(caFile string) (*x509.CertPool, error) {
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("读取 CA 文件失败 %s: %w", caFile, err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("CA 证书 %s 解析失败：文件中没有找到有效的 PEM 格式证书", caFile)
	}
	return pool, nil
}
