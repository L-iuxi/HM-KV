// certgen 是 HMETCD 的 TLS 证书生成工具。
//
// 它负责生成一套完整的证书体系，供 HMETCD 集群开启 TLS 加密通信使用。
//
// # 证书体系说明
//
// 集群通信需要三样东西：
//  1. CA（证书颁发机构）—— 自签名的根证书，用来给其他证书"背书"。CA 自己不加密通信，
//     它的公钥会被分发给所有节点和客户端，用来验证对方证书是否由同一个 CA 签发。
//  2. 节点证书（Server Cert）—— 每个节点各一份，节点启动 gRPC 服务时使用。
//     证书的 SAN（Subject Alternative Name）里要写上该节点的地址（IP/域名），
//     否则客户端连接时会报"证书名称不匹配"。
//  3. 客户端证书（Client Cert）—— 客户端连接服务器时出示。mTLS 模式下，
//     服务器会验证客户端证书，不认识的客户端一律拒绝。
//
// # 用法
//
//	# 1. 生成 CA（只需一次）
//	go run cmd/certgen/main.go --ca --out certs
//
//	# 2. 为每个节点生成证书（地址用实际 IP 或域名）
//	go run cmd/certgen/main.go --node --hosts "localhost,127.0.0.1,192.168.1.10" --out certs --name node-0
//
//	# 3. 生成客户端证书
//	go run cmd/certgen/main.go --client --out certs --name client
//
//	# 生成后把 certs/ 目录分发到各节点的对应路径。
//
// # 证书文件命名
//
//	certs/
//	  ca.pem           — CA 公钥证书（公开，所有节点和客户端都需要）
//	  ca-key.pem       — CA 私钥（绝密，只用于签发，签发后可离线保存或销毁）
//	  node-0.pem       — 节点 0 的公钥证书
//	  node-0-key.pem   — 节点 0 的私钥（每个节点只持有自己的）
//	  client.pem       — 客户端的公钥证书
//	  client-key.pem   — 客户端的私钥
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ================================================================
// 命令行参数
// ================================================================

var (
	outDir  = flag.String("out", "certs", "证书输出目录")  // 所有证书文件写入此目录
	caFlag  = flag.Bool("ca", false, "生成 CA 根证书")   // --ca 触发 CA 生成
	nodeFlag = flag.Bool("node", false, "生成节点证书")   // --node 触发节点证书生成
	clientFlag = flag.Bool("client", false, "生成客户端证书") // --client 触发客户端证书生成
	hostsStr = flag.String("hosts", "localhost,127.0.0.1", "证书 SAN 地址列表，逗号分隔（节点证书使用）")
	name     = flag.String("name", "", "证书文件前缀，如 node-0 / node-1 / client")
)

func main() {
	flag.Parse()

	// 至少选一种模式
	if !*caFlag && !*nodeFlag && !*clientFlag {
		fmt.Println("请指定运行模式，如 --ca / --node / --client。--help 查看详细说明。")
		os.Exit(1)
	}

	// 模式互斥检查
	if (*caFlag && *nodeFlag) || (*caFlag && *clientFlag) || (*nodeFlag && *clientFlag) {
		fmt.Println("--ca / --node / --client 每次只能选一个，分三次运行。")
		os.Exit(1)
	}

	// 确保输出目录存在
	if err := os.MkdirAll(*outDir, 0755); err != nil {
		fmt.Printf("创建输出目录 %s 失败: %v\n", *outDir, err)
		os.Exit(1)
	}

	switch {
	case *caFlag:
		genCA()
	case *nodeFlag:
		if *name == "" {
			fmt.Println("--node 模式下必须指定 --name，如 --name node-0")
			os.Exit(1)
		}
		genNode(*name, parseHosts(*hostsStr))
	case *clientFlag:
		if *name == "" {
			*name = "client"
		}
		genClient(*name)
	}
}

// parseHosts 把逗号分隔的 host 字符串解析为 []string
func parseHosts(s string) []string {
	parts := strings.Split(s, ",")
	var hosts []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			hosts = append(hosts, p)
		}
	}
	return hosts
}

// ================================================================
// 核心函数
// ================================================================

// genCA 生成自签名的 CA 根证书。
//
// CA 是整个证书信任链的"根"：
//   - 它用自己的私钥给自己签名（自签名），不依赖任何上级机构。
//   - 它的公钥证书（ca.pem）被所有节点和客户端信任：
//     配置 TLS 时把 ca.pem 加入"信任的 CA 列表"，
//     Go 的 TLS 库就会用 CA 公钥去验证对端证书的签名是否合法。
//   - 私钥（ca-key.pem）必须妥善保管，只用于签发子证书。
//
// 签名的含义：
//
//	"CA 用自己的私钥为下级证书的内容计算一个数字签名，
//	 验证方用 CA 公钥就能确认：这个证书确实是 CA 签发的，内容没被篡改。"
//
// 这就是 TLS 里"信任链"的基础：你信任 CA，所以信任 CA 签发的所有证书。
func genCA() {
	// ----- 第 1 步：生成 CA 的私钥 -----
	// ECDSA P256 曲线：256 位密钥，安全性和 RSA 3072 相当，但速度快得多。
	// Go 标准库推荐用于 TLS。
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		fmt.Printf("生成 CA 私钥失败: %v\n", err)
		os.Exit(1)
	}

	// ----- 第 2 步：构造证书模板 -----
	// 证书本质上是一个"身份证"，包含以下字段：
	//   - SerialNumber：证书的唯一编号（用于吊销列表 CRL）
	//   - Subject：证书持有者信息（CN=Common Name 是最关键的字段）
	//   - NotBefore/NotAfter：有效期，CA 一般设 10 年
	//   - IsCA: true：声明"我是 CA，我有权签发其他证书"
	//   - KeyUsage：声明密钥用途。CertSign 表示"此密钥用于签发证书"
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128)) // 128 位随机序列号
	caTemplate := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   "HMETCD CA",           // CA 名称，显示在证书信息里
			Organization: []string{"HMETCD"},    // 组织名
		},
		NotBefore:             time.Now(),                              // 生效时间
		NotAfter:              time.Now().Add(365 * 10 * 24 * time.Hour), // 有效期 10 年
		IsCA:                  true,                                    // 关键：标记为 CA 证书
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign, // 允许签发证书和吊销列表
		BasicConstraintsValid: true,                                    // 必须设为 true，IsCA 才会生效
	}

	// ----- 第 3 步：自签名 -----
	// "签名"就是用私钥对证书内容的哈希做加密。
	// 签名参数里的 caTemplate 出现两次，意思是"用 caTemplate 作为证书内容，
	// 用 caKey 对应的公钥（&caKey.PublicKey）作为签发者信息，用 caKey 签名"。
	// 签出来的结果叫"自签名证书"。
	caDER, err := x509.CreateCertificate(
		rand.Reader,    // 随机数源
		caTemplate,     // 证书内容（要签发什么）
		caTemplate,     // 父证书内容（谁签发——这里是"自己签发自己"）
		&caKey.PublicKey, // 父证书的公钥（跟私钥配对，写入证书的 Authority Key Identifier）
		caKey,          // 父证书的私钥（真正执行签名操作）
	)
	if err != nil {
		fmt.Printf("创建 CA 证书失败: %v\n", err)
		os.Exit(1)
	}

	// ----- 第 4 步：PEM 编码写入文件 -----
	// x509.CreateCertificate 返回的是 DER 格式（二进制）。
	// PEM 格式是 DER 的 Base64 编码 + 头尾标记行，方便文本传输。
	// 几乎所有 TLS 工具都要求 PEM 格式。
	saveKey(filepath.Join(*outDir, "ca-key.pem"), caKey)
	saveCert(filepath.Join(*outDir, "ca.pem"), caDER)
	fmt.Printf("CA 证书已生成到 %s/ca.pem（私钥 %s/ca-key.pem，请妥善保管）\n", *outDir, *outDir)
}

// genNode 生成一份节点证书，由 CA 签名。
//
// 节点证书的作用：
//   - 服务器启动 TLS 监听时出示给客户端，客户端用 CA 公钥验证它的真伪。
//   - mTLS 场景下，节点之间互相连接也使用该证书。
//
// 最关键的概念——SAN（Subject Alternative Name）：
//
//	客户端连接服务器时，会校验证书的"身份"是否匹配目标地址。
//	比如客户端连 "192.168.1.10:50051"，证书里就必须包含这个 IP，
//	否则 TLS 握手阶段就会失败，报错 "certificate is valid for xxx, not yyy"。
//
//	SAN 就是证书的"合法身份列表"，可以写 IP 也可以写域名：
//	  - 写 "localhost"  → 本地开发用
//	  - 写 "127.0.0.1"  → 本机回环
//	  - 写实际 IP        → 跨机器部署
//	  - 写域名            → 生产环境
//
//	CommonName 曾经被用来做身份校验，但现代 TLS 已废弃这种做法，
//	必须用 SAN。Go 1.15+ 默认只校验证书中的 SAN 字段。
func genNode(name string, hosts []string) {
	// 1. 加载 CA 证书和私钥（签发要用）
	caCert, caKey := loadCA(*outDir)

	// 2. 生成节点私钥
	nodeKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		fmt.Printf("生成节点私钥失败: %v\n", err)
		os.Exit(1)
	}

	// 3. 构造节点证书模板
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	nodeTemplate := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   name,              // 证书显示名
			Organization: []string{"HMETCD"},
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(365 * 2 * 24 * time.Hour), // 节点证书有效期 2 年，到期重新签发
		// KeyUsage: 此密钥用于 TLS 握手中的"数字签名"步骤（证明"我就是证书持有者"）
		// 和"密钥加密"步骤（DH 密钥交换）
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		// ExtKeyUsage: 进一步限定用途——只用于 TLS 的服务器端和客户端认证
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}

	// 4. 填充 SAN（这是最关键的一步）
	for _, host := range hosts {
		// 尝试解析为 IP 地址
		if ip := net.ParseIP(host); ip != nil {
			// 是 IP 地址（如 "127.0.0.1"），加到 IP SAN
			nodeTemplate.IPAddresses = append(nodeTemplate.IPAddresses, ip)
		} else {
			// 不是 IP（如 "localhost"），加到 DNS SAN
			nodeTemplate.DNSNames = append(nodeTemplate.DNSNames, host)
		}
	}

	// 5. 用 CA 签名
	// 这里签名参数：
	//   - nodeTemplate：要签发的内容
	//   - caCert：上级证书（CA）
	//   - &nodeKey.PublicKey：把节点的公钥写入证书
	//   - caKey：用 CA 私钥签名
	nodeDER, err := x509.CreateCertificate(rand.Reader, nodeTemplate, caCert, &nodeKey.PublicKey, caKey)
	if err != nil {
		fmt.Printf("创建节点证书失败: %v\n", err)
		os.Exit(1)
	}

	saveKey(filepath.Join(*outDir, name+"-key.pem"), nodeKey)
	saveCert(filepath.Join(*outDir, name+".pem"), nodeDER)
	fmt.Printf("节点 %s 证书已生成（SAN: %s）\n", name, strings.Join(hosts, ", "))
}

// genClient 生成客户端证书，由 CA 签名。
//
// 客户端证书在 mTLS（双向 TLS）中起作用：
//   普通 TLS：只验证服务器身份（客户端确认"我没连错服务器"）。
//   mTLS：服务器也要验证客户端身份（"你是谁，有没有权限连接"）。
//
// 工作流程：
//   1. TLS 握手前半段：服务器出示自己的证书，客户端验证（跟普通 TLS 一样）。
//   2. TLS 握手后半段——CertificateRequest：服务器说"请出示你的证书"。
//   3. 客户端发送自己的证书。
//   4. 服务器用 CA 公钥验证客户端证书 → 通过则建立连接，不通过则断开。
//
// mTLS 常用于：
//   - 服务间通信（微服务之间互相认证）
//   - 管理接口（只有持有合法证书的 admin 才能操作）
func genClient(name string) {
	caCert, caKey := loadCA(*outDir)

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		fmt.Printf("生成客户端私钥失败: %v\n", err)
		os.Exit(1)
	}

	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	clientTemplate := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   name,
			Organization: []string{"HMETCD"},
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(365 * 2 * 24 * time.Hour),
		KeyUsage:  x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		// 客户端证书的 ExtKeyUsage 只需要 ClientAuth
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		fmt.Printf("创建客户端证书失败: %v\n", err)
		os.Exit(1)
	}

	saveKey(filepath.Join(*outDir, name+"-key.pem"), clientKey)
	saveCert(filepath.Join(*outDir, name+".pem"), clientDER)
	fmt.Printf("客户端证书 %s 已生成\n", name)
}

// ================================================================
// 工具函数
// ================================================================

// loadCA 从文件加载 CA 证书和私钥，用于签发子证书。
func loadCA(dir string) (*x509.Certificate, *ecdsa.PrivateKey) {
	caPEM, err := os.ReadFile(filepath.Join(dir, "ca.pem"))
	if err != nil {
		fmt.Printf("读取 CA 证书失败: %v（请先执行 --ca 生成 CA）\n", err)
		os.Exit(1)
	}
	caKeyPEM, err := os.ReadFile(filepath.Join(dir, "ca-key.pem"))
	if err != nil {
		fmt.Printf("读取 CA 私钥失败: %v\n", err)
		os.Exit(1)
	}

	// PEM 解码：提取 PEM 块中的 DER 二进制数据
	caBlock, _ := pem.Decode(caPEM)
	if caBlock == nil {
		fmt.Println("CA 证书 PEM 解析失败")
		os.Exit(1)
	}
	caCert, err := x509.ParseCertificate(caBlock.Bytes) // DER → Go 的 *x509.Certificate
	if err != nil {
		fmt.Printf("解析 CA 证书失败: %v\n", err)
		os.Exit(1)
	}

	keyBlock, _ := pem.Decode(caKeyPEM)
	if keyBlock == nil {
		fmt.Println("CA 私钥 PEM 解析失败")
		os.Exit(1)
	}
	caKey, err := x509.ParseECPrivateKey(keyBlock.Bytes) // DER → Go 的 *ecdsa.PrivateKey
	if err != nil {
		fmt.Printf("解析 CA 私钥失败: %v\n", err)
		os.Exit(1)
	}

	return caCert, caKey
}

// saveCert 把 DER 格式的证书编码为 PEM 写入文件。
//
// PEM 格式回顾：
//
//	-----BEGIN CERTIFICATE-----
//	MIIDITCCAgmgAwIBAgI...（Base64 编码的 DER 数据）
//	-----END CERTIFICATE-----
func saveCert(path string, der []byte) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Printf("创建文件 %s 失败: %v\n", path, err)
		os.Exit(1)
	}
	defer f.Close()
	pem.Encode(f, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: der,
	})
}

// saveKey 把 ECDSA 私钥编码为 PEM 写入文件。
//
// EC PRIVATE KEY（未加密）的 PEM 块：
//
//	-----BEGIN EC PRIVATE KEY-----
//	MHcCAQEEIO...（Base64）
//	-----END EC PRIVATE KEY-----
//
// 注：生产环境建议加密私钥，但这里为开发便利不加密。
// etcd 也是这样做的，依赖文件系统权限保护私钥。
func saveKey(path string, key *ecdsa.PrivateKey) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Printf("创建文件 %s 失败: %v\n", path, err)
		os.Exit(1)
	}
	defer f.Close()
	// 把 ECDSA 私钥序列化为 DER，再封装为 PEM
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		fmt.Printf("序列化私钥失败: %v\n", err)
		os.Exit(1)
	}
	pem.Encode(f, &pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: keyDER,
	})
}
