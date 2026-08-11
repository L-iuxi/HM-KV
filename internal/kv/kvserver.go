package kv

import (
	"TicketX/internal/config"
	"TicketX/internal/db"
	"TicketX/internal/lease"
	"TicketX/internal/lock"
	"TicketX/internal/mvcc"
	"TicketX/internal/raft"
	"TicketX/internal/watch"
	"TicketX/proto"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	po "google.golang.org/protobuf/proto"
)

// Close 关闭 KV 服务器：停止 Raft 后台 goroutine、关闭 Badger DB。
// 用于测试中模拟崩溃后重启场景。
func (kv *KvServer) Close() error {
	kv.rf.Stop()
	return kv.mvcc.Close()
}

func (kv *KvServer) GetCurrentRevesion() int64 {
	return kv.mvcc.CurrentRev()
}

func (kv *KvServer) GetRaft() *raft.Raft {
	return kv.rf
}

// 仅 leader 扫描过期 lease，通过 Raft 提交 Expire 命令确保集群一致。
func (kv *KvServer) leaseExpireWorker() {
	ticker := time.NewTicker(kv.cfg.Lease.CheckInterval)
	defer ticker.Stop()

	for range ticker.C {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			continue
		}
		now := time.Now().Unix()
		leases := kv.leaseMgr.ExpiredLeases(now)

		for _, lease := range leases {
			fmt.Printf("[lease] expire lease %d (expired at %d, now %d, keys=%v)\n",
				lease.ID, lease.ExpiresAt, now, lease.Keys)
			for key := range lease.Keys {
				op := &proto.Op{Type: "Expire", Key: key}
				data, _ := po.Marshal(op)
				_, _, isLeader, _ := kv.rf.Start(data)
				if !isLeader {
					fmt.Printf("[lease] expire lease %d key %s: lost leadership\n", lease.ID, key)
				}
			}
		}
	}
}

func MakeKVServer(cfg *config.Config) *KvServer {
	kv := &KvServer{cfg: cfg}
	path := fmt.Sprintf("%s/node-%d", cfg.Node.DataDir, cfg.Node.ID)
	os.MkdirAll(path, 0755)

	store, err := db.NewStore(path)
	if err != nil {
		panic(err)
	}

	//初始化mvcc相关
	kv.InitMvcc(store)

	kv.leaseMgr = lease.NewLeaseManager(cfg.Lease.MinTTL)
	kv.readyCh = make(chan struct{})
	// 初始化kv相关
	kv.InitKvserver(cfg.Peers(), cfg.Node.ID)
	//初始化watch相关
	kv.InitWatch()

	go kv.applier() //循环执行命令
	go kv.leaseExpireWorker()
	go kv.snapshotWorker()

	return kv
}

// 初始化mvcc相关
func (kv *KvServer) InitMvcc(store *db.Store) {

	//新建mvcc
	kv.mvcc = mvcc.New(store)
	//从badger中恢复数据
	if err := kv.mvcc.Recover(); err != nil {
		panic(err)
	}
	// 启动后台compact
	kv.mvcc.StartCompact(kv.cfg.KV.CompactInterval)
}

// 初始化watch相关
func (kv *KvServer) InitWatch() {
	//注册watch管理
	kv.watcherManager = watch.NewWatcherManager()
}

// 初始化kv相关
func (kv *KvServer) InitKvserver(peers []string, me int) {
	applych := make(chan raft.ApplyMsg)
	//提交请求管道
	kv.applyCh = applych

	// ---- Raft 节点间通信的 TLS 配置 ----
	// 节点间通信（心跳、日志复制、快照、成员变更）走独立的 gRPC 连接。
	// 如果配置了 TLS，这些连接同样需要加密 — Raft 流量包含实际数据。
	//
	// serverName 留空的原因：
	//   节点间通信通常用 IP 地址（"192.168.1.10:50051"），
	//   而证书 SAN 写的是生成时的 IP — 如果部署时 IP 变了，严格校验会失败。
	//   留空 serverName = 只验证签名（是否由可信 CA 签发），不校验 SAN。
	//   安全性稍降但灵活很多，适合集群内部通信。
	//   如需更严格，可以把 serverName 设为节点在 peers 中的地址。
	var raftTLSOpt grpc.DialOption
	if kv.cfg.TLS.CA != "" {
		opt, err := config.NewClientTLS(kv.cfg.TLS.CA, kv.cfg.TLS.Cert, kv.cfg.TLS.Key, "")
		if err != nil {
			panic(fmt.Sprintf("加载 Raft TLS 配置失败: %v", err))
		}
		raftTLSOpt = opt
	}

	//raft
	kv.rf = raft.MakeRaft(applych, peers, int32(me), raft.RaftConfig{
		ElectionTimeoutMin: kv.cfg.Raft.ElectionTimeoutMin,
		ElectionTimeoutMax: kv.cfg.Raft.ElectionTimeoutMax,
		HeartbeatInterval:  kv.cfg.Raft.HeartbeatInterval,
		RPCTimeout:         kv.cfg.Raft.RPCTimeout,
		ReadIndexTimeout:   kv.cfg.Raft.ReadIndexTimeout,
			DataDir:            fmt.Sprintf("%s/node-%d", kv.cfg.Node.DataDir, kv.cfg.Node.ID),
	}, raftTLSOpt)
	//put请求通道
	kv.waitCh = make(map[int64]chan result)
	//clientid上次请求表
	kv.lastRequest = make(map[int64]int64)
	//get请求通道
	kv.getCh = make(map[int64]chan result)
	//requestid对应请求结果
	kv.lastResult = make(map[int]result)
	kv.lastTxnResult = make(map[int]txnresult)

	kv.loc = lock.NewLockManager(kv.leaseMgr, kv.mvcc, kv.watcherManager)
}
