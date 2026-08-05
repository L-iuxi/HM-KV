package kv

import (
	"TicketX/internal/config"
	"TicketX/internal/db"
	"TicketX/internal/lease"
	"TicketX/internal/mvcc"
	"TicketX/internal/raft"
	"TicketX/internal/watch"
	"TicketX/proto"
	"fmt"
	"os"
	"time"

	po "google.golang.org/protobuf/proto"
)

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
			for key := range lease.Keys {
				op := &proto.Op{Type: "Expire", Key: key}
				data, _ := po.Marshal(op)
				kv.rf.Start(data)
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
	//raft
	kv.rf = raft.MakeRaft(applych, peers, int32(me), raft.RaftConfig{
		ElectionTimeoutMin: kv.cfg.Raft.ElectionTimeoutMin,
		ElectionTimeoutMax: kv.cfg.Raft.ElectionTimeoutMax,
		HeartbeatInterval:  kv.cfg.Raft.HeartbeatInterval,
		RPCTimeout:         kv.cfg.Raft.RPCTimeout,
		ReadIndexTimeout:   kv.cfg.Raft.ReadIndexTimeout,
	})
	//put请求通道
	kv.waitCh = make(map[int64]chan result)
	//clientid上次请求表
	kv.lastRequest = make(map[int64]int64)
	//get请求通道
	kv.getCh = make(map[int64]chan result)
	//requestid对应请求结果
	kv.lastResult = make(map[int]result)
}
