package kv

import (
	"TicketX/internal/db"
	"TicketX/internal/lease"
	"TicketX/internal/raft"
	"TicketX/internal/watch"
	"TicketX/proto"
	"fmt"
	"os"
	"sync"
	"time"

	po "google.golang.org/protobuf/proto"
)

// 把applyloop结果返回给put/get的
type result struct {
	Value   string
	Version int64
	Err     proto.ErrorType
}

type KvServer struct {
	mu         sync.Mutex
	store      *db.Store
	currentRev int64
	proto.UnimplementedKvServer
	applyCh chan raft.ApplyMsg    //和raft通信的管道
	waitCh  map[int64]chan result //确保put请求成功commit的管道

	getCh map[int64]chan result //get fallback: ReadIndex 失败时走 Raft 日志

	lastRequest map[int64]int64 //请求者对应的最后一个请求编号
	rf          *raft.Raft
	lastApplied int64
	lastResult  map[int]result //上一次请求的结果
	leaseMgr    *lease.LeaseManager
	latest      map[string]int64   //每个建的最新版本
	history     map[string][]int64 //每个建的历史版本

	watcherManager *watch.WatcherManager
	eventNotifier  chan watch.WatchEvent
}

type KeyValue struct {
	Key   string
	Value string
}

func (kv *KvServer) GetCurrentRevesion() int64 {
	return kv.currentRev
}

func (kv *KvServer) GetRaft() *raft.Raft {
	return kv.rf
}

// 仅 leader 扫描，发现到期后通过 Raft 提交 Expire 命令。
func (kv *KvServer) leaseExpireWorker() {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			continue
		}
		now := time.Now().Unix()
		expiredKeys := kv.leaseMgr.ExpiredKeys(now)

		for _, key := range expiredKeys {
			op := &proto.Op{Type: "Expire", Key: key}
			data, _ := po.Marshal(op)
			kv.rf.Start(data)
		}
	}
}

func MakeKVServer(peers []string, me int, dataDir string) *KvServer {
	applych := make(chan raft.ApplyMsg)

	kv := &KvServer{}
	path := fmt.Sprintf("dataDir/node-%d", me)
	os.MkdirAll(path, 0755)

	store, err := db.NewStore(path)
	if err != nil {
		panic(err)
	}
	kv.store = store
	kv.applyCh = applych
	kv.rf = raft.MakeRaft(applych, peers, int32(me))
	kv.waitCh = make(map[int64]chan result)
	kv.lastRequest = make(map[int64]int64)
	kv.getCh = make(map[int64]chan result)
	kv.lastResult = make(map[int]result)
	kv.history = make(map[string][]int64)
	kv.latest = make(map[string]int64)
	kv.leaseMgr = lease.NewLeaseManager(1 * time.Second)

	kv.watcherManager = watch.NewWatcherManager()
	go kv.applier() //循环执行命令
	go kv.leaseExpireWorker()

	return kv
}
