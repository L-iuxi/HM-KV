package kv

import (
	"TicketX/internal/config"
	"TicketX/internal/lease"
	"TicketX/internal/mvcc"
	"TicketX/internal/raft"
	"TicketX/internal/watch"
	"TicketX/proto"
	"context"
	"sync"
)

// 把applyloop结果返回给put/get的
type result struct {
	Value   string
	Version int64
	LeaseID int64
	Err     proto.ErrorType
}

type KvServer struct {
	mu sync.Mutex
	proto.UnimplementedKvServer
	applyCh chan raft.ApplyMsg    //和raft通信的管道
	waitCh  map[int64]chan result //确保put请求成功commit的管道

	getCh map[int64]chan result //get fallback: ReadIndex 失败时走 Raft 日志

	lastRequest map[int64]int64 //请求者对应的最后一个请求编号
	rf          *raft.Raft
	lastApplied int64
	lastResult  map[int]result //上一次请求的结果
	leaseMgr    *lease.LeaseManager
	mvcc        *mvcc.MVCC
	cfg         *config.Config //配置

	watcherManager *watch.WatcherManager
	eventNotifier  chan watch.WatchEvent
}

// KvEngine gRPC 服务接口
type KvEngine interface {
	// 读写
	Put(ctx context.Context, req *proto.PutRequest) (*proto.PutReply, error)
	Get(ctx context.Context, req *proto.GetRequest) (*proto.GetReply, error)
	Delete(ctx context.Context, req *proto.DeleteRequest) (*proto.DeleteReply, error)
	Batch(ctx context.Context, req *proto.BatchRequest) (*proto.BatchReply, error)

	// Watch
	Watch(req *proto.WatchRequest, stream proto.Kv_WatchServer) error

	// Lease
	KeepAlive(ctx context.Context, req *proto.KeepAliveRequest) (*proto.KeepAliveReply, error)

	// Compact
	Compact(ctx context.Context, req *proto.CompactRequest) (*proto.CompactReply, error)

	// Raft 访问
	GetRaft() *raft.Raft
}

type KeyValue struct {
	Key   string
	Value string
}
