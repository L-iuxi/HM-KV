package clerk

import (
	"TicketX/proto"
	"errors"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrWrongLeader = errors.New("wrong leader, retry with new leader")
	ErrNotApplied  = errors.New("command not yet applied")
	ErrCASConflict = errors.New("cas conflict: key was modified by someone else")
)

type Event struct {
	Type     string // "Put"/"Delete"
	Key      string
	Value    string
	Revision int64
}

// Client HMETCD 客户端，线程安全。
//
// 字段说明：
//   addrs — 所有节点地址列表
//   conns — 到每个节点的 gRPC 长连接
//   kvcs — 每个连接对应的 KV 客户端 stub
//   leaderIdx — 当前认为的 Leader 在 addrs 中的下标
//   knownLeader — 服务器端返回的 Leader 下标（WRONG_LEADER 时优先尝试此节点）
//   clientID — 客户端唯一 ID（随机生成，用于去重和分布式锁）
//   requestID — 请求递增 ID（原子递增，保证全局唯一）
type Client struct {
	addrs []string           // 服务器集群地址
	conns []*grpc.ClientConn // 每个 conn 对应一个 gRPC 长连接
	kvcs  []proto.KvClient

	leaderIdx   atomic.Int32 // 当前认为的 leader，atomic 保证并发安全
	knownLeader atomic.Int64 // 上次 WRONG_LEADER 返回的 leader 节点索引，-1 未知
	clientID    int64        // 本客户端 ID
	requestID   atomic.Int64 // 请求 ID

	mu sync.Mutex
}

type KeyValue struct {
	Key   string
	Value string
}
