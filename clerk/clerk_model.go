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

type Client struct {
	addrs []string
	conns []*grpc.ClientConn
	kvcs  []proto.KvClient

	leaderIdx   atomic.Int32
	knownLeader atomic.Int64 // 上次 WRONG_LEADER 返回的 leader 节点索引，-1 未知
	clientID    int64
	requestID   atomic.Int64

	mu sync.Mutex
}

type KeyValue struct {
	Key   string
	Value string
}
