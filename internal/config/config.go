package config

import (
	"time"
)

// Config 全局配置
type Config struct {
	Node   NodeConfig   `yaml:"node"`
	Raft   RaftConfig   `yaml:"raft"`
	KV     KVConfig     `yaml:"kv"`
	Lease  LeaseConfig  `yaml:"lease"`
	Server ServerConfig `yaml:"server"`
}

// NodeConfig 节点信息
type NodeConfig struct {
	ID      int    `yaml:"id"`       // 节点编号，0-based
	DataDir string `yaml:"data_dir"` // BadgerDB 数据目录
}

// ServerConfig gRPC 服务监听地址
type ServerConfig struct {
	Address string `yaml:"address"` // 如 ":50051"
}

// RaftConfig Raft 协议参数
type RaftConfig struct {
	Peers              []string      `yaml:"peers"`                // 所有节点地址列表
	ElectionTimeoutMin time.Duration `yaml:"election_timeout_min"` // 选举超时下限
	ElectionTimeoutMax time.Duration `yaml:"election_timeout_max"` // 选举超时上限
	HeartbeatInterval  time.Duration `yaml:"heartbeat_interval"`   // Leader 心跳间隔
	RPCTimeout         time.Duration `yaml:"rpc_timeout"`          // Raft RPC 超时
	ReadIndexTimeout   time.Duration `yaml:"read_index_timeout"`   // ReadIndex 确认超时
}

// KVConfig KV 层参数
type KVConfig struct {
	ApplyBatchInterval time.Duration `yaml:"apply_batch_interval"` // apply 批处理间隔
	CompactInterval    time.Duration `yaml:"compact_interval"`     // compact 执行间隔
	SnapshotThreshold  int64         `yaml:"snapshot_threshold"`   // 日志超过多少字节触发快照
}

// LeaseConfig Lease 参数
type LeaseConfig struct {
	CheckInterval time.Duration `yaml:"check_interval"` // 过期检查间隔
	MinTTL        time.Duration `yaml:"min_ttl"`        // 最小 TTL
}
