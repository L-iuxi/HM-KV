package config

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
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
	Peers               []string      `yaml:"peers"`                 // 所有节点地址列表
	ElectionTimeoutMin  time.Duration `yaml:"election_timeout_min"`  // 选举超时下限
	ElectionTimeoutMax  time.Duration `yaml:"election_timeout_max"`  // 选举超时上限
	HeartbeatInterval   time.Duration `yaml:"heartbeat_interval"`    // Leader 心跳间隔
	RPCTimeout          time.Duration `yaml:"rpc_timeout"`           // Raft RPC 超时
	ReadIndexTimeout    time.Duration `yaml:"read_index_timeout"`    // ReadIndex 确认超时
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

// Default 返回带默认值的配置
func Default() *Config {
	return &Config{
		Node: NodeConfig{
			ID:      0,
			DataDir: "data",
		},
		Server: ServerConfig{
			Address: ":50051",
		},
		Raft: RaftConfig{
			Peers:              []string{},
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
			RPCTimeout:         200 * time.Millisecond,
			ReadIndexTimeout:   500 * time.Millisecond,
		},
		KV: KVConfig{
			ApplyBatchInterval: 5 * time.Millisecond,
			CompactInterval:    1 * time.Minute,
			SnapshotThreshold:  10 * 1024 * 1024, // 10MB
		},
		Lease: LeaseConfig{
			CheckInterval: 300 * time.Millisecond,
			MinTTL:        1 * time.Second,
		},
	}
}

// Load 从 YAML 文件加载配置，再用环境变量覆盖。
// 环境变量格式：HMETCD_NODE_ID=1, HMETCD_RAFT_HEARTBEAT_INTERVAL=100ms
func Load(path string) (*Config, error) {
	cfg := Default()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("read config file: %w", err)
		}
		if err == nil {
			if err := yaml.Unmarshal(data, cfg); err != nil {
				return nil, fmt.Errorf("parse config file: %w", err)
			}
		}
	}

	applyEnvOverrides(cfg)
	return cfg, nil
}

// applyEnvOverrides 环境变量覆盖配置值
func applyEnvOverrides(cfg *Config) {
	v := reflect.ValueOf(cfg).Elem()
	walkStruct(v, "HMETCD")
}

func walkStruct(v reflect.Value, prefix string) {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldVal := v.Field(i)
		envKey := prefix + "_" + strings.ToUpper(field.Name)

		if fieldVal.Kind() == reflect.Struct {
			walkStruct(fieldVal, envKey)
			continue
		}

		envVal := os.Getenv(envKey)
		if envVal == "" {
			continue
		}

		if fieldVal.CanSet() {
			setField(fieldVal, envVal)
		}
	}
}

func setField(field reflect.Value, val string) {
	switch field.Kind() {
	case reflect.String:
		field.SetString(val)
	case reflect.Int:
		n, err := strconv.Atoi(val)
		if err == nil {
			field.SetInt(int64(n))
		}
	case reflect.Int64:
		// 处理 time.Duration（底层是 int64）
		d, err := time.ParseDuration(val)
		if err == nil {
			field.SetInt(int64(d))
		} else {
			n, err := strconv.ParseInt(val, 10, 64)
			if err == nil {
				field.SetInt(n)
			}
		}
	case reflect.Slice:
		// 切片：逗号分隔
		if field.Type().Elem().Kind() == reflect.String {
			parts := strings.Split(val, ",")
			slice := reflect.MakeSlice(field.Type(), len(parts), len(parts))
			for i, p := range parts {
				slice.Index(i).SetString(strings.TrimSpace(p))
			}
			field.Set(slice)
		}
	}
}

// Peers 返回节点地址列表（YAML 配置的 peers 或命令行推导）
func (c *Config) Peers() []string {
	if len(c.Raft.Peers) > 0 {
		return c.Raft.Peers
	}
	// 无 peers 配置时，视为单节点
	if c.Server.Address != "" {
		return []string{c.Server.Address}
	}
	return []string{":50051"}
}
