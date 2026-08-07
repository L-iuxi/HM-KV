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

// Default 返回带默认值的配置
/*
默认值->yaml配置覆盖->环境变量覆盖
*/
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
func Load(path string) (*Config, error) {
	//先读取默认配置
	cfg := Default()

	if path != "" {
		//配置文件不存在
		data, err := os.ReadFile(path)
		if err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("read config file: %w", err)
		}
		if err == nil {
			//读取yaml获取配置
			if err := yaml.Unmarshal(data, cfg); err != nil {
				return nil, fmt.Errorf("parse config file: %w", err)
			}
		}
	}
	//读取环境变量
	applyEnvOverrides(cfg)
	return cfg, nil
}

// applyEnvOverrides 环境变量覆盖配置值
func applyEnvOverrides(cfg *Config) {
	v := reflect.ValueOf(cfg).Elem()
	walkStruct(v, "HMETCD")
}

// 反射自动找环境变量
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

// 按照反射出来的类型转换为变量
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

// Peers 返回节点地址列表
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
