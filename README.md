# HMETCD

**Go 实现的分布式 KV 存储系统，基于 Raft 共识协议实现多节点数据一致性，采用 MVCC 管理数据多版本，并使用 BadgerDB 实现持久化存储，支持 Watch、Lease、Compact、Snapshot 及动态成员变更等核心功能。**

## 项目结构

```
cmd/
  server/        服务启动入口
  ctl/           交互式客户端 (hmctl)
  bench/         压测工具（throughput/recovery/lease/watch）
  certgen/       证书生成工具
clerk/           客户端 SDK
proto/           Protobuf + gRPC 定义
internal/
  raft/           Raft 共识（选举、心跳、日志、快照、WAL）
  kv/             KV Server（Apply、Get、Watch、Lock、Lease）
  mvcc/           MVCC 多版本控制
  lease/          Lease/TTL 管理
  watch/          Watch 事件管理
  lock/           分布式锁
  wal/            预写日志（文件系统）
  db/             BadgerDB 封装
  config/         配置系统（YAML + 环境变量）
  type/           共享类型
configs/          集群配置文件
```

## 快速开始

```bash
# 构建
make build

# 一键启动 3 节点集群（后台运行）
make cluster

# 停止集群
make stop-cluster

# 交互式客户端
make start-ctl
```

命令
```

hm> help
Commands:
  put    <key> <value> [ttl]    — write key
  get    <key>                  — read key
  delete <key>                  — delete key
  prefix <prefix>               — list keys with prefix
  watch  <key>                  — watch key for changes (bg)
  watchprefix <prefix>          — watch prefix for changes (bg)
  grant  <ttl>                  — create a lease, returns lease ID
  putlease <key> <val> <lease>  — write key bound to existing lease
  keepalive <key>               — renew lease on key
  compact <revision>            — compact history up to revision
  help                          — show this
  exit                          — quit
```


## 核心特性

**Raft 共识** — Leader 选举、日志复制（AppendEntries）、快照安装（InstallSnapshot）。WAL 持久化保证崩溃恢复，节点重启后回放日志回到一致状态。

**并发 ReadIndex** — 每个读请求独立 gate，心跳响应同时推进所有等待中的请求，避免串行阻塞。64 并发 7,344 ops/s，P50 9.4ms。

**MVCC** — 多版本 key/revision 存储模型，支持 CAS 乐观锁、历史版本查询。BadgerDB 关闭后通过 `Recover()` 扫描重建 latest/history/revisions 全部索引。

**Watch 事件流** — key/prefix 级别监听，支持从指定 revision 回放历史事件。Clerk 侧自动重连 + 断点续订（exponential backoff，最大 30s），Leader 切换时透明重建流。

**Lease / TTL** — 绑定 key + TTL，到期自动删除，KeepAlive 续约。分布式锁基于 CAS + Lease 实现，超时自动释放。

**成员变更** — 运行时 AddPeer / RemovePeer，in-place 更新 peers 列表，peerGen 递增做 goroutine 安全检查，心跳/选举协程 O(1) 校验无需加锁。

**TLS/mTLS** — ECDSA P256 双向认证，`cmd/certgen` 一键生成证书。Clerk SDK 支持 `WithTLS(ca, cert, key)` 配置。

**事务** — If-Then-Else 语义，多 key 原子操作，支持版本比较条件。

**压测工具** — 4 场景（throughput / recovery / lease / watch），可指定并发数、读写比例、持续时间，支持连接外部集群。

## 压测

```bash
go run cmd/bench/main.go                    # 吞吐压测（内嵌集群）
go run cmd/bench/main.go --scene=recovery   # 崩溃恢复
go run cmd/bench/main.go --scene=lease      # Lease 压力
go run cmd/bench/main.go --scene=watch      # Watch 压力
```

详见 [TEST.md](TEST.md)。

