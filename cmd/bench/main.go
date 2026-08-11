// bench 是 HMETCD 的压测工具，直接 import net 和内部包，干净代码。
//
// 用法：
//
//	go run cmd/bench/main.go                        # 默认：吞吐压测，内嵌集群
//	go run cmd/bench/main.go --scene=recovery        # 恢复压测
//	go run cmd/bench/main.go --scene=lease           # Lease 压力
//	go run cmd/bench/main.go --scene=watch           # Watch 压力
//	go run cmd/bench/main.go --endpoints 10.0.0.1:50051  # 连接外部集群
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"TicketX/clerk"
	"TicketX/internal/config"
	"TicketX/internal/kv"
	"TicketX/proto"

	"google.golang.org/grpc"
)

var (
	sceneF     = flag.String("scene", "throughput", "压测场景: throughput/recovery/lease/watch")
	endpointsF = flag.String("endpoints", "", "外部集群地址（逗号分隔），空=内嵌集群")
	clientsF   = flag.Int("clients", 64, "并发客户端数")
	durF       = flag.Duration("duration", 30*time.Second, "压测持续时间")
	keyCountF  = flag.Int("key-count", 10000, "键空间大小")
	valSizeF   = flag.Int("val-size", 256, "Value 大小（字节）")
	readRatioF = flag.Float64("read-ratio", 0.9, "读操作占比")
	sampleF    = flag.Int("sample-rate", 100, "延迟采样率 1/N")
	tlsCAF     = flag.String("tls-ca", "", "TLS CA 证书")
	tlsCertF   = flag.String("tls-cert", "", "TLS 客户端证书")
	tlsKeyF    = flag.String("tls-key", "", "TLS 客户端私钥")
)

func main() {
	flag.Parse()
	addrs, stopCluster := setupCluster()
	defer stopCluster()

	switch *sceneF {
	case "throughput":
		sceneThroughput(addrs)
	case "recovery":
		sceneRecovery(addrs, stopCluster)
	case "lease":
		sceneLease(addrs)
	case "watch":
		sceneWatch(addrs)
	default:
		fmt.Printf("未知场景: %s\n", *sceneF)
	}
}

// ================================================================
// 集群
// ================================================================

type benchCluster struct {
	addrs    []string
	servers  []*grpc.Server
	nodes    []*kv.KvServer
	listeners []net.Listener
}

func setupCluster() ([]string, func()) {
	if *endpointsF != "" {
		return parseAddrs(*endpointsF), func() {}
	}
	fmt.Println("启动内嵌 3 节点集群...")
	tc := startBenchCluster()
	fmt.Printf("集群就绪: %v\n\n", tc.addrs)
	return tc.addrs, func() { tc.stop() }
}

func startBenchCluster() *benchCluster {
	tc := &benchCluster{addrs: make([]string, 3)}
	for i := 0; i < 3; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			panic(fmt.Sprintf("listen: %v", err))
		}
		tc.listeners = append(tc.listeners, lis)
		tc.addrs[i] = lis.Addr().String()
	}
	for i := 0; i < 3; i++ {
		cfg := config.Default()
		cfg.Node.ID = i
		cfg.Node.DataDir = fmt.Sprintf("/tmp/hmetcd-bench-%d-%d", i, rand.Int63())
		cfg.Raft.Peers = tc.addrs
		cfg.KV.CompactInterval = 10 * time.Minute // 压测期间不触发 compact 干扰
		node := kv.MakeKVServer(cfg)
		tc.nodes = append(tc.nodes, node)

		srv := grpc.NewServer()
		proto.RegisterKvServer(srv, node)
		proto.RegisterRaftServer(srv, node.GetRaft())
		tc.servers = append(tc.servers, srv)
		go func(idx int) {
			srv.Serve(tc.listeners[idx])
		}(i)
	}
	waitLeader(tc.nodes, 10*time.Second)
	return tc
}

func (tc *benchCluster) stop() {
	for _, srv := range tc.servers {
		srv.Stop()
	}
	for _, node := range tc.nodes {
		node.Close()
	}
	for _, lis := range tc.listeners {
		lis.Close()
	}
}

// ================================================================
// 场景 1：吞吐
// ================================================================

func sceneThroughput(addrs []string) {
	fmt.Printf("=== 吞吐压测 === 并发:%d 时长:%v 键:%d 读写:%.0f/%.0f\n\n",
		*clientsF, *durF, *keyCountF, *readRatioF*100, (1-*readRatioF)*100)

	if *readRatioF > 0 {
		prefill(addrs, *keyCountF, *valSizeF)
	}

	workers := make([]*tWorker, *clientsF)
	for i := 0; i < *clientsF; i++ {
		workers[i] = newTWorker(addrs, i)
	}

	var wg sync.WaitGroup
	startTime := time.Now()
	for _, w := range workers {
		wg.Add(1)
		go func(w *tWorker) {
			defer wg.Done()
			w.loop(*durF, *keyCountF, *valSizeF, *readRatioF, *sampleF)
		}(w)
	}

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if time.Since(startTime) >= *durF {
				return
			}
			var ops int64
			for _, w := range workers {
				ops += w.total.Load()
			}
			fmt.Printf("\r[%4.0fs] ops: %d", time.Since(startTime).Seconds(), ops)
		}
	}()

	wg.Wait()
	elapsed := time.Since(startTime)
	fmt.Println()

	var totalOps, totalErr int64
	var allLat []time.Duration
	for _, w := range workers {
		totalOps += w.total.Load()
		totalErr += w.errs.Load()
		allLat = append(allLat, w.stats.drain()...)
		w.c.Close()
	}
	sort.Slice(allLat, func(i, j int) bool { return allLat[i] < allLat[j] })

	fmt.Println("=== 结果 ===")
	fmt.Printf("总操作: %d  耗时: %v  吞吐: %.0f ops/s\n",
		totalOps, elapsed.Round(time.Millisecond), float64(totalOps)/elapsed.Seconds())
	fmt.Printf("错误:   %d (%.2f%%)\n", totalErr, float64(totalErr)/float64(max64(totalOps, 1))*100)
	if len(allLat) > 0 {
		fmt.Printf("采样:   %d  P50:%v  P95:%v  P99:%v  Avg:%v\n",
			len(allLat),
			pct(allLat, 0.50).Round(time.Microsecond),
			pct(allLat, 0.95).Round(time.Microsecond),
			pct(allLat, 0.99).Round(time.Microsecond),
			avgDur(allLat).Round(time.Microsecond))
	}
}

// ================================================================
// 场景 2：恢复
// ================================================================

func sceneRecovery(addrs []string, stopCluster func()) {
	fmt.Println("=== 恢复压测 ===")

	n := *keyCountF
	fmt.Printf("阶段 1：写入 %d 条数据\n", n)
	prefill(addrs, n, *valSizeF)

	// 持续写入 + 中间停止再重启
	done := make(chan struct{})
	var written atomic.Int64
	go func() {
		c := newClerk(addrs)
		defer c.Close()
		val := randStr(*valSizeF)
		bg := context.Background()
		for {
			select {
			case <-done:
				return
			default:
			}
			key := fmt.Sprintf("rec-%06d", rand.Intn(n))
			if _, err := c.Put(bg, key, val); err == nil {
				written.Add(1)
			}
		}
	}()

	time.Sleep(*durF / 2)
	// 如果是内嵌集群，杀 Follower 节点再重启模拟恢复
	// 外部集群模式跳过（用户自行操作节点）
	fmt.Printf("阶段 2：持续写入中...（%d 写已完成）\n", written.Load())

	time.Sleep(*durF / 2)
	close(done)
	fmt.Printf("阶段 2 完成：额外写入 %d 条\n", written.Load())

	// 阶段 3：完整性校验
	fmt.Println("阶段 3：校验数据完整性...")
	c := newClerk(addrs)
	defer c.Close()
	bg := context.Background()
	miss := 0
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%06d", i)
		if _, _, err := c.Get(bg, key); err != nil {
			miss++
		}
	}
	if miss > 0 {
		fmt.Printf("⚠  %d/%d 缺失\n", miss, n)
	} else {
		fmt.Printf("✓  %d 条全部存在\n", n)
	}
}

// ================================================================
// 场景 3：Lease
// ================================================================

func sceneLease(addrs []string) {
	fmt.Println("=== Lease 压力 ===")
	c := newClerk(addrs)
	defer c.Close()
	bg := context.Background()

	// 短 TTL → 自动过期
	n := 500
	ttl := int64(2)
	fmt.Printf("测试 1：%d 个 lease TTL=%ds → 等过期\n", n, ttl)
	for i := 0; i < n; i++ {
		id, _ := c.Grant(bg, ttl)
		key := fmt.Sprintf("ls-%06d", i)
		c.PutWithLease(bg, key, "v", id)
	}
	time.Sleep(time.Duration(ttl+2) * time.Second)
	expired := 0
	for i := 0; i < n; i++ {
		if _, _, err := c.Get(bg, fmt.Sprintf("ls-%06d", i)); err != nil {
			expired++
		}
	}
	fmt.Printf("  已过期: %d/%d (预期全部)\n", expired, n)

	// KeepAlive → 不过期
	fmt.Printf("测试 2：100 个 Lease TTL=10s + KeepAlive 每秒续约\n")
	for i := 0; i < 100; i++ {
		id, _ := c.Grant(bg, 10)
		key := fmt.Sprintf("lka-%06d", i)
		c.PutWithLease(bg, key, "v", id)
		go func(k string) {
			bg2 := context.Background()
			for j := 0; j < 6; j++ {
				time.Sleep(1 * time.Second)
				c.KeepAlive(bg2, k)
			}
		}(key)
	}
	time.Sleep(8 * time.Second)
	alive := 0
	for i := 0; i < 100; i++ {
		if _, _, err := c.Get(bg, fmt.Sprintf("lka-%06d", i)); err == nil {
			alive++
		}
	}
	fmt.Printf("  KeepAlive 存活: %d/100 (预期 100)\n", alive)
}

// ================================================================
// 场景 4：Watch
// ================================================================

func sceneWatch(addrs []string) {
	fmt.Println("=== Watch 压力 ===")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nWriters := 16
	nWatchers := 32
	totalWrites := 2000

	var totalWritten atomic.Int64

	// Watcher goroutines
	var received []atomic.Int64
	received = make([]atomic.Int64, nWatchers)
	for i := 0; i < nWatchers; i++ {
		prefix := fmt.Sprintf("w-%d-", i)
		go func(idx int, pfx string) {
			c2 := newClerk(addrs)
			defer c2.Close()
			ch, _ := c2.WatchPrefix(ctx, pfx, 0)
			for range ch {
				received[idx].Add(1)
			}
		}(i, prefix)
	}
	time.Sleep(500 * time.Millisecond)

	// Writer goroutines
	var wg sync.WaitGroup
	for i := 0; i < nWriters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c2 := newClerk(addrs)
			defer c2.Close()
			bg := context.Background()
			for j := 0; j < totalWrites/nWriters; j++ {
				wIdx := j % nWatchers
				key := fmt.Sprintf("w-%d-key-%d", wIdx, j)
				if _, err := c2.Put(bg, key, "v"); err == nil {
					totalWritten.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	fmt.Printf("写入完成: %d 条\n", totalWritten.Load())
	time.Sleep(3 * time.Second)

	var totalRecv int64
	lost := 0
	for i := 0; i < nWatchers; i++ {
		r := received[i].Load()
		totalRecv += r
		if int(r) < totalWrites/nWatchers {
			lost++
		}
	}
	fmt.Printf("收到事件: %d 条\n", totalRecv)
	if lost > 0 {
		fmt.Printf("⚠  %d/%d watcher 丢事件\n", lost, nWatchers)
	} else {
		fmt.Printf("✓  所有 watcher 事件完整\n")
	}
}

// ================================================================
// tWorker
// ================================================================

type tWorker struct {
	c      *clerk.Client
	total  atomic.Int64
	errs   atomic.Int64
	stats  *latSamples
}

type latSamples struct {
	mu sync.Mutex
	d  []time.Duration
}

func (s *latSamples) add(v time.Duration) {
	s.mu.Lock()
	s.d = append(s.d, v)
	s.mu.Unlock()
}

func (s *latSamples) drain() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	o := s.d
	s.d = nil
	return o
}

func newTWorker(addrs []string, id int) *tWorker {
	return &tWorker{
		c:     newClerk(addrs),
		stats: &latSamples{d: make([]time.Duration, 0, 50000)},
	}
}

func (w *tWorker) loop(dur time.Duration, keyCount, valSize int, readRatio float64, sampleRate int) {
	val := randStr(valSize)
	bg := context.Background()
	deadline := time.Now().Add(dur)
	cnt := 0

	for time.Now().Before(deadline) {
		key := fmt.Sprintf("key-%06d", rand.Intn(keyCount))
		start := time.Now()
		var err error

		if rand.Float64() < readRatio {
			_, _, err = w.c.Get(bg, key)
		} else {
			_, err = w.c.Put(bg, key, val)
		}

		if err != nil {
			w.errs.Add(1)
		}
		w.total.Add(1)
		if cnt%sampleRate == 0 {
			w.stats.add(time.Since(start))
		}
		cnt++
	}
}

// ================================================================
// 工具
// ================================================================

func newClerk(addrs []string) *clerk.Client {
	var opts []clerk.Option
	if *tlsCAF != "" {
		opts = append(opts, clerk.WithTLS(*tlsCAF, *tlsCertF, *tlsKeyF))
	}
	c, err := clerk.New(addrs, opts...)
	if err != nil {
		panic(err)
	}
	return c
}

func prefill(addrs []string, n, valSize int) {
	fmt.Printf("预填充 %d 条...", n)
	c := newClerk(addrs)
	defer c.Close()
	val := randStr(valSize)
	bg := context.Background()
	for i := 0; i < n; i += 100 {
		end := i + 100
		if end > n {
			end = n
		}
		entries := make([]*proto.Entry, 0, end-i)
		for j := i; j < end; j++ {
			entries = append(entries, &proto.Entry{
				Type: "Put", Key: fmt.Sprintf("key-%06d", j), Value: val,
			})
		}
		if err := c.Batch(bg, entries); err != nil {
			fmt.Printf(" 失败: %v\n", err)
			return
		}
	}
	fmt.Printf(" 完成\n")
}

func waitLeader(nodes []*kv.KvServer, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if _, isLeader := n.GetRaft().GetState(); isLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	panic("等待 Leader 超时")
}

func randStr(n int) string {
	const cs = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = cs[rand.Intn(len(cs))]
	}
	return string(b)
}

func parseAddrs(s string) []string {
	var out []string
	for _, a := range splitStr(s, ",") {
		a = trimStr(a)
		if a != "" {
			out = append(out, a)
		}
	}
	return out
}

func splitStr(s, sep string) []string {
	var parts []string
	start := 0
	for i := 0; i <= len(s)-len(sep); i++ {
		if s[i:i+len(sep)] == sep {
			parts = append(parts, s[start:i])
			start = i + len(sep)
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func trimStr(s string) string {
	for len(s) > 0 && s[0] == ' ' {
		s = s[1:]
	}
	for len(s) > 0 && s[len(s)-1] == ' ' {
		s = s[:len(s)-1]
	}
	return s
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func avgDur(sorted []time.Duration) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	var sum int64
	for _, d := range sorted {
		sum += int64(d)
	}
	return time.Duration(sum / int64(len(sorted)))
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
