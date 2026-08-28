package main

import (
	"TicketX/clerk"
	"TicketX/internal/config"
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

var helpText = `Commands:
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
  member add <id> <address>     — add cluster member
  member del <id> <address>     — remove cluster member
  help                          — show this
  exit                          — quit
`

var fallbackPeers = []string{
	"localhost:50051", "localhost:50052", "localhost:50053",
	"localhost:50054", "localhost:50055",
}

// getPeers 从配置文件读取 raft.peers，文件不存在时用 fallback。
func getPeers(configPath string) []string {
	cfg, err := config.Load(configPath)
	if err != nil || len(cfg.Peers()) == 0 {
		return fallbackPeers
	}
	return cfg.Peers()
}

func main() {
	configPath := flag.String("config", "configs/node0.yaml", "配置文件路径（读取 raft.peers）")
	flag.Parse()

	peers := getPeers(*configPath)
	args := flag.Args() // 非 flag 参数

	if len(args) > 0 {
		// 一次性命令模式：hmctl [--config path] put key val
		runCommand(peers, args[0], args[1:])
		return
	}

	// 交互模式
	client, err := clerk.New(peers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()
	fmt.Println("connected. type 'help' for commands.")

	ctx := context.Background()
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("hm> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		dispatch(ctx, client, line)
	}
}

func dispatch(ctx context.Context, c *clerk.Client, line string) {
	parts := strings.Fields(line)
	cmd := parts[0]

	switch cmd {
	case "put":
		if len(parts) < 3 {
			fmt.Println("usage: put <key> <value> [ttl]")
			return
		}
		key, val := parts[1], parts[2]
		var rev int64
		var err error
		if len(parts) >= 4 {
			ttl, _ := strconv.ParseInt(parts[3], 10, 64)
			leaseID, err := c.Grant(ctx, ttl)
			if err != nil {
				fmt.Println("error:", err)
				return
			}
			rev, err = c.PutWithLease(ctx, key, val, leaseID)
		} else {
			rev, err = c.Put(ctx, key, val)
		}
		if err != nil {
			fmt.Println("error:", err)
		} else {
			fmt.Printf("OK rev=%d\n", rev)
		}

	case "get":
		if len(parts) < 2 {
			fmt.Println("usage: get <key>")
			return
		}
		val, rev, err := c.Get(ctx, parts[1])
		if err != nil {
			fmt.Println("error:", err)
		} else {
			fmt.Printf("%s = %s  rev=%d\n", parts[1], val, rev)
		}

	case "delete":
		if len(parts) < 2 {
			fmt.Println("usage: delete <key>")
			return
		}
		rev, err := c.Delete(ctx, parts[1])
		if err != nil {
			fmt.Println("error:", err)
		} else {
			fmt.Printf("OK rev=%d\n", rev)
		}

	case "prefix":
		if len(parts) < 2 {
			fmt.Println("usage: prefix <prefix>")
			return
		}
		kvs, _, err := c.GetPrefix(ctx, parts[1])
		if err != nil {
			fmt.Println("error:", err)
		} else if len(kvs) == 0 {
			fmt.Println("(empty)")
		} else {
			for _, kv := range kvs {
				fmt.Printf("%s = %s\n", kv.Key, kv.Value)
			}
		}

	case "watch":
		if len(parts) < 2 {
			fmt.Println("usage: watch <key>")
			return
		}
		key := parts[1]
		ch, err := c.Watch(ctx, key, 0)
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		go func() {
			for ev := range ch {
				fmt.Printf("\n[%s] %s %s=%s rev=%d\nhm> ", key, ev.Type, ev.Key, ev.Value, ev.Revision)
			}
		}()
		fmt.Printf("watching %s...\n", key)

	case "watchprefix":
		if len(parts) < 2 {
			fmt.Println("usage: watchprefix <prefix>")
			return
		}
		prefix := parts[1]
		ch, err := c.WatchPrefix(ctx, prefix, 0)
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		go func() {
			for ev := range ch {
				fmt.Printf("\n[%s*] %s %s=%s rev=%d\nhm> ", prefix, ev.Type, ev.Key, ev.Value, ev.Revision)
			}
		}()
		fmt.Printf("watching prefix %s...\n", prefix)

	case "grant":
		if len(parts) < 2 {
			fmt.Println("usage: grant <ttl>")
			return
		}
		ttl, _ := strconv.ParseInt(parts[1], 10, 64)
		leaseID, err := c.Grant(ctx, ttl)
		if err != nil {
			fmt.Println("error:", err)
		} else {
			fmt.Printf("lease %d granted (TTL=%ds)\n", leaseID, ttl)
		}

	case "putlease":
		if len(parts) < 4 {
			fmt.Println("usage: putlease <key> <value> <leaseID>")
			return
		}
		leaseID, _ := strconv.ParseInt(parts[3], 10, 64)
		rev, err := c.PutWithLease(ctx, parts[1], parts[2], leaseID)
		if err != nil {
			fmt.Println("error:", err)
		} else {
			fmt.Printf("OK rev=%d (lease=%d)\n", rev, leaseID)
		}

	case "keepalive":
		if len(parts) < 2 {
			fmt.Println("usage: keepalive <key>")
			return
		}
		if err := c.KeepAlive(ctx, parts[1]); err != nil {
			fmt.Println("error:", err)
		} else {
			fmt.Println("OK")
		}

	case "compact":
		if len(parts) < 2 {
			fmt.Println("usage: compact <revision>")
			return
		}
		rev, _ := strconv.ParseInt(parts[1], 10, 64)
		if err := c.Compact(ctx, rev); err != nil {
			fmt.Println("error:", err)
		} else {
			fmt.Println("OK")
		}

	case "member":
		if len(parts) < 2 {
			fmt.Println("usage: member add <id> <address> | member del <id> <address>")
			return
		}
		if len(parts) < 4 {
			fmt.Println("usage: member", parts[1], "<id> <address>")
			return
		}
		id, err := strconv.ParseInt(parts[2], 10, 32)
		if err != nil {
			fmt.Println("error: id must be int:", parts[2])
			return
		}
		addr := parts[3]
		switch parts[1] {
		case "add":
			if err := c.AddMember(ctx, int32(id), addr); err != nil {
				fmt.Println("error:", err)
			} else {
				fmt.Printf("OK member %s added\n", addr)
			}
		case "del":
			if err := c.DeleteMember(ctx, int32(id), addr); err != nil {
				fmt.Println("error:", err)
			} else {
				fmt.Printf("OK member %s removed\n", addr)
			}
		default:
			fmt.Println("usage: member add <id> <address> | member del <id> <address>")
		}

	case "help":
		fmt.Print(helpText)

	case "exit":
		os.Exit(0)

	default:
		fmt.Printf("unknown: %s\n", cmd)
	}
}

func runCommand(peers []string, cmd string, args []string) {
	client, err := clerk.New(peers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	dispatch(context.Background(), client, cmd+" "+strings.Join(args, " "))
}
