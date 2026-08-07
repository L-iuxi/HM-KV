// Package clerk provides a client SDK for the HMETCD distributed KV store.
package clerk

import (
	"errors"
	"fmt"
	"math/rand"

	"TicketX/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func New(addrs []string) (*Client, error) {
	if len(addrs) == 0 {
		return nil, errors.New("clerk: at least one address required")
	}

	c := &Client{
		addrs:    addrs,
		conns:    make([]*grpc.ClientConn, len(addrs)),
		kvcs:     make([]proto.KvClient, len(addrs)),
		clientID: rand.Int63(),
	}

	for i, addr := range addrs {
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("clerk: dial %s: %w", addr, err)
		}
		c.conns[i] = conn
		c.kvcs[i] = proto.NewKvClient(conn)
	}

	c.leaderIdx.Store(0)
	c.knownLeader.Store(-1)
	c.requestID.Store(1)
	return c, nil
}

func (c *Client) Close() error {
	var errs []error
	for _, conn := range c.conns {
		if conn != nil {
			if err := conn.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("clerk: close errors: %v", errs)
	}
	return nil
}

// 下一个requestid
func (c *Client) nextID() int64 {
	return c.requestID.Add(1)
}

// 找新leader：优先用 knownLeader 提示，否则顺序探测
func (c *Client) tryNextLeader(current int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if int(c.leaderIdx.Load()) != current {
		return // 已被其他请求切换
	}

	// 优先尝试已知 leader
	known := c.knownLeader.Load()
	if known >= 0 && int(known) != current && int(known) < len(c.addrs) {
		c.leaderIdx.Store(int32(known))
		return
	}

	// 已知 leader 不可用或就是当前节点，顺序探测下一个
	next := (current + 1) % len(c.addrs)
	c.leaderIdx.Store(int32(next))
}
