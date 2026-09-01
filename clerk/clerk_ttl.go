package clerk

import (
	"TicketX/proto"
	"context"
	"fmt"
)

// 创建一个TTL并返回他的ID
func (c *Client) Grant(ctx context.Context, ttl int64) (int64, error) {
	req := &proto.GrantRequest{Ttl: ttl}

	for {
		idx := int(c.leaderIdx.Load())
		reply, err := c.kvcs[idx].Grant(ctx, req)
		if err != nil {
			c.tryNextLeader(idx)
			continue
		}

		switch reply.Error {
		case proto.ErrorType_OK:
			return reply.LeaseId, nil
		case proto.ErrorType_WRONG_LEADER:
			c.setLeader(reply.LeaderId)
		default:
			return 0, fmt.Errorf("clerk: grant: unexpected error %v", reply.Error)
		}
	}
}

// KeepAlive 续约 key 对应的 lease
func (c *Client) KeepAlive(ctx context.Context, key string) error {
	req := &proto.KeepAliveRequest{Key: key}

	for {
		idx := int(c.leaderIdx.Load())
		reply, err := c.kvcs[idx].KeepAlive(ctx, req)
		if err != nil {
			c.tryNextLeader(idx)
			continue
		}

		switch reply.Error {
		case proto.ErrorType_OK:
			return nil
		case proto.ErrorType_WRONG_LEADER:
			c.setLeader(reply.LeaderId)
		case proto.ErrorType_KEY_NOT_EXIST:
			return ErrKeyNotFound
		default:
			return fmt.Errorf("clerk: keepalive %s: unexpected error %v", key, reply.Error)
		}
	}
}
