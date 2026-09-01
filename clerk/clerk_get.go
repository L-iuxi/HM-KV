package clerk

import (
	"TicketX/proto"
	"context"
	"fmt"
)

// 返回建和版本
// 建不存在ErrKeyNotFound.
func (c *Client) Get(ctx context.Context, key string) (string, int64, error) {
	req := &proto.GetRequest{
		Key:       key,
		ClientId:  c.clientID,
		RequestId: c.nextID(),
	}

	for {
		idx := int(c.leaderIdx.Load())
		reply, err := c.kvcs[idx].Get(ctx, req)
		if err != nil {
			c.tryNextLeader(idx)
			continue
		}

		switch reply.Error {
		case proto.ErrorType_OK:
			if len(reply.Kvs) == 0 {
				return "", 0, ErrKeyNotFound
			}
			return reply.Kvs[0].Value, reply.Version, nil
		case proto.ErrorType_WRONG_LEADER:
			c.setLeader(reply.LeaderId)
		case proto.ErrorType_KEY_NOT_EXIST:
			return "", 0, ErrKeyNotFound
		default:
			return "", 0, fmt.Errorf("clerk: get %s: unexpected error %v", key, reply.Error)
		}
	}
}

// 返回前缀匹配的所有键值对
func (c *Client) GetPrefix(ctx context.Context, prefix string) ([]*proto.KeyValue, int64, error) {
	req := &proto.GetRequest{
		Key:       prefix,
		Prefix:    true,
		ClientId:  c.clientID,
		RequestId: c.nextID(),
	}

	for {
		idx := int(c.leaderIdx.Load())
		reply, err := c.kvcs[idx].Get(ctx, req)
		if err != nil {
			c.tryNextLeader(idx)
			continue
		}

		switch reply.Error {
		case proto.ErrorType_OK:
			return reply.Kvs, reply.Version, nil
		case proto.ErrorType_WRONG_LEADER:
			c.setLeader(reply.LeaderId)
		case proto.ErrorType_KEY_NOT_EXIST:
			return nil, 0, ErrKeyNotFound
		default:
			return nil, 0, fmt.Errorf("clerk: prefix get %s: unexpected error %v", prefix, reply.Error)
		}
	}
}
