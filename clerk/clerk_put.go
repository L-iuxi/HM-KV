package clerk

import (
	"TicketX/proto"
	"context"
	"fmt"
)

// put单个键值对
func (c *Client) Put(ctx context.Context, key, value string) (int64, error) {
	req := &proto.PutRequest{
		Key:       key,
		Value:     value,
		ClientId:  c.clientID,
		RequestId: c.nextID(),
	}

	for {
		idx := int(c.leaderIdx.Load())
		reply, err := c.kvcs[idx].Put(ctx, req)
		if err != nil {
			c.tryNextLeader(idx)
			continue
		}

		switch reply.Error {
		case proto.ErrorType_OK:
			return reply.Version, nil
		case proto.ErrorType_WRONG_LEADER:
			c.setLeader(reply.LeaderId)
		default:
			return 0, fmt.Errorf("clerk: put %s: unexpected error %v", key, reply.Error)
		}
	}
}

// keyput的时候绑定设置好的lease号
func (c *Client) PutWithLease(ctx context.Context, key, value string, leaseID int64) (int64, error) {
	req := &proto.PutRequest{
		Key:       key,
		Value:     value,
		LeaseId:   leaseID,
		ClientId:  c.clientID,
		RequestId: c.nextID(),
	}

	for {
		idx := int(c.leaderIdx.Load())
		reply, err := c.kvcs[idx].Put(ctx, req)
		if err != nil {
			c.tryNextLeader(idx)
			continue
		}

		switch reply.Error {
		case proto.ErrorType_OK:
			return reply.Version, nil
		case proto.ErrorType_WRONG_LEADER:
			c.setLeader(reply.LeaderId)
		default:
			return 0, fmt.Errorf("clerk: put %s: unexpected error %v", key, reply.Error)
		}
	}
}

// 在预期版本put(应该后面用事务)
func (c *Client) PutWithCAS(ctx context.Context, key, value string, expectedVersion int64) (int64, error) {
	req := &proto.PutRequest{
		Key:             key,
		Value:           value,
		ExpectedVersion: expectedVersion,
		ClientId:        c.clientID,
		RequestId:       c.nextID(),
	}

	for {
		idx := int(c.leaderIdx.Load())
		reply, err := c.kvcs[idx].Put(ctx, req)
		if err != nil {
			c.tryNextLeader(idx)
			continue
		}

		switch reply.Error {
		case proto.ErrorType_OK:
			return reply.Version, nil
		case proto.ErrorType_WRONG_LEADER:
			c.setLeader(reply.LeaderId)
		case proto.ErrorType_WRONG_VERSION:
			return 0, ErrCASConflict
		default:
			return 0, fmt.Errorf("clerk: put %s: unexpected error %v", key, reply.Error)
		}
	}
}
