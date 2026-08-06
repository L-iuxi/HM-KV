package clerk

import (
	"TicketX/proto"
	"context"
	"fmt"
)

// 删除key，返回key被删除时候的版本号
func (c *Client) Delete(ctx context.Context, key string) (int64, error) {
	req := &proto.DeleteRequest{
		Key:       key,
		ClientId:  c.clientID,
		RequestId: c.nextID(),
	}

	for {
		idx := int(c.leaderIdx.Load())
		reply, err := c.kvcs[idx].Delete(ctx, req)
		if err != nil {
			c.tryNextLeader(idx)
			continue
		}

		switch reply.Error {
		case proto.ErrorType_OK:
			return reply.Version, nil
		case proto.ErrorType_WRONG_LEADER:
			c.knownLeader.Store(reply.LeaderId)
			c.leaderIdx.Store(int32(reply.LeaderId))
		default:
			return 0, fmt.Errorf("clerk: delete %s: unexpected error %v", key, reply.Error)
		}
	}
}
