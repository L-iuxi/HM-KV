package clerk

import (
	"TicketX/proto"
	"context"
	"fmt"
)

// 删除小于某版本的历史建
func (c *Client) Compact(ctx context.Context, revision int64) error {
	req := &proto.CompactRequest{
		Revision:  revision,
		ClientId:  c.clientID,
		RequestId: c.nextID(),
	}

	for {
		idx := int(c.leaderIdx.Load())
		reply, err := c.kvcs[idx].Compact(ctx, req)
		if err != nil {
			c.tryNextLeader(idx)
			continue
		}

		switch reply.Error {
		case proto.ErrorType_OK:
			return nil
		case proto.ErrorType_WRONG_LEADER:
			c.setLeader(reply.LeaderId)
		default:
			return fmt.Errorf("clerk: compact: unexpected error %v", reply.Error)
		}
	}
}
