package clerk

import (
	"TicketX/proto"
	"context"
	"fmt"
)

// 批量提交请求
func (c *Client) Batch(ctx context.Context, entries []*proto.Entry) error {
	req := &proto.BatchRequest{
		Entries:   entries,
		ClientId:  c.clientID,
		RequestId: c.nextID(),
	}

	for {
		idx := int(c.leaderIdx.Load())
		reply, err := c.kvcs[idx].Batch(ctx, req)
		if err != nil {
			c.tryNextLeader(idx)
			continue
		}

		switch reply.Error {
		case proto.ErrorType_OK:
			return nil
		case proto.ErrorType_WRONG_LEADER:
			c.knownLeader.Store(reply.LeaderId)
			c.leaderIdx.Store(int32(reply.LeaderId))
		default:
			return fmt.Errorf("clerk: batch: unexpected error %v", reply.Error)
		}
	}
}
