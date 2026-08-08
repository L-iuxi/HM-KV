package clerk

import (
	"TicketX/proto"
	"context"
	"fmt"
)

// TxnResponse Txn 请求的返回结果
type TxnResponse struct {
	Succeeded bool
	Results   []*proto.KeyValue
}

// Txn 事务：如果所有 compare 条件满足，执行 success 分支；否则执行 failure 分支。
// 所有操作在一个 Raft entry 内原子完成。
func (c *Client) Txn(ctx context.Context, compares []*proto.Compare, success, failure []*proto.Entry) (*TxnResponse, error) {
	req := &proto.TxnRequest{
		Compare:   compares,
		Success:   success,
		Failed:    failure,
		ClientId:  c.clientID,
		RequestId: c.nextID(),
	}

	for {
		idx := int(c.leaderIdx.Load())
		reply, err := c.kvcs[idx].Txn(ctx, req)
		if err != nil {
			c.tryNextLeader(idx)
			continue
		}

		switch reply.Error {
		case proto.ErrorType_OK:
			return &TxnResponse{
				Succeeded: reply.Succeeded,
				Results:   reply.Results,
			}, nil
		case proto.ErrorType_WRONG_LEADER:
			c.knownLeader.Store(reply.LeaderId)
			c.leaderIdx.Store(int32(reply.LeaderId))
		default:
			return nil, fmt.Errorf("clerk: txn: unexpected error %v", reply.Error)
		}
	}
}
