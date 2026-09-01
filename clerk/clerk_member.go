package clerk

import (
	"TicketX/proto"
	"context"
	"fmt"
)

// AddMember 向集群添加一个成员节点。
// id 为成员编号，address 为新节点的 gRPC 地址（如 "localhost:50056"）。
// 返回 nil 表示添加成功；非 leader 时自动切换到 leader 重试。
func (c *Client) AddMember(ctx context.Context, id int32, address string) error {
	req := &proto.AddMemberRequest{
		Id:      id,
		Address: address,
	}

	for {
		idx := int(c.leaderIdx.Load())
		reply, err := c.kvcs[idx].AddMember(ctx, req)
		if err != nil {
			c.tryNextLeader(idx)
			continue
		}

		switch reply.Error {
		case proto.ErrorType_OK:
			return nil
		case proto.ErrorType_WRONG_LEADER:
			c.setLeader(reply.LeaderId)
		case proto.ErrorType_MEMBER_ALREADY_EXISTS:
			return fmt.Errorf("clerk: add member %s: already exists", address)
		default:
			return fmt.Errorf("clerk: add member %s: %v", address, reply.Error)
		}
	}
}

// DeleteMember 从集群移除一个成员节点，按 address 匹配。
func (c *Client) DeleteMember(ctx context.Context, id int32, address string) error {
	req := &proto.DeleteMemberRequest{
		Id:      id,
		Address: address,
	}

	for {
		idx := int(c.leaderIdx.Load())
		reply, err := c.kvcs[idx].DeleteMember(ctx, req)
		if err != nil {
			c.tryNextLeader(idx)
			continue
		}

		switch reply.Error {
		case proto.ErrorType_OK:
			return nil
		case proto.ErrorType_WRONG_LEADER:
			c.setLeader(reply.LeaderId)
		case proto.ErrorType_MEMBER_NOT_FOUND:
			return fmt.Errorf("clerk: delete member %s: not found", address)
		default:
			return fmt.Errorf("clerk: delete member %s: %v", address, reply.Error)
		}
	}
}
