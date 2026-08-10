package clerk

import (
	"TicketX/proto"
	"context"
	"fmt"
)

// LockResponse Lock 返回信息，需保留用于 Unlock
type LockResponse struct {
	Key      string // 原始资源 key
	LockKey  string // 内部锁 key（/lock/<key>/<client>-<request>）
	Revision int64  // CAS 版本号
}

// Lock 获取分布式锁。
// key: 资源名称；leaseID: 绑定的 lease（通过 Grant 预创建）。
// 返回 LockResponse 需保存，后续 Unlock 使用。
func (c *Client) Lock(ctx context.Context, key string, leaseID int64) (*LockResponse, error) {
	requestID := c.nextID()
	lockKey := fmt.Sprintf("/lock/%s/%d-%d", key, c.clientID, requestID)

	req := &proto.LockRequest{
		Key:       key,
		LeaseId:   leaseID,
		ClientId:  c.clientID,
		RequestId: requestID,
	}

	for {
		idx := int(c.leaderIdx.Load())
		reply, err := c.kvcs[idx].Lock(ctx, req)
		if err != nil {
			c.tryNextLeader(idx)
			continue
		}

		switch reply.Error {
		case proto.ErrorType_OK:
			return &LockResponse{
				Key:      key,
				LockKey:  lockKey,
				Revision: reply.Revision,
			}, nil
		case proto.ErrorType_LOCK_EXIST:
			return &LockResponse{
				Key:     key,
				LockKey: lockKey,
			}, fmt.Errorf("clerk: lock: key %s already locked", key)
		case proto.ErrorType_LEASE_NO_EXIST:
			return nil, fmt.Errorf("clerk: lock: lease %d not found", leaseID)
		case proto.ErrorType_WRONG_LEADER:
			c.knownLeader.Store(reply.LeaderId)
			c.leaderIdx.Store(int32(reply.LeaderId))
		default:
			return nil, fmt.Errorf("clerk: lock: unexpected error %v", reply.Error)
		}
	}
}

// Unlock 释放锁。
// resp 为 Lock 返回的 LockResponse。
func (c *Client) Unlock(ctx context.Context, resp *LockResponse) error {
	req := &proto.UnlockRequest{
		LockKey:   resp.LockKey,
		Version:   resp.Revision,
		ClientId:  c.clientID,
		RequestId: c.nextID(),
	}

	for {
		idx := int(c.leaderIdx.Load())
		reply, err := c.kvcs[idx].Unlock(ctx, req)
		if err != nil {
			c.tryNextLeader(idx)
			continue
		}

		switch reply.Error {
		case proto.ErrorType_OK:
			if !reply.Success {
				return fmt.Errorf("clerk: unlock: lock %s already released or version mismatch", resp.LockKey)
			}
			return nil
		case proto.ErrorType_WRONG_LEADER:
			c.knownLeader.Store(reply.LeaderId)
			c.leaderIdx.Store(int32(reply.LeaderId))
		default:
			return fmt.Errorf("clerk: unlock: unexpected error %v", reply.Error)
		}
	}
}
