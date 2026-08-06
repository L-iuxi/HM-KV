package clerk

import (
	"TicketX/proto"
	"context"
)

/*
Watch 返回一个 channel，该 channel 会接收指定 key 的变化事件。
取消 ctx 可以停止监听，并关闭该 channel。
startRev=0 表示："从当前版本开始监听"。
startRev>0 表示："从指定版本开始重新播放事件"
*/
func (c *Client) Watch(ctx context.Context, key string, startRev int64) (<-chan Event, error) {
	ch := make(chan Event, 64)

	go func() {
		defer close(ch)

		req := &proto.WatchRequest{
			Key:            key,
			StartReversion: startRev,
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			idx := int(c.leaderIdx.Load())
			stream, err := c.kvcs[idx].Watch(ctx, req)
			if err != nil {
				c.tryNextLeader(idx)
				continue
			}

		readLoop:
			for {
				resp, err := stream.Recv()
				if err != nil {
					break // 管道broke，重新连接
				}

				switch resp.Err {
				case proto.ErrorType_OK:
					select {
					case ch <- Event{
						Type:     resp.Type,
						Key:      resp.Key,
						Value:    resp.Value,
						Revision: resp.Revision,
					}:
					case <-ctx.Done():
						return
					}

				case proto.ErrorType_WRONG_LEADER:
					c.knownLeader.Store(resp.LeaderId)
					c.leaderIdx.Store(int32(resp.LeaderId))
					break readLoop // 重新连接leader

				default:

				}
			}
		}
	}()

	return ch, nil
}
