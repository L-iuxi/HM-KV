package clerk

import (
	"TicketX/proto"
	"context"
	"time"
)

/*
Watch 返回一个 channel，该 channel 会接收指定 key 的变化事件。
取消 ctx 可以停止监听，并关闭该 channel。
startRev=0 表示："从当前版本开始监听"。
startRev>0 表示："从指定版本开始重新播放事件"
*/
func (c *Client) Watch(ctx context.Context, key string, startRev int64) (<-chan Event, error) {
	return c.watch(ctx, key, false, startRev)
}

/*
WatchPrefix 返回一个 channel，该 channel 会接收指定前缀下所有 key 的变化事件。
取消 ctx 可以停止监听，并关闭该 channel。
startRev=0 表示："从当前版本开始监听"。
startRev>0 表示："从指定版本开始重新播放事件"
*/
func (c *Client) WatchPrefix(ctx context.Context, prefix string, startRev int64) (<-chan Event, error) {
	return c.watch(ctx, prefix, true, startRev)
}

func (c *Client) watch(ctx context.Context, key string, prefix bool, startRev int64) (<-chan Event, error) {
	ch := make(chan Event, 64)
	lastReversion := startRev

	//等待时间
	backoff := time.Second //要等待的时间
	maxBackoff := 30 * time.Second

	go func() {

		defer close(ch)

		for {

			select {
			case <-ctx.Done():
				return
			default:
			}
			req := &proto.WatchRequest{
				Key:            key,
				Prefix:         prefix,
				StartReversion: lastReversion,
			}

			idx := int(c.leaderIdx.Load())
			stream, err := c.kvcs[idx].Watch(ctx, req)
			if err != nil {
				if !waitBack(ctx, backoff) {
					return
				}
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				c.tryNextLeader(idx)
				continue
			}

		readLoop:
			for {
				resp, err := stream.Recv()
				if err != nil {
					if !waitBack(ctx, backoff) {
						return
					}

					backoff *= 2

					if backoff > maxBackoff {
						backoff = maxBackoff
					}
					break // 管道broke，重新连接
				}

				backoff = time.Second

				switch resp.Err {
				case proto.ErrorType_OK:
					//更新最新监听到的版本
					lastReversion = resp.Revision
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
					c.setLeader(resp.LeaderId)

					break readLoop // 重新连接leader

				default:

				}
			}
		}
	}()

	return ch, nil
}

// 在等待时间监听，用户是否还在watch
func waitBack(ctx context.Context, backoff time.Duration) bool {
	timer := time.NewTimer(backoff)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true

	case <-ctx.Done():
		return false
	}
}
