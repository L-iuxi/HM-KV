package kv

import (
	"TicketX/internal/watch"
	"TicketX/proto"
)

// 监控某个建
func (kv *KvServer) Watch(req *proto.WatchRequest, stream proto.Kv_WatchServer) error {

	if req.StartReversion > 0 && req.StartReversion <= kv.watcherManager.GetCompactRevision() {
		err := stream.Send(
			&proto.WatchResponse{
				Err: proto.ErrorType_COMPACT_ERROR,
			},
		)
		return err

	}

	curRev := kv.mvcc.CurrentRev()
	watcher := kv.watcherManager.Register(req.Key, req.Prefix, req.StartReversion, curRev)
	defer kv.watcherManager.RemoveWatch(req.Key, watcher.Id)

	if watcher.StartReversion > 0 && watcher.StartReversion < curRev { //需要历史数据
		if !req.Prefix {
			kv.Synced(watcher, req.Key)
		}
		// 前缀 watch 历史回放留待后续：需 PrefixScan 所有匹配 key 再做全量 replay
	}

	for ev := range watcher.Ch {

		err := stream.Send(
			&proto.WatchResponse{
				Key:      ev.Key,
				Value:    ev.Value,
				Revision: ev.Revision,
				Type:     ev.Type,
				Err:      proto.ErrorType_OK,
			},
		)

		if err != nil {


			return err
		}
	}
	return nil
}

func (kv *KvServer) Synced(watcher *watch.Watcher, key string) {
	revs := kv.mvcc.GetHistory(key)

	for _, rev := range revs {
		if rev > watcher.StartReversion {
			value, _, err := kv.mvcc.Get(key, rev)
			if err != nil {
				continue
			}
			watcher.Ch <- watch.WatchEvent{
				Key:      key,
				Value:    value,
				Revision: rev,
				Type:     "Put",
			}
		}
	}
	kv.watcherManager.MoveToSynced(key, watcher.Id)
}
