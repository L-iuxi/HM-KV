package lock

import (
	"TicketX/internal/lease"
	"TicketX/internal/mvcc"
	"TicketX/internal/txn"
	"TicketX/internal/watch"
	"TicketX/proto"
	"context"
	"fmt"
)

func NewLockManager(leaseMgr *lease.LeaseManager, mvcc *mvcc.MVCC, watcher *watch.WatcherManager) *LockManager {

	return &LockManager{
		leaseMgr: leaseMgr,
		mvcc:     mvcc,
		watcher:  watcher,
	}
}

// 依据clientid，requestid唯一标识号创建一个对于key的锁
func (lock *LockManager) GenerateLockKey(key string, clientID int64, requestID int64) string {
	return fmt.Sprintf(
		"/lock/%s/%d-%d",
		key,
		clientID,
		requestID,
	)
}

// 检查key是否存在，不存在写入
func (lm *LockManager) CreateLockTxn(key string, leaseID int64) *txn.Txn {

	t := txn.New()

	t.If(
		&proto.Compare{
			Key:         key,
			Version:     0,
			CompareType: proto.CompareType_EQUAL,
		},
	).Then(
		&proto.Entry{
			Type:    "Put",
			Key:     key,
			Value:   fmt.Sprintf("%d", leaseID),
			LeaseId: leaseID,
		},
	)

	return t
}

func (lm *LockManager) FindPredecessor(key string, myRevision int64) string {

	prefix := fmt.Sprintf("/lock/%s/", key)

	entries, _ := lm.mvcc.PrefixScan(prefix)

	var predecessor string
	var maxRevision int64 = 0

	for _, entry := range entries {

		// 自己跳过
		if entry.Version == myRevision {
			continue
		}

		// 找前驱
		if entry.Version < myRevision &&
			entry.Version > maxRevision {

			maxRevision = entry.Version
			predecessor = entry.Key
		}
	}

	return predecessor
}

// WaitLock 监听前驱 key 直到被删除
func (lm *LockManager) WaitLock(ctx context.Context, target string) error {
	w := lm.watcher.Register(target, false, 0, lm.mvcc.CurrentRev())
	defer lm.watcher.RemoveWatch(target, w.Id)

	// double check：注册 watch 后再次检查 key 是否已被删除
	if _, ok := lm.mvcc.GetLatest(target); !ok {
		return nil
	}

	for {
		select {
		case event := <-w.Ch:
			if event.Key == target && event.Type == "Delete" {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (lm *LockManager) WaitUntilAcquire(ctx context.Context, key string, myRevision int64) error {

	for {
		predecessor := lm.FindPredecessor(key, myRevision)

		// 自己已经是最小revision
		if predecessor == "" {
			return nil
		}

		// 等待前驱删除
		err := lm.WaitLock(ctx, predecessor)

		if err != nil {
			return err
		}
		// 删除后重新循环检查
	}

}
