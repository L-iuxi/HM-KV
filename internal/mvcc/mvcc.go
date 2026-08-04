package mvcc

import (
	"TicketX/internal/db"
	types "TicketX/internal/type"
	"fmt"
)

// New 创建 MVCC 实例
func New(store *db.Store) *MVCC {
	return &MVCC{
		store:     store,
		latest:    make(map[string]int64),
		history:   make(map[string][]int64),
		revisions: make([]RevisionEntry, 0),
		stop:      make(chan struct{}),
	}
}

// Put 写一条新版本。返回分配的 revision。
func (mvcc *MVCC) Put(key, value string) int64 {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	mvcc.currentRev++
	rev := mvcc.currentRev

	v := types.Value{
		Value:   value,
		Version: rev,
	}

	realKey := fmt.Sprintf("%s/%d", key, rev)
	mvcc.store.Put(realKey, v)

	mvcc.latest[key] = rev
	mvcc.history[key] = append(mvcc.history[key], rev)

	re := RevisionEntry{
		Key:      key,
		Revision: rev,
	}

	mvcc.revisions = append(mvcc.revisions, re)

	return rev
}

// Delete 写一条 tombstone。返回分配的 revision。
func (mvcc *MVCC) Delete(key string) int64 {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	mvcc.currentRev++
	rev := mvcc.currentRev

	v := types.Value{
		Value:   "",
		Version: rev,
		Deleted: true,
	}

	realKey := fmt.Sprintf("%s/%d", key, rev)
	mvcc.store.Put(realKey, v)

	mvcc.latest[key] = rev
	mvcc.history[key] = append(mvcc.history[key], rev)
	re := RevisionEntry{
		Key:      key,
		Revision: rev,
	}

	mvcc.revisions = append(mvcc.revisions, re)

	return rev
}

// Get 读 key。version=0 读最新，否则读 <=version 的最大版本。
func (mvcc *MVCC) Get(key string, version int64) (string, int64, error) {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	var rev int64
	var ok bool

	if version == 0 {
		rev, ok = mvcc.latest[key]
		if !ok {
			return "", 0, fmt.Errorf("key not found")
		}
	} else {
		revs, ok := mvcc.history[key]
		if !ok {
			return "", 0, fmt.Errorf("key not found")
		}
		rev = findLE(revs, version)
		if rev == 0 {
			return "", 0, fmt.Errorf("version not found")
		}
	}

	realKey := fmt.Sprintf("%s/%d", key, rev)
	v, err := mvcc.store.Get(realKey)
	if err != nil || v.Deleted {
		return "", 0, fmt.Errorf("key not found")
	}

	return v.Value, v.Version, nil
}

// GetLatest revision of key（不读值，给 prefix scan 和 CAS 用）
func (mvcc *MVCC) GetLatest(key string) (int64, bool) {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()
	rev, ok := mvcc.latest[key]
	return rev, ok
}

// PutWithCAS 带版本检查的写入。expectedVersion=0 表示无条件写。
// 返回新 revision，版本冲突时返回 error。
func (mvcc *MVCC) PutWithCAS(key, value string, expectedVersion int64) (int64, error) {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	if expectedVersion != 0 {
		latestRev, ok := mvcc.latest[key]
		if !ok || latestRev != expectedVersion {
			return 0, fmt.Errorf("cas conflict")
		}
	}

	mvcc.currentRev++
	rev := mvcc.currentRev

	v := types.Value{
		Value:   value,
		Version: rev,
	}

	realKey := fmt.Sprintf("%s/%d", key, rev)
	mvcc.store.Put(realKey, v)

	mvcc.latest[key] = rev
	mvcc.history[key] = append(mvcc.history[key], rev)
	re := RevisionEntry{
		Key:      key,
		Revision: rev,
	}

	mvcc.revisions = append(mvcc.revisions, re)

	return rev, nil
}

// GetHistory 返回 key 的历史 revision 列表（给 Watch 回放用）
func (mvcc *MVCC) GetHistory(key string) []int64 {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()
	return mvcc.history[key]
}

// CurrentRev 返回当前全局 revision
func (mvcc *MVCC) CurrentRev() int64 {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()
	return mvcc.currentRev
}

// findLE 二分查找 ≤ target 的最大 revision。revs 必须升序。
func findLE(revs []int64, target int64) int64 {
	var res int64 = 0
	for _, r := range revs {
		if r <= target {
			res = r
		} else {
			break
		}
	}
	return res
}
