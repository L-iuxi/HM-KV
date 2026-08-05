package mvcc

import (
	"fmt"
	"time"
)

func (mvcc *MVCC) StartCompact(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				mvcc.Compact(mvcc.CurrentRev())
			case <-mvcc.stop:
				return
			}
		}
	}()
}

/*
通过 revision index 找出 compactRevision 之前发生过修改的 key，然后针对这些 key，
在 history 中找到每个 key 需要删除的旧版本，最后批量删除 Badger 中对应的物理 key，并更新 history。
*/
// 删除version以前的版本
func (mvcc *MVCC) Compact(compactRevision int64) error {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	index := 0

	for index < len(mvcc.revisions) && mvcc.revisions[index].Revision <= compactRevision {
		index++
	}
	compactEntries := mvcc.revisions[:index]

	keys := make(map[string]struct{})

	//提取所有涉及的key并去重
	for _, entry := range compactEntries {
		keys[entry.Key] = struct{}{}
	}

	deleteKeys := make([]string, 0)

	//找所有要删除的建
	for key := range keys {
		err := mvcc.compactKey(
			key,
			compactRevision,
			&deleteKeys,
		)
		if err != nil {
			return err
		}
	}

	//批量删除
	if len(deleteKeys) > 0 {
		err := mvcc.store.BatchDelete(deleteKeys)
		if err != nil {
			return err
		}

	}
	mvcc.revisions = mvcc.revisions[index:]
	mvcc.compactrev = compactRevision

	return nil
}

// 收集
func (mvcc *MVCC) compactKey(key string, compactRevision int64, deleteKeys *[]string) error {
	versions := mvcc.history[key]

	if len(versions) == 0 {
		return nil
	}

	keep := int64(-1)

	for _, v := range versions {
		if v <= compactRevision {
			keep = v
		} else {
			break
		}
	}

	if keep == -1 {
		return nil
	}

	newHistory := make([]int64, 0, len(versions))

	for _, v := range versions {
		if v < keep {
			realKey := fmt.Sprintf("%s/%d", key, v)
			*deleteKeys = append(*deleteKeys, realKey)
			continue
		}

		newHistory = append(newHistory, v)
	}

	mvcc.history[key] = newHistory

	return nil
}
