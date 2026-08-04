package mvcc

import (
	"fmt"
	"sort"
	"strings"
)

// 从BadgerDB扫描重建history，currrev，laster
/*
map[string][]int64 一个key(string)对应一个int数组
第一次扫数据库获取每一个建的全部历史版本，恢复history
第二次扫map，找到每个建的最新版本，恢复laster，找到最新rev，恢复currentRev
*/
func (mvcc *MVCC) Recover() error {
	result, err := mvcc.store.ScanAll()
	if err != nil {
		return err
	}

	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	keyReversion := make(map[string][]int64)

	//找一个建的所有历史版本
	for _, r := range result {
		lastSlash := strings.LastIndex(r.Key, "/")
		if lastSlash == -1 {
			continue
		}

		originalKey := r.Key[:lastSlash]
		keyReversion[originalKey] = append(keyReversion[originalKey], r.Version)

		//更新全局version
		if r.Version > mvcc.currentRev {
			mvcc.currentRev = r.Version
		}
	}

	//对于每个建，确定最新版本
	for key, revs := range keyReversion {
		sort.Slice(revs, func(i int, j int) bool {
			return revs[i] < revs[j]
		})
		mvcc.history[key] = revs

		for i := len(revs) - 1; i >= 0; i-- {
			realKey := fmt.Sprintf("%s/%d", key, revs[i])
			v, err := mvcc.store.Get(realKey) //从数据库中查最新版本
			if err != nil {
				continue
			}
			if !v.Deleted {
				mvcc.latest[key] = revs[i]
				break
			}
		}
	}

	return nil
}
