package mvcc

import "strings"

// PrefixScan 按前缀扫描，返回每个 key 的最新非删除版本
func (mvcc *MVCC) PrefixScan(prefix string) ([]KeyValue, error) {
	results, err := mvcc.store.PrefixScan(prefix)
	if err != nil {
		return nil, err
	}

	type latestEntry struct {
		value   string
		version int64
		deleted bool
	}
	latestMap := make(map[string]latestEntry)

	for _, r := range results {
		lastSlash := strings.LastIndex(r.Key, "/")
		if lastSlash == -1 {
			continue
		}
		originalKey := r.Key[:lastSlash]

		current, exists := latestMap[originalKey]
		if !exists || r.Version > current.version {
			latestMap[originalKey] = latestEntry{
				value:   r.Value,
				version: r.Version,
				deleted: r.Deleted,
			}
		}
	}

	var kvs []KeyValue
	for key, entry := range latestMap {
		if entry.deleted {
			continue
		}
		kvs = append(kvs, KeyValue{Key: key, Value: entry.value, Version: entry.version})
	}

	return kvs, nil
}
