package mvcc

import (
	"TicketX/internal/db"
	types "TicketX/internal/type"
	"encoding/json"
	"fmt"
)

// SnapshotData 快照数据：MVCC 元数据 + 全部 BadgerDB 条目。
type SnapshotData struct {
	CurrentRev int64              `json:"current_rev"`
	CompactRev int64              `json:"compact_rev"`
	Latest     map[string]int64   `json:"latest"`
	History    map[string][]int64 `json:"history"`
	Revisions  []RevisionEntry    `json:"revisions"`
	Entries    []db.ScanResult    `json:"entries"`
}

// Serialize 序列化当前 MVCC 状态（含 BadgerDB 全量数据）。
func (mvcc *MVCC) Serialize() ([]byte, error) {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	entries, err := mvcc.store.ScanAll()
	if err != nil {
		return nil, fmt.Errorf("snapshot scan: %w", err)
	}

	data := SnapshotData{
		CurrentRev: mvcc.currentRev,
		CompactRev: mvcc.compactrev,
		Latest:     mvcc.latest,
		History:    mvcc.history,
		Revisions:  mvcc.revisions,
		Entries:    entries,
	}

	return json.Marshal(data)
}

// Deserialize 从快照数据恢复 MVCC 状态（清空 BadgerDB 后重建）。
func (mvcc *MVCC) Deserialize(data []byte) error {
	var snap SnapshotData
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("snapshot unmarshal: %w", err)
	}

	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	// 清空 BadgerDB
	if err := mvcc.store.DropAll(); err != nil {
		return fmt.Errorf("snapshot dropall: %w", err)
	}

	// 写入快照条目
	for _, entry := range snap.Entries {
		v := types.Value{
			Value:   entry.Value,
			Version: entry.Version,
			Deleted: entry.Deleted,
		}
		_ = mvcc.store.Put(entry.Key, v)
	}

	// 恢复元数据
	mvcc.currentRev = snap.CurrentRev
	mvcc.compactrev = snap.CompactRev
	mvcc.latest = snap.Latest
	mvcc.history = snap.History
	mvcc.revisions = snap.Revisions

	fmt.Printf("snapshot restored: rev=%d entries=%d\n", snap.CurrentRev, len(snap.Entries))
	return nil
}
