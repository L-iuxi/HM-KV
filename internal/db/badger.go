package db

import (
	types "TicketX/internal/type"
	"encoding/json"

	"github.com/dgraph-io/badger/v4"
)

func NewStore(path string) (*Store, error) {
	opts := badger.DefaultOptions(path)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &Store{
		DB: db,
	}, nil
}

func (s *Store) Put(key string, value types.Value) error {
	return s.DB.Update(func(txn *badger.Txn) error {

		data, err := json.Marshal(value)
		if err != nil {
			return err
		}

		return txn.Set([]byte(key), data)
	})
}

func (s *Store) Get(key string) (types.Value, error) {

	var val types.Value

	err := s.DB.View(func(txn *badger.Txn) error {

		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		data, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		return json.Unmarshal(data, &val)
	})

	return val, err
}

func (s *Store) Delete(key string) error {
	return s.DB.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

// RawPut 写入原始字节。供查重表等非 MVCC 结构（非 types.Value）持久化用。
func (s *Store) RawPut(key string, data []byte) error {
	return s.DB.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
}

// RawGet 读取原始字节。key 不存在时返回 badger.ErrKeyNotFound。
func (s *Store) RawGet(key string) ([]byte, error) {
	var data []byte
	err := s.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		data, err = item.ValueCopy(nil)
		return err
	})
	return data, err
}

// DropAll 清空所有数据，用于快照恢复时重建状态。
func (s *Store) DropAll() error {
	return s.DB.DropAll()
}

func (s *Store) Close() error {
	return s.DB.Close()
}

func (s *Store) PrefixScan(prefix string) ([]ScanResult, error) {

	result := make([]ScanResult, 0)

	err := s.DB.View(func(txn *badger.Txn) error {

		opts := badger.DefaultIteratorOptions

		opts.Prefix = []byte(prefix)

		iter := txn.NewIterator(opts)

		defer iter.Close()

		for iter.Seek([]byte(prefix)); iter.Valid(); iter.Next() {

			item := iter.Item()

			value, err := item.ValueCopy(nil)

			if err != nil {
				return err
			}

			var v types.Value
			if err := json.Unmarshal(value, &v); err != nil {
				continue
			}
			result = append(result, ScanResult{
				Key:     string(item.Key()),
				Value:   v.Value,
				Version: v.Version,
				Deleted: v.Deleted,
			})
		}
		return nil
	})

	return result, err
}

func (s *Store) ScanAll() ([]ScanResult, error) {
	result := make([]ScanResult, 0)
	err := s.DB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		iter := txn.NewIterator(opts)

		defer iter.Close()
		for iter.Rewind(); iter.Valid(); iter.Next() {
			item := iter.Item()
			value, err := item.ValueCopy(nil)

			if err != nil {
				return err
			}
			var v types.Value
			if err := json.Unmarshal(value, &v); err != nil {
				continue
			}

			result = append(result, ScanResult{
				Key:     string(item.Key()),
				Value:   v.Value,
				Version: v.Version,
				Deleted: v.Deleted,
			})
		}
		return nil
	})
	return result, err
}

// 批量删除建
func (s *Store) BatchDelete(keys []string) error {

	return s.DB.Update(func(txn *badger.Txn) error {

		for _, key := range keys {

			err := txn.Delete([]byte(key))

			if err != nil {
				return err
			}

		}

		return nil
	})
}
