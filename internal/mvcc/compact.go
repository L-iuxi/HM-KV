package mvcc

import "time"

func (mvcc *MVCC) StartCompact(interval time.Duration) {

	go func() {

		ticker := time.NewTicker(interval)

		defer ticker.Stop()

		for range ticker.C {

			mvcc.Compact()
		}

	}()
}

func (mvcc *MVCC) Compact() {
	return
}
