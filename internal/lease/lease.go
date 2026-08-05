package lease

import "time"

func NewLeaseManager(minLeaseTTL time.Duration) *LeaseManager {
	minTTL := int64(minLeaseTTL.Seconds())
	if minTTL <= 0 {
		minTTL = 1
	}
	return &LeaseManager{
		leases:      make(map[int64]*Lease),
		keyToLease:  make(map[string]int64),
		nextLeaseID: 1,
		minLeaseTTL: minTTL,
	}
}

// 创建新租约
func (lm *LeaseManager) Grant(ttl int64, now int64) int64 {
	lm.Lock()
	defer lm.Unlock()

	if ttl < lm.minLeaseTTL {
		ttl = lm.minLeaseTTL
	}

	id := lm.nextLeaseID
	lm.nextLeaseID++

	lea := &Lease{ID: id,
		TTL:       ttl,
		ExpiresAt: now + ttl,
		Keys:      make(map[string]struct{}),
	}

	lm.leases[id] = lea
	return id

}

// 把建绑定到id上
func (lm *LeaseManager) Attach(key string, leaseID int64) error {
	lm.Lock()
	defer lm.Unlock()

	lease, ok := lm.leases[leaseID]
	if !ok {
		return errLeaseNotFound
	}

	lease.Keys[key] = struct{}{}

	lm.keyToLease[key] = leaseID

	return nil
}

// 寻找过期建
func (lm *LeaseManager) ExpiredLeases(now int64) []*Lease {
	lm.RLock()
	defer lm.RUnlock()

	ret := make([]*Lease, 0)

	for _, lease := range lm.leases {
		if lease.ExpiresAt <= now {
			ret = append(ret, lease)
		}
	}
	return ret
}

// 移除过期建
func (lm *LeaseManager) RemoveLease(id int64) error {
	lm.Lock()
	defer lm.Unlock()
	lease, ok := lm.leases[id]
	if !ok {
		return errLeaseNotFound
	}
	// 删除key反向索引
	for key := range lease.Keys {
		delete(lm.keyToLease, key)
	}
	// 删除lease
	delete(lm.leases, id)

	return nil
}

// 根据 key 查找 leaseID
func (lm *LeaseManager) GetLeaseIDByKey(key string) (int64, error) {
	lm.RLock()
	defer lm.RUnlock()
	id, ok := lm.keyToLease[key]
	if !ok {
		return 0, errLeaseNotFound
	}
	return id, nil
}

// 续约
func (lm *LeaseManager) KeepAliveByKey(key string, now int64) error {
	lm.Lock()
	defer lm.Unlock()
	id, ok := lm.keyToLease[key]
	if !ok {
		return errLeaseNotFound
	}
	l := lm.leases[id]
	l.ExpiresAt = now + l.TTL
	return nil
}
