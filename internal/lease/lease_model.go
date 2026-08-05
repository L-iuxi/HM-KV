package lease

import (
	"errors"
	"sync"
)

var errLeaseNotFound = errors.New("lease not found")

type Lease struct {
	ID        int64               // 每个lease对象的编号
	TTL       int64               // 租约时间
	ExpiresAt int64               // 开始时间
	Keys      map[string]struct{} // 所有建

}

type LeaseManager struct {
	sync.RWMutex
	leases      map[int64]*Lease // 保存所有lease对象
	keyToLease  map[string]int64 // key对应的leaseid
	nextLeaseID int64            //下一个id
	minLeaseTTL int64            //最小可设置时间
}

type Engine interface {
	Grant(ttl int64, now int64) int64
	Attach(key string, leaseID int64) error
	KeepAliveByKey(key string, now int64) error
	GetLeaseIDByKey(key string) (int64, error)
	ExpiredLeases(now int64) []*Lease
	RemoveLease(id int64) error
}
