package watch

//watch结构定义
import (
	"sync"
)

// 一次变化事件
type WatchEvent struct {
	Key      string //建
	Value    string //值
	Type     string //修改类型
	Revision int64  //版本号
}

// 一个监听者，监听一个key
type Watcher struct {
	Key            string          //被监听的建
	Id             int64           //监听者id
	StartReversion int64           //客户端想从哪个版本开始看
	NextReversion  int64           //watch监听期待的下一个版本
	Ch             chan WatchEvent //通知管道
}

// 管理所有监听者
type WatcherManager struct {
	mu             sync.RWMutex                  //锁
	nextWatcherId  int64                         //下一个新监听者的id
	compactversion int64                         //最早可以追溯到的版本
	synced         map[string]map[int64]*Watcher //已经同步的watch
	unsynced       map[string]map[int64]*Watcher
	syncedroot     *TreeNode //同步前缀数
	unsyncedroot   *TreeNode //不同步前缀数
}

// WatchEngine KV 层调用的 Watcher 接口
type WatchEngine interface {
	Register(key string, prefix bool, startrev int64, currev int64) *Watcher
	Notify(event WatchEvent)
	RemoveWatch(key string, id int64)
	MoveToSynced(key string, id int64)
	Compact(revsion int64)
	GetCompactRevision() int64
}
