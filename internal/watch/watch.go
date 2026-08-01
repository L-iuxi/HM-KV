package watch

import (
	"fmt"
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
	mu            sync.RWMutex                  //锁
	nextWatcherId int64                         //下一个新监听者的id
	synced        map[string]map[int64]*Watcher //已经同步的watch
	unsynced      map[string]map[int64]*Watcher
	//      map[string]map[int64]*Watcher //某个key下面的监听者们
}

// 新建监听管理者
func NewWatcherManager() *WatcherManager {
	return &WatcherManager{
		nextWatcherId: 1,
		synced:        make(map[string]map[int64]*Watcher),
		unsynced:      make(map[string]map[int64]*Watcher),
	}
}

// 添加到监听表中
func (wm *WatcherManager) AddWatcher(key string, startrev int64, currev int64) *Watcher {

	sync := false
	if startrev >= currev || startrev == 0 {
		sync = true
	}

	//不允许多个groutine同时修改
	wm.mu.Lock()
	defer wm.mu.Unlock()

	next := currev + 1
	if startrev > 0 || startrev <= currev {
		next = currev
	}
	w := &Watcher{
		Key:            key,
		Id:             wm.nextWatcherId,
		StartReversion: startrev,
		NextReversion:  next,
		Ch:             make(chan WatchEvent, 100),
	}

	//下一个id++
	wm.nextWatcherId++
	if sync { //加入已经同步监听列表
		if wm.synced[key] == nil {
			wm.synced[key] = make(map[int64]*Watcher)
		}
		wm.synced[key][w.Id] = w
	} else { //加入未同步监听列表，需要返回历史版本
		if wm.unsynced[key] == nil {
			wm.unsynced[key] = make(map[int64]*Watcher)
		}
		wm.unsynced[key][w.Id] = w
	}

	return w
}

// 通知客户端
func (wm *WatcherManager) Notify(event WatchEvent) {

	//读锁允许并发读
	wm.mu.RLock()
	list := make([]*Watcher, 0)

	//复制一份发送
	for _, w := range wm.synced[event.Key] {
		list = append(list, w)
	}
	wm.mu.RUnlock()

	fmt.Println("notify watcher:", event.Key)

	for _, w := range list {

		//非阻塞发送给所有监听该建的客户端
		select {
		case w.Ch <- event:
			fmt.Println("send event to watcher")
			w.NextReversion = event.Revision + 1
		default:
			fmt.Println("watcher channel full")
		}
	}
}

// 解除监听
func (wm *WatcherManager) RemoveWatch(key string, id int64) {

	wm.mu.Lock()
	defer wm.mu.Unlock()

	if watcher, ok := wm.synced[key]; ok {
		if _, ok := watcher[id]; ok {
			delete(watcher, id)
			return
		}
	}
	if watcher, ok := wm.unsynced[key]; ok {
		if _, ok := watcher[id]; ok {
			delete(watcher, id)
			return
		}
	}
}

func (wm *WatcherManager) MoveToSynced(key string, id int64) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if watchers, ok := wm.unsynced[key]; ok {
		if w, ok := watchers[id]; ok {
			delete(watchers, id)
			if wm.synced[key] == nil {
				wm.synced[key] = make(map[int64]*Watcher)
			}
			wm.synced[key][id] = w
		}
	}
}
