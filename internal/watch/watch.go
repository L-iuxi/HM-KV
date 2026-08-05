package watch


// 新建监听管理者
func NewWatcherManager() *WatcherManager {
	return &WatcherManager{
		nextWatcherId: 1,
		synced:        make(map[string]map[int64]*Watcher),
		unsynced:      make(map[string]map[int64]*Watcher),
		syncedroot:    NewTree(),
		unsyncedroot:  NewTree(),
	}
}

// 注册监听，按情况分发
func (wm *WatcherManager) Register(key string, prefix bool, startrev int64, currev int64) *Watcher {
	if prefix {
		isSync := startrev >= currev || startrev == 0

		next := currev + 1
		if !isSync {
			next = currev
		}

		wm.mu.Lock()
		w := &Watcher{
			Key:            key,
			Id:             wm.nextWatcherId,
			StartReversion: startrev,
			NextReversion:  next,
			Ch:             make(chan WatchEvent, 100),
		}
		wm.nextWatcherId++

		if isSync {
			wm.Insert(wm.syncedroot, key, w)
		} else {
			wm.Insert(wm.unsyncedroot, key, w)
		}
		wm.mu.Unlock()

		return w
	}
	return wm.AddWatcher(key, startrev, currev)
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
	if !sync {
		next = currev // unsynced: needs catch-up to current
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

	//精确匹配
	for _, w := range wm.synced[event.Key] {
		list = append(list, w)
	}
	//前缀匹配（synced + unsynced）
	list = append(list, wm.Match(wm.syncedroot, event.Key)...)
	list = append(list, wm.Match(wm.unsyncedroot, event.Key)...)
	wm.mu.RUnlock()

	for _, w := range list {

		//非阻塞发送给所有监听该建的客户端
		select {
		case w.Ch <- event:
			w.NextReversion = event.Revision + 1
		default:
			// 客户端消费太慢，解除注册防止泄漏和 panic
			wm.RemoveWatch(w.Key, w.Id)
		}
	}
}

// 解除监听
func (wm *WatcherManager) RemoveWatch(key string, id int64) {

	wm.mu.Lock()
	defer wm.mu.Unlock()

	if watcher, ok := wm.synced[key]; ok {
		if w, ok := watcher[id]; ok {
			close(w.Ch)
			delete(watcher, id)
			return
		}
	}
	if watcher, ok := wm.unsynced[key]; ok {
		if w, ok := watcher[id]; ok {
			close(w.Ch)
			delete(watcher, id)
			return
		}
	}
	// 前缀 trie 里也可能有
	wm.Remove(wm.syncedroot, key, id)
	wm.Remove(wm.unsyncedroot, key, id)
}

// 同步
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

// 删除后同步
func (wm *WatcherManager) Compact(revsion int64) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	wm.compactversion = revsion
}

func (wm *WatcherManager) GetCompactRevision() int64 {
	return wm.compactversion
}
