package watch

//新建一个tree
func NewTree() *TreeNode {
    return &TreeNode{
        children: make(map[byte]*TreeNode),
        watchers: make(map[int64]*Watcher),
    }
}

//在前缀处加watch（调用方持有 wm.mu）
func (wm *WatcherManager) Insert (root *TreeNode,prefix string,w *Watcher){

	node := root
	for i:= 0; i < len(prefix);i++{
		ch := prefix[i]
		if node.children[ch] == nil{
			node.children[ch] = NewTree()
		}

		node = node.children[ch]
	}
	//到达prefix处
	node.watchers[w.Id] = w
}

//查找匹配前缀的watcher（调用方持有 wm.mu 读锁）
func (wm *WatcherManager) Match(root *TreeNode,key string,) []*Watcher {

    result := make([]*Watcher,0)
    node := root

    for i := 0; i < len(key); i++ {
        // 当前节点的watcher也匹配
        for _,w := range node.watchers {
            result = append(result,w)
        }

        c := key[i]
        next := node.children[c]

        if next == nil {
            return result // 路径到头，后面没有了
        }

        node = next
    }

    // 最后节点（正常到达 key 末尾）
    for _,w := range node.watchers {
        result = append(result,w)
    }

    return result
}

//解除对应id的watch（调用方持有 wm.mu）
func (wm *WatcherManager) Remove(root *TreeNode,prefix string, id int64) {

   node := root

    // 找到prefix对应节点
    for i := 0; i < len(prefix); i++ {

        c := prefix[i]

        next := node.children[c]

        if next == nil {
            return
        }

        node = next
    }


    // 删除watcher
    delete(node.watchers, id)
}