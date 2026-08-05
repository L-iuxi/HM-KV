package watch
//watch树结构定义
import (
	"sync"
)

type TreeNode struct{
	mu            sync.RWMutex                  //锁
	children 	map[byte]*TreeNode//子节点们
	watchers	map[int64]*Watcher//watch们
}