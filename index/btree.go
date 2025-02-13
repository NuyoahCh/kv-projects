// Package index
// @Author NuyoahCh
// @Date 2025/2/12 23:16
// @Desc Google 自带 btree 库的封装和实现
package index

import (
	"github.com/google/btree"
	"kv-projects/data"
	"sync"
)

// BTree 索引，主要封装了 google 的 btree ku
// https://github.com/google/btree
type BTree struct {
	tree *btree.BTree  // 实现库中的结构
	lock *sync.RWMutex // 由于 Btree 库原生不支持安全并发，所以进行加锁
}

// NewBTree 新建 BTree 索引结构
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),     // 树的结点创建是要程序员自行创建
		lock: new(sync.RWMutex), // 创建读写锁
	}
}

// Put 向索引中存储 key 对应的数据位置信息
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

// Get 根据 key 取出对应的索引位置信息
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

// Delete 根据 key 删除对应的索引位置信息
func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}
