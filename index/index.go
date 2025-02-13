// Package index
// @Author NuyoahCh
// @Date 2025/2/12 23:13
// @Desc 内存设计的接口和结构体实现
package index

import (
	"bytes"
	"kv-projects/data"

	"github.com/google/btree"
)

// Indexer 抽象索引接口，后续如果想要接入其他的数据结构，则直接实现这个接口即可
type Indexer interface {

	// Put 向索引中存储 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 根据 key 取出对应的索引位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 根据 key 删除对应的索引位置信息
	Delete(key []byte) bool
}

// Item 由于 Btree 中自带的 Item 接口不符合预期，则自己创建
type Item struct {
	key []byte             // 键值
	pos *data.LogRecordPos // 位置信息
}

// Less 前者小于后者的判断，The result will be 0 if a == b, -1 if a < b, and +1 if a > b.
func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
