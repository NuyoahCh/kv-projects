// Package index
// @Author NuyoahCh
// @Date 2025/2/12 23:13
// @Desc 内存设计测试类
package index

import (
	"github.com/stretchr/testify/assert"
	"kv-projects/data"
	"testing"
)

// TestBTree_Put 测试数据的存放
func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
	// === RUN   TestBTree_Put
	// --- PASS: TestBTree_Put (0.00s)
	// PASS
}

// TestBTree_Get 测试数据获取
func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.True(t, res3)

	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)
	// === RUN   TestBTree_Get
	// --- PASS: TestBTree_Get (0.00s)
	// PASS
}

// TestBTree_Delete 测试数据删除
func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)
	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 22, Offset: 33})
	assert.True(t, res3)
	res4 := bt.Delete([]byte("aaa"))
	assert.True(t, res4)
	// === RUN   TestBTree_Delete
	// --- PASS: TestBTree_Delete (0.00s)
	// PASS
}

/*
测试完毕
=== RUN   TestBTree_Put
--- PASS: TestBTree_Put (0.00s)
=== RUN   TestBTree_Get
--- PASS: TestBTree_Get (0.00s)
=== RUN   TestBTree_Delete
--- PASS: TestBTree_Delete (0.00s)
PASS
ok      kv-projects/index       0.456s
*/
