// Package fio
// @Author NuyoahCh
// @Date 2025/2/12 23:13
// @Desc IO 操作测试方法
package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

// destroyFile 自动销毁文件，防止影响下一次的操作
func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

// TestNewFileIOManager 创建新的文件
func TestNewFileIOManager(t *testing.T) {
	path := filepath.Join("/Users/spring/workspace/assault/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)
	// === RUN   TestNewFileIOManager
	// --- PASS: TestNewFileIOManager (0.00s)
	// PASS
}

// TestFileIO_Write 文件的写入
func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("/Users/spring/workspace/assault/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("kv"))
	assert.Equal(t, 10, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("storage"))
	assert.Equal(t, 7, n)
	assert.Nil(t, err)
	// === RUN   TestFileIO_Write
	// --- PASS: TestFileIO_Write (0.00s)
	// PASS
}

// TestFileIO_Read 文件读取
func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("/Users/spring/workspace/assault/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	b1 := make([]byte, 5)
	n, err := fio.Read(b1, 0)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b1)

	b2 := make([]byte, 5)
	n, err = fio.Read(b2, 5)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-b"), b2)
	// === RUN   TestFileIO_Read
	// --- PASS: TestFileIO_Read (0.00s)
	// PASS
}

// TestFileIO_Sync 文件同步
func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("/Users/spring/workspace/assault/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
	// === RUN   TestFileIO_Sync
	// --- PASS: TestFileIO_Sync (0.01s)
	// PASS
}

// TestFileIO_Close 文件关闭
func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("/Users/spring/workspace/assault/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
	// === RUN   TestFileIO_Close
	// --- PASS: TestFileIO_Close (0.00s)
	// PASS
}

/*
测试完毕
=== RUN   TestNewFileIOManager
--- PASS: TestNewFileIOManager (0.00s)
=== RUN   TestFileIO_Write
--- PASS: TestFileIO_Write (0.00s)
=== RUN   TestFileIO_Read
--- PASS: TestFileIO_Read (0.00s)
=== RUN   TestFileIO_Sync
--- PASS: TestFileIO_Sync (0.00s)
=== RUN   TestFileIO_Close
--- PASS: TestFileIO_Close (0.00s)
PASS
ok      kv-projects/fio 0.366s

*/
