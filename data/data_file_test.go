// Package data
// @Author NuyoahCh
// @Date 2025/2/12 23:13
// @Desc
package data

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	dataFile2, err := OpenDataFile(os.TempDir(), 111)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)

	dataFile3, err := OpenDataFile(os.TempDir(), 111)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile3)
	// === RUN   TestOpenDataFile
	// --- PASS: TestOpenDataFile (0.00s)
	// PASS
}

func TestDataFile_Write(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile.Write([]byte("bbb"))
	assert.Nil(t, err)

	err = dataFile.Write([]byte("ccc"))
	assert.Nil(t, err)
	// === RUN   TestDataFile_Write
	// --- PASS: TestDataFile_Write (0.00s)
	// PASS
}

func TestDataFile_Close(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 123)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile.Close()
	assert.Nil(t, err)
	// === RUN   TestDataFile_Close
	// --- PASS: TestDataFile_Close (0.00s)
	// PASS
}

func TestDataFile_Sync(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 456)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile.Sync()
	assert.Nil(t, err)
	// === RUN   TestDataFile_Sync
	// --- PASS: TestDataFile_Sync (0.01s)
	// PASS
}

/*
测试完毕
=== RUN   TestOpenDataFile
--- PASS: TestOpenDataFile (0.00s)
=== RUN   TestDataFile_Write
--- PASS: TestDataFile_Write (0.00s)
=== RUN   TestDataFile_Close
--- PASS: TestDataFile_Close (0.00s)
=== RUN   TestDataFile_Sync
--- PASS: TestDataFile_Sync (0.01s)
PASS
ok      kv-projects/data        0.375s
*/
