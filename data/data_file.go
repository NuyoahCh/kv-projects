// Package data
// @Author NuyoahCh
// @Date 2025/2/12 23:13
// @Desc 数据文件读取写入同步
package data

import "kv-projects/fio"

// DataFile 数据文件
type DataFile struct {
	FileId    uint32        // 文件 id
	WriteOff  int64         // 文件写到了哪个位置
	IoManager fio.IOManager // io 读写管理
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

// ReadLogRecord 读取日志记录
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, error) {
	return nil, nil
}

// Write 写入文件
func (df *DataFile) Write(buf []byte) error {
	return nil
}

// Sync 同步
func (df *DataFile) Sync() error {
	return nil
}
