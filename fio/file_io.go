// Package fio
// @Author NuyoahCh
// @Date 2025/2/12 23:13
// @Desc IO 操作实现方法
package fio

import "os"

// FileIO 标准系统文件 IO
type FileIO struct {
	fd *os.File // 系统文件描述符
}

// NewFileIOManager 初始化标准文件 IO, 创建实例用于测试
func NewFileIOManager(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(
		fileName,                          // 文件名称
		os.O_CREATE|os.O_RDWR|os.O_APPEND, // 操作方式
		DataFilePerm,                      //设置权限
	)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

// Read 从文件的给定位置读取到对应数据
func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

// Write 写入字节数组到文件中
func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

// Sync 持久化数据
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

// Close 关闭文件
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

// Size 检查 Size 大小
func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
