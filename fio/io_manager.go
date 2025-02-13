// Package fio
// @Author NuyoahCh
// @Date 2025/2/12 23:13
// @Desc IO 输入输出抽象管理接口
package fio

// DataFilePerm 设定文件访问权限
const DataFilePerm = 0644

// IOManager 抽象 IO 管理接口，可以接入不同的 IO 类型，目前支持标准文件 IO
type IOManager interface {
	// Read 从文件的给定位置读取到对应数据
	Read([]byte, int64) (int, error)

	// Write 写入字节数组到文件中
	Write([]byte) (int, error)

	// Sync 持久化数据
	Sync() error

	// Close 关闭文件
	Close() error

	// Size 获取到文件大小
	Size() (int64, error)
}

// NewIOManager 初始化 IOManager，目前只支持标准 FileIO
func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
