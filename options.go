// Package kv_projects
// @Author NuyoahCh
// @Date 2025/2/12 23:13
// @Desc 文件执行的选项
package kv_projects

// Options 文件执行的选项
type Options struct {
	// 数据库数据目录
	DirPath string

	// 数据文件的大小
	DataFileSize int64

	// 每次写数据是否持久化
	SyncWrites bool
}
