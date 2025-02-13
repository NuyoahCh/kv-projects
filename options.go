// Package kv_projects
// @Author NuyoahCh
// @Date 2025/2/12 23:13
// @Desc 文件执行的选项
package kv_projects

import "os"

// Options 文件执行的选项
type Options struct {
	// 数据库数据目录
	DirPath string

	// 数据文件的大小
	DataFileSize int64

	// 每次写数据是否持久化
	SyncWrites bool

	IndexType IndexerType
}

type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota + 1

	// ART Adaptive Radix Tree 自适应基数树索引
	ART
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
	IndexType:    BTree,
}
