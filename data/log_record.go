// Package data
// @Author NuyoahCh
// @Date 2025/2/12 23:12
// @Desc 创建内存索引结构和其声明
package data

// LogRecordPos 数据内存索引，主要是描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id，表示将数据存储到了哪个文件当中
	Offset int64  // 偏移，表示将数据存储到了数据文件中的哪个位置
}
