// Package data
// @Author NuyoahCh
// @Date 2025/2/12 23:12
// @Desc 创建内存索引结构和其声明
package data

// LogRecordType 日志类型
type LogRecordType = byte

// 日志记录常量
const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// LogRecord 写入到数据文件的记录，之所以叫做日志，是因为数据文件中的数据是追加写入的，类型日志格式
type LogRecord struct {
	Key   []byte        // 键
	Value []byte        //值
	Type  LogRecordType // 日志类型
}

// LogRecordPos 数据内存索引，主要是描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id，表示将数据存储到了哪个文件当中
	Offset int64  // 偏移，表示将数据存储到了数据文件中的哪个位置
}

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组及长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}
