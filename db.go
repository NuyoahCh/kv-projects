// Package kv_projects
// @Author NuyoahCh
// @Date 2025/2/12 23:13
// @Desc 数据读写操作流程
package kv_projects

import (
	"kv-projects/data"
	"kv-projects/index"
	"sync"
)

// DB bitcask 存储引擎实例
type DB struct {
	options    Options                   //文件执行的选项
	mu         *sync.RWMutex             // 创建读写锁
	activeFile *data.DataFile            // 当前的活跃数据文件，可以用于写入
	olderFiles map[uint32]*data.DataFile // 旧的数据文件，只能用于读
	index      index.Indexer             // 内存索引
}

// Put 写入 Key/Value 数据，key 不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入到当前活跃数据文件当中
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}
	// 更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// Get 根据 key 读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()         // 操作前先加锁，保证并发安全
	defer db.mu.Unlock() // 兜底策略，保证一定会解锁，防止死锁的现象

	// 判断 key 的有效性
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	// 从内存数据结构中取出 key 对应的索引信息
	logRecordPos := db.index.Get(key)
	// 如果 key 不在内存索引中，说明 key 不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// 根据文件 id 找到对应的数据文件
	var dataFile *data.DataFile
	// 活跃文件的 id 和日志记录位置 id
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}
	// 数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	// 根据偏移读取对应的数据
	logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	// 日志记录类型已经被删除
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	// 返回日志的值
	return logRecord.Value, nil
}

// appendLogRecord 追加写数据到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()         // 操作前先加锁，保证并发安全
	defer db.mu.Unlock() // 兜底策略，保证一定会解锁，防止死锁的现象

	// 判断当前活跃数据文件是否存在，因为数据库在没有写入的时候是没有文件生成的
	// 如果为空则初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	// 写入数据编码
	encRecord, size := data.EncodeLogRecord(logRecord)
	// 如果写入的数据已经到达了活跃文件的阈值，则关闭活跃文件，并且打开新的文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// 先同步持久化数据文件，保证已有的数据持久化到磁盘当中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 当前活跃文件转化为旧的数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	// 文件写入的位置
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}
	// 根据用户配置决定是否持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	// 构造内存索引信息，确定其位置
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil
}

// setActiveDataFile 设置当前活跃文件，访问之前必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0 // 初始化文件 id
	// 判断文件的性质
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1 // 更改初始文件 id
	}
	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	// 新的数据文件传递给活跃文件
	db.activeFile = dataFile
	return nil
}
