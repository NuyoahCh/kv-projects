// Package kv_projects
// @Author NuyoahCh
// @Date 2025/2/12 23:13
// @Desc 数据读写操作流程
package kv_projects

import (
	"errors"
	"io"
	"kv-projects/data"
	"kv-projects/index"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask 存储引擎实例
type DB struct {
	options    Options                   //文件执行的选项
	mu         *sync.RWMutex             // 创建读写锁
	fileIds    []int                     // 文件 id，只能在加载索引的时候使用，不能在其他的地方更新和使用
	activeFile *data.DataFile            // 当前的活跃数据文件，可以用于写入
	olderFiles map[uint32]*data.DataFile // 旧的数据文件，只能用于读
	index      index.Indexer             // 内存索引
}

// Open 打开 bitcask 存储引擎实例
func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 判断数据目录是否存在，如果不存在的话，则创建这个目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化 DB 实例结构体
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	// 加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 从数据文件中加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
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
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
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

// loadDataFiles 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	// 读取文件目录
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// 遍历目录中的所有的文件，找到所有以 .data 结尾的文件
	for _, entry := range dirEntries {
		// 字符串是否以后缀结束 DataFileNameSuffix
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			// 遍历出来的所有文件都以 "." 的方式进行分割
			spiltNames := strings.Split(entry.Name(), ".")
			// 转化成为数字
			fileId, err := strconv.Atoi(spiltNames[0])
			// 数据目录有可能损坏了
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			// 追加到文件的 Id 上
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件 id 进行排序，从小到大一次进行加载
	sort.Ints(fileIds)
	db.fileIds = fileIds // 放回到数据库中进行存储

	// 遍历每个文件 id，打开对应的数据文件
	for i, fid := range fileIds {
		// 打开对应文件
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}

		// 最后一个，id 是最大的，说明是当前的活跃文件
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else { // 说明是旧的文件
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

// loadIndexFromDataFiles 从数据文件中加载索引，遍历文件中所有记录，更新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
	// 没有文件，说明数据库是空的，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	// 遍历所有的文件 id，处理文件中的记录
	for i, fid := range db.fileIds {
		// 文件 id
		var fileId = uint32(fid)
		// 数据文件
		var dataFile *data.DataFile
		// 文件 id 是活跃文件
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0
		for {
			// 读取日志记录
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				// io 异常
				if err == io.EOF {
					break
				}
				return err
			}
			// 构造内存索引并且保存
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}
			if logRecord.Type == data.LogRecordDeleted {
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, logRecordPos)
			}
			// 递增 offset，下一次从新的位置开始读取
			offset += size
		}
		// 如果是当前活跃文件，更新这个文件的 WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	return nil
}

// checkOptions 检查 Options 结构体的异常问题
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	return nil
}
