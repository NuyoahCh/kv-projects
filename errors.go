// Package kv_projects
// @Author NuyoahCh
// @Date 2025/2/12 23:13
// @Desc 通用错误枚举
package kv_projects

import "errors"

// 预先枚举声明错误类型
var (
	ErrKeyIsEmpty        = errors.New("the key is empty")
	ErrIndexUpdateFailed = errors.New("failed to update index")
	ErrKeyNotFound       = errors.New("key not found in database")
	ErrDataFileNotFound  = errors.New("data file is not found")
)
