// Package store 提供本地列式分析存储：并发写入、快照查询、
// 结构演进与可区分错误的查询执行。
package store

import "fmt"

// ErrCode 错误分类码，用于区分失败原因。
type ErrCode string

const (
	// ErrCodeConflict 写冲突（可再细分原因）。
	ErrCodeConflict ErrCode = "CONFLICT"
	// ErrCodeSchemaIncompatible 不兼容的结构变更。
	ErrCodeSchemaIncompatible ErrCode = "SCHEMA_INCOMPATIBLE"
	// ErrCodeCorruptFragment 数据片段损坏（校验失败）。
	ErrCodeCorruptFragment ErrCode = "CORRUPT_FRAGMENT"
	// ErrCodeResourceExhausted 资源受限（内存/配额超限）。
	ErrCodeResourceExhausted ErrCode = "RESOURCE_EXHAUSTED"
	// ErrCodeInterrupted 读取中断（上下文取消等）。
	ErrCodeInterrupted ErrCode = "INTERRUPTED"
	// ErrCodeNotFound 表或快照不存在。
	ErrCodeNotFound ErrCode = "NOT_FOUND"
	// ErrCodeVersionUnavailable 请求的历史版本超出保留窗口或尚未提交。
	ErrCodeVersionUnavailable ErrCode = "VERSION_UNAVAILABLE"
)

// VersionUnavailable 的细分原因。
const (
	// ReasonVersionTrimmed 版本曾存在但已超出保留窗口被裁剪。
	ReasonVersionTrimmed = "VERSION_TRIMMED"
	// ReasonVersionNotCommitted 请求版本晚于当前最新版本（尚未提交）。
	ReasonVersionNotCommitted = "VERSION_NOT_COMMITTED"
)

// ConflictReason 写冲突的细分原因。
type ConflictReason string

const (
	// ConflictSchemaVersion 基于过期的结构版本提交。
	ConflictSchemaVersion ConflictReason = "STALE_SCHEMA_VERSION"
	// ConflictFragmentOverlap 写入的片段键范围与已提交片段重叠。
	ConflictFragmentOverlap ConflictReason = "FRAGMENT_OVERLAP"
	// ConflictTableDropped 目标表已被删除。
	ConflictTableDropped ConflictReason = "TABLE_DROPPED"
)

// Error 归一化错误，携带分类码与原因。
type Error struct {
	Code   ErrCode
	Reason string
	Op     string
	Err    error
}

func (e *Error) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("%s: %s (%s): %v", e.Op, e.Code, e.Reason, e.Err)
	}
	return fmt.Sprintf("%s: %s: %v", e.Op, e.Code, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// CodeOf 提取错误的分类码；非归一化错误返回空串。
func CodeOf(err error) ErrCode {
	if e, ok := err.(*Error); ok {
		return e.Code
	}
	return ""
}
