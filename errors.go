package colstore

import (
	"context"
	"errors"
	"fmt"
)

// ErrorCategory 是归一化后的错误大类，调用方据此区分失败原因，
// 而不必依赖底层错误文案。大类取值稳定，不会随实现调整而变化。
type ErrorCategory string

const (
	CatNone               ErrorCategory = ""
	CatConflict           ErrorCategory = "conflict"            // 写冲突：被拒绝的提交
	CatIncompatibleSchema ErrorCategory = "incompatible_schema" // 结构变更不兼容：被拒绝
	CatCorruptSegment     ErrorCategory = "corrupt_segment"     // 数据片段损坏
	CatReadInterrupted    ErrorCategory = "read_interrupted"    // 读取被外部中断（context 取消）
	CatResourceLimit      ErrorCategory = "resource_limit"      // 资源预算耗尽
	CatInvalidQuery       ErrorCategory = "invalid_query"       // 查询语义不合法
	CatInvalidInput       ErrorCategory = "invalid_input"       // 写入输入不合法
	CatIO                 ErrorCategory = "io_error"            // 其他底层 I/O 故障
)

// ConflictReason 区分写冲突的具体原因。
type ConflictReason string

const (
	ConflictNone             ConflictReason = ""
	ConflictConcurrentCommit ConflictReason = "concurrent_commit" // 开启事务后有其它事务已提交
	ConflictSchemaModified   ConflictReason = "schema_modified"   // 开启事务后结构版本已推进（DDL 与 DML 撞车）
)

// SchemaChangeReason 区分不兼容结构变更的原因。
type SchemaChangeReason string

const (
	SchemaChangeNone          SchemaChangeReason = ""
	SchemaDuplicateColumn     SchemaChangeReason = "duplicate_column"       // 同名字段（含历史名字）已存在
	SchemaColumnNotFound      SchemaChangeReason = "column_not_found"       // 目标字段不存在或已删除
	SchemaTypeMismatch        SchemaChangeReason = "type_mismatch"          // 默认值类型与字段类型不一致
	SchemaRenameTargetExists  SchemaChangeReason = "rename_target_exists"   // 新名字已被其它字段占用
	SchemaNotNullWithoutValue SchemaChangeReason = "not_null_without_value" // NOT NULL 字段必须提供默认值
)

// 哨兵错误，配合 errors.Is 使用。
var (
	ErrConflict           = errors.New("write conflict")
	ErrIncompatibleSchema = errors.New("incompatible schema change")
	ErrCorruptSegment     = errors.New("corrupt segment")
	ErrReadInterrupted    = errors.New("read interrupted")
	ErrResourceLimit      = errors.New("resource limit exceeded")
	ErrInvalidQuery       = errors.New("invalid query")
	ErrInvalidInput       = errors.New("invalid input")
)

// 片段损坏的细粒度原因，写入 ClassifiedError.Reason。
const (
	CorruptBadMagic   = "bad_magic"
	CorruptBadVersion = "bad_version"
	CorruptTruncated  = "truncated"
	CorruptCRC        = "crc_mismatch"
	CorruptBadHeader  = "bad_header"
	CorruptChunkOrder = "chunk_order"
	CorruptDuplicate  = "duplicate_chunk"
	CorruptMissing    = "missing_chunk"
)

// 资源受限的细粒度原因。
const (
	LimitScanBytes   = "scan_bytes"
	LimitMatchedRows = "matched_rows"
)

// ClassifiedError 是归一化后的错误形态：稳定的大类 + 细粒度原因 + 定位信息。
type ClassifiedError struct {
	Category  ErrorCategory
	Reason    string
	Op        string
	SegmentID int64
	Message   string
	Cause     error
}

func (e *ClassifiedError) Error() string {
	if e == nil {
		return ""
	}
	s := string(e.Category)
	if e.Reason != "" {
		s += "(" + e.Reason + ")"
	}
	if e.Op != "" {
		s += " at " + e.Op
	}
	if e.SegmentID != 0 {
		s += fmt.Sprintf(" segment=%d", e.SegmentID)
	}
	if e.Message != "" {
		s += ": " + e.Message
	}
	if e.Cause != nil {
		s += ": " + e.Cause.Error()
	}
	return s
}

func (e *ClassifiedError) Unwrap() error { return e.Cause }

// ConflictError 表示写事务因冲突被拒绝，事务不会产生任何片段，已提交状态不受影响。
type ConflictError struct {
	Reason               ConflictReason
	BaseGeneration       int
	CurrentGeneration    int
	BaseSchemaVersion    int
	CurrentSchemaVersion int
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("write conflict (%s): base generation=%d current=%d, base schemaVersion=%d current=%d",
		e.Reason, e.BaseGeneration, e.CurrentGeneration, e.BaseSchemaVersion, e.CurrentSchemaVersion)
}

func (e *ConflictError) Unwrap() error { return ErrConflict }

// SchemaChangeError 表示结构变更因不兼容被明确拒绝。
type SchemaChangeError struct {
	Reason SchemaChangeReason
	Column string
	Detail string
}

func (e *SchemaChangeError) Error() string {
	s := fmt.Sprintf("incompatible schema change (%s)", e.Reason)
	if e.Column != "" {
		s += " column=" + e.Column
	}
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	return s
}

func (e *SchemaChangeError) Unwrap() error { return ErrIncompatibleSchema }

func newConflictError(reason ConflictReason, baseGen, curGen, baseSV, curSV int) *ConflictError {
	return &ConflictError{
		Reason:               reason,
		BaseGeneration:       baseGen,
		CurrentGeneration:    curGen,
		BaseSchemaVersion:    baseSV,
		CurrentSchemaVersion: curSV,
	}
}

func newSchemaChangeError(reason SchemaChangeReason, column, detail string) *SchemaChangeError {
	return &SchemaChangeError{Reason: reason, Column: column, Detail: detail}
}

func classified(category ErrorCategory, reason, op string, segID int64, msg string, cause error) *ClassifiedError {
	// 按大类把稳定哨兵接入错误链，使 errors.Is 与 Normalize 同时可用；
	// 若已有底层 cause，则用 errors.Join 同时保留二者。
	sentinel := sentinelFor(category)
	switch {
	case sentinel != nil && cause != nil && !errors.Is(cause, sentinel):
		cause = errors.Join(sentinel, cause)
	case sentinel != nil && cause == nil:
		cause = sentinel
	}
	return &ClassifiedError{Category: category, Reason: reason, Op: op, SegmentID: segID, Message: msg, Cause: cause}
}

func sentinelFor(c ErrorCategory) error {
	switch c {
	case CatConflict:
		return ErrConflict
	case CatIncompatibleSchema:
		return ErrIncompatibleSchema
	case CatCorruptSegment:
		return ErrCorruptSegment
	case CatReadInterrupted:
		return ErrReadInterrupted
	case CatResourceLimit:
		return ErrResourceLimit
	case CatInvalidQuery:
		return ErrInvalidQuery
	case CatInvalidInput:
		return ErrInvalidInput
	default:
		return nil
	}
}

func corruptError(segID int64, reason, msg string, cause error) *ClassifiedError {
	return classified(CatCorruptSegment, reason, "read_segment", segID, msg, cause)
}

func limitError(reason string, msg string) *ClassifiedError {
	return classified(CatResourceLimit, reason, "query", 0, msg, ErrResourceLimit)
}

// Normalize 把任意错误归一化为 *ClassifiedError。
// 归一化规则是确定的：同类底层错误永远映射到同一 Category/Reason，
// 使调用方（与测试）可以稳定地区分失败原因。
func Normalize(err error) *ClassifiedError {
	if err == nil {
		return nil
	}
	var ce *ClassifiedError
	if errors.As(err, &ce) {
		return ce
	}
	var conf *ConflictError
	if errors.As(err, &conf) {
		return &ClassifiedError{
			Category: CatConflict,
			Reason:   string(conf.Reason),
			Op:       "commit",
			Message:  conf.Error(),
			Cause:    conf,
		}
	}
	var sce *SchemaChangeError
	if errors.As(err, &sce) {
		return &ClassifiedError{
			Category: CatIncompatibleSchema,
			Reason:   string(sce.Reason),
			Op:       "alter_schema",
			Message:  sce.Error(),
			Cause:    sce,
		}
	}
	// context 取消 / 超时一律归一为“读取中断”，区别于片段损坏与预算耗尽。
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return classified(CatReadInterrupted, "context_canceled", "query", 0, "read canceled by caller", err)
	}
	// 未知错误不猜测，归入通用 I/O，保证总有明确分类。
	return classified(CatIO, "unknown", "", 0, err.Error(), err)
}
