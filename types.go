package colstore

import (
	"fmt"
	"strconv"
)

// ColumnType 是字段的逻辑类型。字段一经建立，类型永不变更。
type ColumnType uint8

const (
	TypeUnknown ColumnType = 0
	TypeInt64   ColumnType = 1
	TypeFloat64 ColumnType = 2
	TypeString  ColumnType = 3
	TypeBool    ColumnType = 4
)

func (t ColumnType) String() string {
	switch t {
	case TypeInt64:
		return "int64"
	case TypeFloat64:
		return "float64"
	case TypeString:
		return "string"
	case TypeBool:
		return "bool"
	default:
		return "unknown"
	}
}

// valid 判定类型本身是否合法。
func (t ColumnType) valid() bool {
	return t == TypeInt64 || t == TypeFloat64 || t == TypeString || t == TypeBool
}

// ordered 判定类型是否支持 < <= > >=。
func (t ColumnType) ordered() bool {
	return t == TypeInt64 || t == TypeFloat64 || t == TypeString
}

// FieldID 是字段的稳定身份；重命名不改变 FieldID，列式片段按 FieldID 存取。
type FieldID int64

// Value 是标量值的统一表示。Null 为真时其余载荷无意义。
type Value struct {
	Null  bool
	Type  ColumnType
	Int   int64
	Float float64
	Str   string
	Bool  bool
}

func nullValue() Value { return Value{Null: true} }

// String 给出确定性的可读形式，供测试日志与结果复现使用。
func (v Value) String() string {
	if v.Null {
		return "NULL"
	}
	switch v.Type {
	case TypeInt64:
		return strconv.FormatInt(v.Int, 10)
	case TypeFloat64:
		return strconv.FormatFloat(v.Float, 'g', -1, 64)
	case TypeString:
		return strconv.Quote(v.Str)
	case TypeBool:
		if v.Bool {
			return "true"
		}
		return "false"
	default:
		return "<?>"
	}
}

// matchesType 判定值的载荷类型与声明类型是否一致。
func (v Value) matchesType(t ColumnType) bool {
	if v.Null {
		return true
	}
	return v.Type == t
}

// asFloat 把数值转为 float64，仅供聚合使用。
func (v Value) asFloat() (float64, bool) {
	switch v.Type {
	case TypeInt64:
		return float64(v.Int), true
	case TypeFloat64:
		return v.Float, true
	default:
		return 0, false
	}
}

// compareValues 按确定序比较两个同类型非空值：
// 返回 -1 / 0 / 1。类型不一致视为非法，由调用方提前拦截。
func compareValues(a, b Value) int {
	switch a.Type {
	case TypeInt64:
		switch {
		case a.Int < b.Int:
			return -1
		case a.Int > b.Int:
			return 1
		default:
			return 0
		}
	case TypeFloat64:
		switch {
		case a.Float < b.Float:
			return -1
		case a.Float > b.Float:
			return 1
		default:
			return 0
		}
	case TypeString:
		switch {
		case a.Str < b.Str:
			return -1
		case a.Str > b.Str:
			return 1
		default:
			return 0
		}
	case TypeBool:
		// bool 只定义相等序；true > false 仅用于完整性。
		ai, bi := 0, 0
		if a.Bool {
			ai = 1
		}
		if b.Bool {
			bi = 1
		}
		return ai - bi
	default:
		panic(fmt.Sprintf("colstore: cannot compare values of type %s", a.Type))
	}
}

// compareFilter 按算子计算 cell 是否满足谓词。
// NULL 与任何值的比较结果都不为真（SQL 三值逻辑中 unknown 不满足 WHERE）。
func compareFilter(cell Value, op FilterOp, rhs Value) (bool, error) {
	if cell.Null || rhs.Null {
		return false, nil
	}
	if cell.Type != rhs.Type {
		return false, fmt.Errorf("%w: comparing %s with %s", ErrInvalidQuery, cell.Type, rhs.Type)
	}
	c := compareValues(cell, rhs)
	switch op {
	case OpEq:
		return c == 0, nil
	case OpNe:
		return c != 0, nil
	case OpLt:
		return c < 0, nil
	case OpLe:
		return c <= 0, nil
	case OpGt:
		return c > 0, nil
	case OpGe:
		return c >= 0, nil
	default:
		return false, fmt.Errorf("%w: unknown filter operator %q", ErrInvalidQuery, op)
	}
}

// Row 是一行数据，按 FieldID 取值。
type Row map[FieldID]Value

// ColumnDef 描述一个字段（含已删除字段的墓碑）。
type ColumnDef struct {
	ID            FieldID
	Name          string
	Type          ColumnType
	Nullable      bool
	HasDefault    bool
	Default       Value
	Dropped       bool
	PreviousNames []string
}

// Schema 是某一版本的表结构，Columns 包含已删除字段（墓碑），
// 因此历史片段中的 FieldID 永远可解析。
type Schema struct {
	Version int
	Columns []*ColumnDef
}

// FilterOp 是过滤比较算子。
type FilterOp string

const (
	OpEq FilterOp = "="
	OpNe FilterOp = "!="
	OpLt FilterOp = "<"
	OpLe FilterOp = "<="
	OpGt FilterOp = ">"
	OpGe FilterOp = ">="
)

// Filter 是一个等值/比较谓词。
type Filter struct {
	Column string
	Op     FilterOp
	Value  Value
}

// AggFunc 是聚合函数。
type AggFunc string

const (
	AggCount AggFunc = "count"
	AggSum   AggFunc = "sum"
	AggMin   AggFunc = "min"
	AggMax   AggFunc = "max"
	AggAvg   AggFunc = "avg"
)

// AggSpec 描述一个聚合；Column 为空时 COUNT 表示 COUNT(*)。
type AggSpec struct {
	Func   AggFunc
	Column string
}

// Query 是一次快照查询。
type Query struct {
	Filters     []Filter
	Projections []string // 为空表示所有当前可见字段
	Aggregates  []AggSpec
	Limit       int // 仅对非聚合行扫描生效；0 走预算上限
}

// AggResult 是单个聚合的结果。
type AggResult struct {
	Func   AggFunc
	Column string
	Value  Value
	Count  int64 // 聚合计入的非空行数
}

// Result 是查询结果。
type Result struct {
	Generation      int
	SchemaVersion   int
	SegmentsScanned int
	RowsScanned     int64
	BytesRead       int64
	MatchedRows     int64
	Columns         []string
	Rows            [][]Value
	Aggregates      []AggResult
}
