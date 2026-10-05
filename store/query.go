package store

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
)

// Predicate 行过滤条件。
type Predicate struct {
	Field string // 字段名（按快照结构解析）
	Op    CmpOp
	Value Value
}

// CmpOp 比较运算符。
type CmpOp int

const (
	OpEq CmpOp = iota
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
)

// AggKind 聚合类型。
type AggKind int

const (
	AggCount AggKind = iota
	AggSum
	AggMin
	AggMax
)

// Aggregate 一个聚合表达式。
type Aggregate struct {
	Kind  AggKind
	Field string // Count 时可为空（计数行数）
}

// Query 查询描述：过滤 + 聚合。
type Query struct {
	Predicates []Predicate
	Aggregates []Aggregate
}

// Result 聚合结果，与 Query.Aggregates 一一对应。
type Result struct {
	Values []Value
}

// QueryOptions 查询执行的附加预算。
type QueryOptions struct {
	// FragmentBytesBudget 读取片段数据的字节预算（按实际读取的文件大小计费，
	// 被区段裁剪跳过的片段不占用预算）。0 表示不限。
	// 超限时返回 RESOURCE_EXHAUSTED 且不产出结果。
	FragmentBytesBudget int64
}

// MemLimiter 限制查询内存占用；超出时返回
// ErrCodeResourceExhausted，保证内存随命中范围有界。
type MemLimiter struct {
	BudgetBytes int64
	used        atomic.Int64
}

// Acquire 申请 n 字节；不足时返回资源受限错误。
func (m *MemLimiter) Acquire(n int64) error {
	if m == nil {
		return nil
	}
	for {
		cur := m.used.Load()
		if cur+n > m.BudgetBytes {
			return &Error{Code: ErrCodeResourceExhausted, Op: "mem-limit",
				Reason: fmt.Sprintf("BUDGET_%d_EXCEEDED", m.BudgetBytes),
				Err:    fmt.Errorf("need %d bytes, used %d", n, cur)}
		}
		if m.used.CompareAndSwap(cur, cur+n) {
			return nil
		}
	}
}

// Release 释放 n 字节。
func (m *MemLimiter) Release(n int64) {
	if m == nil {
		return
	}
	m.used.Add(-n)
}

// 每行命中数据的估算内存（用于把内存占用与命中范围挂钩）。
const perRowBytes = 64

// aggState 单个聚合的运行态。
type aggState struct {
	kind    AggKind
	fieldID int // -1 表示行计数
	count   int64
	sum     float64
	hasVal  bool
	min     float64
	max     float64
}

// Execute 在快照上执行查询。逐片段、逐列流式处理，
// 内存占用与命中数据相关而非整表规模。
// 片段损坏 / 上下文取消 / 内存不足分别返回可区分错误。
func Execute(ctx context.Context, sn *Snapshot, q *Query, lim *MemLimiter, opts ...QueryOptions) (*Result, error) {
	schema := sn.Schema()
	var qopt QueryOptions
	if len(opts) > 0 {
		qopt = opts[0]
	}
	var fragLim *MemLimiter
	if qopt.FragmentBytesBudget > 0 {
		fragLim = &MemLimiter{BudgetBytes: qopt.FragmentBytesBudget}
	}

	// 解析谓词与聚合所需的字段 ID（按快照结构的名字解析，
	// 重命名后按新名命中同一列，语义稳定）。
	var preds []boundPred
	for _, p := range q.Predicates {
		f := schema.FieldByName(p.Field)
		if f == nil {
			return nil, &Error{Code: ErrCodeSchemaIncompatible, Op: "bind-predicate",
				Reason: "NO_SUCH_FIELD:" + p.Field, Err: fmt.Errorf("predicate field not in snapshot schema")}
		}
		preds = append(preds, boundPred{fieldID: f.ID, op: p.Op, val: p.Value})
	}
	aggs := make([]*aggState, len(q.Aggregates))
	for i, a := range q.Aggregates {
		st := &aggState{kind: a.Kind, fieldID: -1}
		if a.Kind != AggCount || a.Field != "" {
			f := schema.FieldByName(a.Field)
			if f == nil {
				return nil, &Error{Code: ErrCodeSchemaIncompatible, Op: "bind-aggregate",
					Reason: "NO_SUCH_FIELD:" + a.Field, Err: fmt.Errorf("aggregate field not in snapshot schema")}
			}
			st.fieldID = f.ID
		}
		aggs[i] = st
	}

	var fragHeld int64 // 已扫描片段累计占用，查询结束统一释放
	for _, meta := range sn.Fragments() {
		if err := ctx.Err(); err != nil {
			fragLim.Release(fragHeld)
			return nil, &Error{Code: ErrCodeInterrupted, Op: "execute", Err: err}
		}
		// 区段裁剪：缺列/全 NULL/区段不相交的片段不可能命中，直接跳过
		// （不读文件、不计预算），结果与完整扫描完全一致。
		if !fragmentCanMatch(meta, preds) {
			continue
		}
		// 片段级预算：按文件大小累计计费，查询结束才释放；被裁剪的片段
		// 不计费，因此预算只与实际扫描的数据相关，不随整表历史规模膨胀。
		fragCost := fragmentBudgetCost(meta)
		if err := fragLim.Acquire(fragCost); err != nil {
			fragLim.Release(fragHeld)
			return nil, err
		}
		fragHeld += fragCost
		frag, err := sn.store.loadFragment(sn.table, meta)
		if err != nil {
			fragLim.Release(fragHeld)
			return nil, err // 已归一化：CORRUPT_FRAGMENT / INTERRUPTED
		}

		// 逐行评估谓词；命中行才计入行级内存配额与聚合状态。
		n := frag.NumRows
		for row := 0; row < n; row++ {
			if row%1024 == 0 {
				if err := ctx.Err(); err != nil {
					fragLim.Release(fragHeld)
					return nil, &Error{Code: ErrCodeInterrupted, Op: "execute", Err: err}
				}
			}
			if !matchAll(frag, preds, row) {
				continue
			}
			if err := lim.Acquire(perRowBytes); err != nil {
				fragLim.Release(fragHeld)
				return nil, err
			}
			for _, st := range aggs {
				st.add(frag, row)
			}
		}
	}
	fragLim.Release(fragHeld)

	res := &Result{Values: make([]Value, len(aggs))}
	for i, st := range aggs {
		res.Values[i] = st.final()
	}
	return res, nil
}

type boundPred struct {
	fieldID int
	op      CmpOp
	val     Value
}

func matchAll(frag *Fragment, preds []boundPred, row int) bool {
	for _, p := range preds {
		col, ok := frag.Columns[p.fieldID]
		var v Value
		if ok {
			v = col[row]
		}
		// 历史片段缺列（结构演进前写入）=> 该列值为 NULL，不匹配任何比较。
		if v == nil {
			return false
		}
		if !compare(v, p.op, p.val) {
			return false
		}
	}
	return true
}

func compare(v Value, op CmpOp, bound Value) bool {
	c, ok := compareValues(v, bound)
	if !ok {
		return false
	}
	switch op {
	case OpEq:
		return c == 0
	case OpNe:
		return c != 0
	case OpLt:
		return c < 0
	case OpLe:
		return c <= 0
	case OpGt:
		return c > 0
	case OpGe:
		return c >= 0
	}
	return false
}

// compareValues 同类型比较；数值类型 int64/float64 可互比。
// int64 与 float64 的混合比较是精确的（不做 float64 舍入），
// 保证大整数（>2^53）参与比较时行级过滤与区段裁剪判定一致。
func compareValues(a, b Value) (int, bool) {
	switch x := a.(type) {
	case int64:
		switch y := b.(type) {
		case int64:
			return cmpOrdered(x, y), true
		case float64:
			return cmpInt64Float64(x, y), true
		}
	case float64:
		switch y := b.(type) {
		case int64:
			return -cmpInt64Float64(y, x), true
		case float64:
			return cmpOrdered(x, y), true
		}
	case string:
		if y, ok := b.(string); ok {
			return cmpOrdered(x, y), true
		}
	case bool:
		if y, ok := b.(bool); ok {
			if x == y {
				return 0, true
			}
			if !x {
				return -1, true
			}
			return 1, true
		}
	}
	return 0, false
}

// cmpInt64Float64 精确比较 int64 与 float64，不经过 float64 舍入。
// float64 只能精确表示 2^53 以内的整数，直接 float64(x) 会把
// 9007199254740993 之类的值舍入成相邻偶数，导致比较结果错误。
func cmpInt64Float64(x int64, y float64) int {
	if math.IsNaN(y) {
		return 0 // 与既有 cmpOrdered(NaN) 行为一致：视为相等
	}
	// 2^63 与 -2^63 都可精确表示；超出 int64 范围的 y 直接定胜负。
	if y >= 9223372036854775808.0 { // 2^63
		return -1
	}
	if y < -9223372036854775808.0 { // -2^63
		return 1
	}
	yi := int64(y) // 向零截断；|y|<2^63 时安全
	if x < yi {
		return -1
	}
	if x > yi {
		return 1
	}
	// x == yi：比较小数部分。|y|<2^52 时 float64(yi) 精确；
	// |y|>=2^52 时 y 必为整数，frac 恰为 0。
	frac := y - float64(yi)
	if frac > 0 {
		return -1
	}
	if frac < 0 {
		return 1
	}
	return 0
}

func cmpOrdered[T ~int64 | ~float64 | ~string](a, b T) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func (st *aggState) add(frag *Fragment, row int) {
	st.count++
	if st.fieldID < 0 {
		return
	}
	col, ok := frag.Columns[st.fieldID]
	if !ok {
		return // 历史片段缺列 => NULL，不参与数值聚合
	}
	v := col[row]
	if v == nil {
		return
	}
	f, ok := toFloat(v)
	if !ok {
		return
	}
	st.sum += f
	if !st.hasVal || f < st.min {
		st.min = f
	}
	if !st.hasVal || f > st.max {
		st.max = f
	}
	st.hasVal = true
}

func (st *aggState) final() Value {
	switch st.kind {
	case AggCount:
		return st.count
	case AggSum:
		return st.sum
	case AggMin:
		if !st.hasVal {
			return nil
		}
		return st.min
	case AggMax:
		if !st.hasVal {
			return nil
		}
		return st.max
	}
	return nil
}

func toFloat(v Value) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}
