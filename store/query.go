package store

import (
	"context"
	"fmt"
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
func Execute(ctx context.Context, sn *Snapshot, q *Query, lim *MemLimiter) (*Result, error) {
	res, _, err := execute(ctx, sn, q, lim)
	return res, err
}

// ExecuteStats 与 Execute 结果完全一致，额外返回裁剪统计
// （总片段数 / 被跳过片段数 / 实际扫描片段数 / 命中行数）。
func ExecuteStats(ctx context.Context, sn *Snapshot, q *Query, lim *MemLimiter) (*Result, *QueryStats, error) {
	return execute(ctx, sn, q, lim)
}

func execute(ctx context.Context, sn *Snapshot, q *Query, lim *MemLimiter) (*Result, *QueryStats, error) {
	stats := &QueryStats{}
	schema := sn.Schema()

	// 解析谓词与聚合所需的字段 ID（按快照结构的名字解析，
	// 重命名后按新名命中同一列，语义稳定）。
	var preds []boundPred
	for _, p := range q.Predicates {
		f := schema.FieldByName(p.Field)
		if f == nil {
			return nil, nil, &Error{Code: ErrCodeSchemaIncompatible, Op: "bind-predicate",
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
				return nil, nil, &Error{Code: ErrCodeSchemaIncompatible, Op: "bind-aggregate",
					Reason: "NO_SUCH_FIELD:" + a.Field, Err: fmt.Errorf("aggregate field not in snapshot schema")}
			}
			st.fieldID = f.ID
		}
		aggs[i] = st
	}

	metas := sn.Fragments()
	stats.FragmentsTotal = len(metas)
	for _, meta := range metas {
		if err := ctx.Err(); err != nil {
			return nil, nil, &Error{Code: ErrCodeInterrupted, Op: "execute", Err: err}
		}

		// 裁剪：先只读片段尾部 zone map（不加载列数据）。
		// CFR1（无 zone）不裁剪；缺列/全 NULL 列由 pruneFragment 安全
		// 处理，保证“被跳过的片段不可能有任何行满足全部谓词”。
		if len(preds) > 0 {
			zones, err := sn.store.loadZone(sn.table, meta)
			if err != nil {
				return nil, nil, err
			}
			if zones != nil && pruneFragment(zones, preds) {
				stats.FragmentsSkipped++
				continue
			}
		}

		frag, err := sn.store.loadFragment(sn.table, meta)
		if err != nil {
			return nil, nil, err // 已归一化：CORRUPT_FRAGMENT / INTERRUPTED
		}
		stats.FragmentsScanned++

		// 逐行评估谓词；命中行才计入内存配额与聚合状态。
		n := frag.NumRows
		for row := 0; row < n; row++ {
			if row%1024 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, nil, &Error{Code: ErrCodeInterrupted, Op: "execute", Err: err}
				}
			}
			if !matchAll(frag, preds, row) {
				continue
			}
			if err := lim.Acquire(perRowBytes); err != nil {
				return nil, nil, err
			}
			stats.RowsMatched++
			for _, st := range aggs {
				st.add(frag, row)
			}
		}
	}

	res := &Result{Values: make([]Value, len(aggs))}
	for i, st := range aggs {
		res.Values[i] = st.final()
	}
	return res, stats, nil
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
func compareValues(a, b Value) (int, bool) {
	switch x := a.(type) {
	case int64:
		switch y := b.(type) {
		case int64:
			return cmpOrdered(x, y), true
		case float64:
			return cmpOrdered(float64(x), y), true
		}
	case float64:
		switch y := b.(type) {
		case int64:
			return cmpOrdered(x, float64(y)), true
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
