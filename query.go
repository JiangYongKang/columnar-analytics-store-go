package colstore

import (
	"context"
	"fmt"
	"path/filepath"
)

// scanBudget 统计一次查询的读取量。预算在“产生读取”之前扣除，
// 因此超限会在实际分配大块内存之前中止，保证占用与命中范围相关、可解释。
type scanBudget struct {
	maxBytes   int64
	bytes      int64
	maxMatched int64
	matched    int64
}

func newScanBudget(maxBytes, maxMatched int64) *scanBudget {
	return &scanBudget{maxBytes: maxBytes, maxMatched: maxMatched}
}

func (b *scanBudget) charge(n int64) error {
	b.bytes += n
	if b.maxBytes > 0 && b.bytes > b.maxBytes {
		return limitError(LimitScanBytes,
			fmt.Sprintf("scan bytes %d exceed budget %d", b.bytes, b.maxBytes))
	}
	return nil
}

func (b *scanBudget) noteMatch() error {
	b.matched++
	if b.maxMatched > 0 && b.matched > b.maxMatched {
		return limitError(LimitMatchedRows,
			fmt.Sprintf("matched rows %d exceed budget %d", b.matched, b.maxMatched))
	}
	return nil
}

// resolvedFilter 是解析到 FieldID 后的谓词。
type resolvedFilter struct {
	col *ColumnDef
	op  FilterOp
	rhs Value
}

type aggState struct {
	spec   AggSpec
	col    *ColumnDef // COUNT(*) 时为 nil
	count  int64      // 计入的非空行数（COUNT(*) 为命中行数）
	sum    float64
	hasMin bool
	hasMax bool
	min    Value
	max    Value
}

// Query 在快照上执行过滤与聚合查询。
// 语义保证：
//   - 只读取该快照清单内的片段，新并发提交对本次查询不可见；
//   - 任何片段损坏 / 中断 / 预算超限都返回归一化错误且不返回行结果，
//     绝不静默给出部分结果；
//   - 仅解码过滤/投影/聚合涉及的字段块，内存与命中范围而非整表规模相关。
func (snap *Snapshot) Query(ctx context.Context, q Query) (*Result, error) {
	schema := snap.st.schema

	filterCols, filters, err := resolveFilters(schema, q.Filters)
	if err != nil {
		return nil, err
	}
	projCols, err := resolveProjections(schema, q.Projections)
	if err != nil {
		return nil, err
	}
	aggs, err := resolveAggregates(schema, q.Aggregates)
	if err != nil {
		return nil, err
	}

	isAgg := len(aggs) > 0
	if isAgg {
		projCols = nil // 聚合查询不返回行
	}

	// 汇总需要读取的字段集合（裁剪列式读取）。
	wanted := make(map[FieldID]bool)
	for id := range filterCols {
		wanted[id] = true
	}
	for _, c := range projCols {
		wanted[c.ID] = true
	}
	for _, a := range aggs {
		if a.col != nil {
			wanted[a.col.ID] = true
		}
	}

	res := &Result{
		Generation:    snap.st.generation,
		SchemaVersion: snap.st.schemaVersion,
	}
	if !isAgg {
		res.Columns = make([]string, len(projCols))
		for i, c := range projCols {
			res.Columns[i] = c.Name
		}
	}

	budget := newScanBudget(snap.opts.MaxScanBytes, snap.opts.MaxMatchedRows)

	for _, seg := range snap.st.segments {
		if err := ctx.Err(); err != nil {
			return nil, classified(CatReadInterrupted, "context_canceled", "query", seg.ID,
				"query canceled before segment", err)
		}
		path := seg.FileName
		if path == "" || !filepath.IsAbs(path) {
			path = segmentFilePath(snap.opts.Dir, seg.ID)
		}
		chunks, bytesRead, derr := decodeChunks(ctx, path, seg.ID, wanted, budget)
		if derr != nil {
			return nil, derr
		}
		res.SegmentsScanned++
		res.BytesRead += bytesRead
		res.RowsScanned += int64(seg.RowCount)

		// 类型交叉校验：片段块类型必须能在快照结构中解析且类型一致，
		// 否则属于数据损坏（防止错列被静默读出）。
		for id, ck := range chunks {
			cdef, ok := schema.columnByID(id)
			if !ok {
				return nil, corruptError(seg.ID, CorruptBadHeader,
					fmt.Sprintf("chunk field id %d not found in snapshot schema", id), nil)
			}
			if cdef.Type != ck.typ {
				return nil, corruptError(seg.ID, CorruptBadHeader,
					fmt.Sprintf("chunk %d type %s disagrees with schema type %s",
						id, ck.typ, cdef.Type), nil)
			}
		}

		for row := 0; row < seg.RowCount; row++ {
			matched, ferr := evalFilters(chunks, row, filters)
			if ferr != nil {
				return nil, corruptError(seg.ID, CorruptBadHeader, ferr.Error(), ferr)
			}
			if !matched {
				continue
			}
			if err := budget.noteMatch(); err != nil {
				return nil, err
			}
			res.MatchedRows++

			if isAgg {
				for _, a := range aggs {
					if err := updateAgg(a, chunks, row); err != nil {
						return nil, corruptError(seg.ID, CorruptBadHeader, err.Error(), err)
					}
				}
			} else {
				if q.Limit > 0 && int64(len(res.Rows)) >= int64(q.Limit) {
					break
				}
				vals := make([]Value, len(projCols))
				for i, c := range projCols {
					vals[i] = readCell(chunks, c, row)
				}
				res.Rows = append(res.Rows, vals)
			}
		}

		if !isAgg && q.Limit > 0 && len(res.Rows) >= q.Limit {
			break
		}
	}

	if isAgg {
		res.Aggregates = make([]AggResult, len(aggs))
		for i, a := range aggs {
			res.Aggregates[i] = finalizeAgg(a)
		}
	}
	return res, nil
}

func resolveFilters(schema *Schema, fs []Filter) (map[FieldID]bool, []resolvedFilter, error) {
	ids := make(map[FieldID]bool)
	out := make([]resolvedFilter, 0, len(fs))
	for _, f := range fs {
		c, ok := schema.columnByName(f.Column)
		if !ok {
			return nil, nil, classified(CatInvalidQuery, "unknown_column", "query", 0,
				fmt.Sprintf("filter references unknown or dropped column %q", f.Column), ErrInvalidQuery)
		}
		if f.rhsOrNull() {
			return nil, nil, classified(CatInvalidQuery, "null_predicate", "query", 0,
				fmt.Sprintf("filter on %q with NULL is not supported (NULL never matches)", f.Column), ErrInvalidQuery)
		}
		if f.Value.Type != c.Type {
			return nil, nil, classified(CatInvalidQuery, "type_mismatch", "query", 0,
				fmt.Sprintf("filter on %q compares %s with %s", f.Column, c.Type, f.Value.Type), ErrInvalidQuery)
		}
		switch f.Op {
		case OpEq, OpNe, OpLt, OpLe, OpGt, OpGe:
		default:
			return nil, nil, classified(CatInvalidQuery, "bad_operator", "query", 0,
				fmt.Sprintf("unknown operator %q", f.Op), ErrInvalidQuery)
		}
		if (f.Op == OpLt || f.Op == OpLe || f.Op == OpGt || f.Op == OpGe) && !c.Type.ordered() {
			return nil, nil, classified(CatInvalidQuery, "unordered_type", "query", 0,
				fmt.Sprintf("column %q of type %s does not support ordering operator %s", f.Column, c.Type, f.Op),
				ErrInvalidQuery)
		}
		ids[c.ID] = true
		out = append(out, resolvedFilter{col: c, op: f.Op, rhs: f.Value})
	}
	return ids, out, nil
}

func resolveProjections(schema *Schema, names []string) ([]*ColumnDef, error) {
	if len(names) == 0 {
		return schema.liveColumns(), nil
	}
	out := make([]*ColumnDef, 0, len(names))
	seen := make(map[FieldID]bool)
	for _, name := range names {
		c, ok := schema.columnByName(name)
		if !ok {
			return nil, classified(CatInvalidQuery, "unknown_column", "query", 0,
				fmt.Sprintf("projection references unknown or dropped column %q", name), ErrInvalidQuery)
		}
		if seen[c.ID] {
			continue
		}
		seen[c.ID] = true
		out = append(out, c)
	}
	return out, nil
}

func resolveAggregates(schema *Schema, specs []AggSpec) ([]*aggState, error) {
	out := make([]*aggState, 0, len(specs))
	for _, sp := range specs {
		a := &aggState{spec: sp}
		switch sp.Func {
		case AggCount, AggSum, AggMin, AggMax, AggAvg:
		default:
			return nil, classified(CatInvalidQuery, "bad_aggregate", "query", 0,
				fmt.Sprintf("unknown aggregate %q", sp.Func), ErrInvalidQuery)
		}
		if sp.Column != "" {
			c, ok := schema.columnByName(sp.Column)
			if !ok {
				return nil, classified(CatInvalidQuery, "unknown_column", "query", 0,
					fmt.Sprintf("aggregate references unknown or dropped column %q", sp.Column), ErrInvalidQuery)
			}
			if sp.Func != AggCount {
				if c.Type != TypeInt64 && c.Type != TypeFloat64 {
					return nil, classified(CatInvalidQuery, "agg_type", "query", 0,
						fmt.Sprintf("%s requires numeric column, got %q of type %s", sp.Func, sp.Column, c.Type),
						ErrInvalidQuery)
				}
			}
			a.col = c
		} else if sp.Func != AggCount {
			return nil, classified(CatInvalidQuery, "agg_requires_column", "query", 0,
				fmt.Sprintf("%s requires a column name", sp.Func), ErrInvalidQuery)
		}
		out = append(out, a)
	}
	return out, nil
}

func evalFilters(chunks map[FieldID]*chunk, row int, fs []resolvedFilter) (bool, error) {
	for _, f := range fs {
		ck := chunks[f.col.ID]
		var cell Value
		if ck == nil {
			// 片段生成于该字段新增之前：历史行取默认值，无默认值则 NULL。
			if f.col.HasDefault {
				cell = f.col.Default
			} else {
				continue // NULL 不满足任何谓词
			}
		} else {
			cell = readCellFromChunk(ck, row)
		}
		ok, err := compareFilter(cell, f.op, f.rhs)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func readCell(chunks map[FieldID]*chunk, c *ColumnDef, row int) Value {
	ck := chunks[c.ID]
	if ck == nil {
		// 旧片段生成于该字段加入之前：历史行取字段创建时声明的默认值；
		// 未声明默认值才是 NULL。这样历史数据取值确定、可复现。
		if c.HasDefault {
			return c.Default
		}
		return nullValue()
	}
	return readCellFromChunk(ck, row)
}

func readCellFromChunk(ck *chunk, row int) Value {
	if ck.isNull(row) {
		return nullValue()
	}
	switch ck.typ {
	case TypeInt64:
		return Value{Type: TypeInt64, Int: ck.ints[row]}
	case TypeFloat64:
		return Value{Type: TypeFloat64, Float: ck.floats[row]}
	case TypeBool:
		return Value{Type: TypeBool, Bool: ck.bools[row] != 0}
	case TypeString:
		return Value{Type: TypeString, Str: ck.strs[row]}
	default:
		return nullValue()
	}
}

func updateAgg(a *aggState, chunks map[FieldID]*chunk, row int) error {
	if a.col == nil {
		// COUNT(*)
		a.count++
		return nil
	}
	ck := chunks[a.col.ID]
	if ck == nil {
		// 旧片段无此字段：历史行取默认值参与列聚合；无默认值视为 NULL 跳过。
		if a.col.HasDefault {
			v := a.col.Default
			aggTakeValue(a, v)
		}
		return nil
	}
	v := readCellFromChunk(ck, row)
	if v.Null {
		return nil
	}
	return aggTakeValue(a, v)
}

func aggTakeValue(a *aggState, v Value) error {
	switch a.spec.Func {
	case AggCount:
		a.count++
	case AggSum, AggAvg:
		f, ok := v.asFloat()
		if !ok {
			return fmt.Errorf("non-numeric value in numeric aggregate for field %d", a.col.ID)
		}
		a.sum += f
		a.count++
	case AggMin:
		if !a.hasMin || compareValues(v, a.min) < 0 {
			a.min = v
			a.hasMin = true
		}
		a.count++
	case AggMax:
		if !a.hasMax || compareValues(v, a.max) > 0 {
			a.max = v
			a.hasMax = true
		}
		a.count++
	}
	return nil
}

func finalizeAgg(a *aggState) AggResult {
	r := AggResult{Func: a.spec.Func, Column: a.spec.Column, Count: a.count}
	switch a.spec.Func {
	case AggCount:
		r.Value = Value{Type: TypeInt64, Int: a.count}
	case AggSum:
		r.Value = Value{Type: TypeFloat64, Float: a.sum}
		if a.col != nil && a.col.Type == TypeInt64 {
			r.Value = Value{Type: TypeInt64, Int: int64(a.sum)}
		}
	case AggAvg:
		if a.count > 0 {
			r.Value = Value{Type: TypeFloat64, Float: a.sum / float64(a.count)}
		} else {
			r.Value = nullValue()
		}
	case AggMin:
		if a.hasMin {
			r.Value = a.min
		} else {
			r.Value = nullValue()
		}
	case AggMax:
		if a.hasMax {
			r.Value = a.max
		} else {
			r.Value = nullValue()
		}
	}
	return r
}

// rhsOrNull 仅为占位辅助，过滤右侧 NULL 在 resolve 阶段已拒绝。
func (f Filter) rhsOrNull() bool { return f.Value.Null }
