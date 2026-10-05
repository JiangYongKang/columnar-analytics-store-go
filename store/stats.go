package store

import "fmt"

// ColumnStat 单列的区段统计（zone map），用于查询时跳过不可能命中的片段。
// 片段缺列（字段新增前写入的历史数据）不会有对应统计，按全 NULL 处理。
type ColumnStat struct {
	Type    FieldType `json:"type"`
	HasNull bool      `json:"has_null"`
	// Min/Max 为 nil 表示该列在片段中全部为 NULL（列存在但无取值）。
	Min Value `json:"min,omitempty"`
	Max Value `json:"max,omitempty"`
	// Untrusted 为 true 表示落盘的边界无法精确还原（旧版清单被截断的
	// float 边界），此时 Min/Max 为空且裁剪必须保守地视为恒可能命中，
	// 绝不据此跳过片段。仅存在于解码后的内存态，不参与序列化。
	Untrusted bool `json:"-"`
}

// computeColumnStats 按当前结构计算片段中各可见列的区段统计。
// 片段中不存在的列（历史缺列、已删除列）不会出现在结果中；
// 列存在但全部为 NULL 时记录 HasNull=true 且 Min/Max 为空。
// 调用前片段已经过 validateFragmentForSchema 校验，类型可信。
func computeColumnStats(f *Fragment, schema *Schema) map[int]ColumnStat {
	stats := make(map[int]ColumnStat)
	for i := range schema.Fields {
		fld := &schema.Fields[i]
		col, ok := f.Columns[fld.ID]
		if !ok {
			continue // 历史片段缺列：不产出统计，裁剪时按全 NULL 处理
		}
		st := ColumnStat{Type: fld.Type}
		switch fld.Type {
		case TypeInt64:
			var mn, mx int64
			first := true
			for _, v := range col {
				if v == nil {
					st.HasNull = true
					continue
				}
				x := v.(int64)
				if first || x < mn {
					mn = x
				}
				if first || x > mx {
					mx = x
				}
				first = false
			}
			if !first {
				st.Min, st.Max = mn, mx
			}
		case TypeFloat64:
			var mn, mx float64
			first := true
			for _, v := range col {
				if v == nil {
					st.HasNull = true
					continue
				}
				x := v.(float64)
				if first || x < mn {
					mn = x
				}
				if first || x > mx {
					mx = x
				}
				first = false
			}
			if !first {
				st.Min, st.Max = mn, mx
			}
		case TypeString:
			var mn, mx string
			first := true
			for _, v := range col {
				if v == nil {
					st.HasNull = true
					continue
				}
				x := v.(string)
				if first || x < mn {
					mn = x
				}
				if first || x > mx {
					mx = x
				}
				first = false
			}
			if !first {
				st.Min, st.Max = mn, mx
			}
		case TypeBool:
			anyFalse, anyTrue := false, false
			for _, v := range col {
				if v == nil {
					st.HasNull = true
					continue
				}
				if v.(bool) {
					anyTrue = true
				} else {
					anyFalse = true
				}
			}
			// 布尔区段：Min=false（存在 false 时），Max=true（存在 true 时）。
			if anyFalse {
				st.Min = false
			}
			if anyTrue {
				st.Max = true
			}
			// 只有 true：Min 也为 true；只有 false：Max 也为 false。
			if anyTrue && !anyFalse {
				st.Min = true
			}
			if anyFalse && !anyTrue {
				st.Max = false
			}
		default:
			panic(fmt.Sprintf("computeColumnStats: unknown field type %q", fld.Type))
		}
		stats[fld.ID] = st
	}
	return stats
}

// estimateCellsBytes 估算片段中当前结构可见列各单元格的内存字节数，
// 用于整理预算（只统计实际搬运的数据，不随整表历史规模膨胀）。
func estimateCellsBytes(f *Fragment, schema *Schema) int64 {
	var n int64
	for i := range schema.Fields {
		col, ok := f.Columns[schema.Fields[i].ID]
		if !ok {
			continue
		}
		for _, v := range col {
			switch x := v.(type) {
			case nil:
				n += 0
			case int64:
				n += 8
			case float64:
				n += 8
			case bool:
				n += 1
			case string:
				n += int64(16 + len(x))
			default:
				n += 16
			}
		}
	}
	return n
}
