package store

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// ColumnStat 单列的区段统计（zone map），用于查询时跳过不可能命中的片段。
// 片段缺列（字段新增前写入的历史数据）不会有对应统计，按全 NULL 处理。
type ColumnStat struct {
	Type    FieldType `json:"type"`
	HasNull bool      `json:"has_null"`
	// Min/Max 为 nil 表示该列在片段中全部为 NULL（列存在但无取值）。
	Min Value `json:"min,omitempty"`
	Max Value `json:"max,omitempty"`
}

// UnmarshalJSON 按列类型精确还原 Min/Max。
//
// 关键正确性要求：区段统计随清单落盘（JSON），重启后读回时必须与写入前
// 完全同值同型，否则裁剪判定会在重启后改变（例如 int64 大整数 > 2^53 经
// encoding/json 默认解码成 float64 会丢精度，导致本应命中的片段被跳过）。
// 因此这里按 Type 显式解码：int64 用 json.Number 精确解析（兼容旧清单的
// 普通 JSON 整数），float64/string/bool 按各自类型解码。
func (st *ColumnStat) UnmarshalJSON(data []byte) error {
	var raw struct {
		Type    FieldType       `json:"type"`
		HasNull bool            `json:"has_null"`
		Min     json.RawMessage `json:"min"`
		Max     json.RawMessage `json:"max"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	st.Type = raw.Type
	st.HasNull = raw.HasNull
	decode := func(b json.RawMessage) (Value, error) {
		if len(b) == 0 || bytes.Equal(b, []byte("null")) {
			return nil, nil
		}
		switch raw.Type {
		case TypeInt64:
			// json.Number 不做 float64 舍入，可精确表示任意 int64。
			var n json.Number
			if err := json.Unmarshal(b, &n); err != nil {
				return nil, err
			}
			if i, err := n.Int64(); err == nil {
				return i, nil
			}
			// 兜底：旧数据若意外写成浮点形式，按 float64 读回（裁剪比较
			// 对 int64/float64 混合比较是精确的，不会因此误判）。
			var f float64
			if err := json.Unmarshal(b, &f); err != nil {
				return nil, err
			}
			return f, nil
		case TypeFloat64:
			var f float64
			if err := json.Unmarshal(b, &f); err != nil {
				return nil, err
			}
			return f, nil
		case TypeString:
			var s string
			if err := json.Unmarshal(b, &s); err != nil {
				return nil, err
			}
			return s, nil
		case TypeBool:
			var bo bool
			if err := json.Unmarshal(b, &bo); err != nil {
				return nil, err
			}
			return bo, nil
		}
		return nil, fmt.Errorf("column stat: unknown field type %q", raw.Type)
	}
	var err error
	if st.Min, err = decode(raw.Min); err != nil {
		return fmt.Errorf("column stat min: %w", err)
	}
	if st.Max, err = decode(raw.Max); err != nil {
		return fmt.Errorf("column stat max: %w", err)
	}
	return nil
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
