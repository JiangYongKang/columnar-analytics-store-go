package store

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// ColumnStat 的 JSON 序列化必须保证 Min/Max 无损往返：
// 区段裁剪的正确性依赖统计边界与写入时完全一致，重启后精度丢失
// （如 int64 经 JSON 解码为 float64，|x| > 2^53 时丢精度）会让
// 区段区间“缩水”，导致本可命中的片段被错误跳过、查询结果随重启改变。
//
// 因此 Min/Max 以带类型标签的形式落盘，例如：
//
//	{"type":"int64","has_null":false,"min":{"i64":9007199254740995},"max":{"i64":9007199254740999}}
//
// 解码时同时兼容旧版清单（Min/Max 为裸 JSON 值）：
//   - 旧版整数按 json.Number 精确解析（旧代码写盘时数字文本本身是精确的，
//     精度丢失只发生在旧代码的解码侧，因此磁盘上的旧清单仍可无损恢复）；
//   - 唯一无法恢复的是“旧代码重启后又回写过清单”的场景：此时磁盘文本已是
//     被截断的 float（如 9.007199254740992e+15）。这种边界不可信，
//     标记 Untrusted，裁剪时保守地视为恒可能命中（照常扫描），
//     宁可放弃跳过也绝不改变查询结果。

// statJSON 是 ColumnStat 的落盘形态。
type statJSON struct {
	Type    FieldType       `json:"type"`
	HasNull bool            `json:"has_null"`
	Min     json.RawMessage `json:"min,omitempty"`
	Max     json.RawMessage `json:"max,omitempty"`
}

// MarshalJSON 以带类型标签的形式编码 Min/Max，保证任意取值无损往返。
func (s ColumnStat) MarshalJSON() ([]byte, error) {
	out := statJSON{Type: s.Type, HasNull: s.HasNull}
	var err error
	if out.Min, err = encodeStatValue(s.Min); err != nil {
		return nil, err
	}
	if out.Max, err = encodeStatValue(s.Max); err != nil {
		return nil, err
	}
	return json.Marshal(out)
}

// UnmarshalJSON 还原统计；兼容旧版裸值清单，无法精确还原的边界标记为
// Untrusted（裁剪时保守不跳过），保证重启前后查询结果一致。
func (s *ColumnStat) UnmarshalJSON(data []byte) error {
	var raw statJSON
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&raw); err != nil {
		return err
	}
	s.Type = raw.Type
	s.HasNull = raw.HasNull
	s.Untrusted = false
	s.Min, s.Max = nil, nil
	if len(raw.Min) > 0 {
		v, trusted, err := decodeStatValue(raw.Min, raw.Type)
		if err != nil {
			return fmt.Errorf("decode stat min: %w", err)
		}
		if !trusted {
			s.Untrusted = true
		} else {
			s.Min = v
		}
	}
	if len(raw.Max) > 0 {
		v, trusted, err := decodeStatValue(raw.Max, raw.Type)
		if err != nil {
			return fmt.Errorf("decode stat max: %w", err)
		}
		if !trusted {
			s.Untrusted = true
		} else {
			s.Max = v
		}
	}
	// 任一边界不可信时，整条统计都不得用于跳过判定。
	if s.Untrusted {
		s.Min, s.Max = nil, nil
	}
	return nil
}

// encodeStatValue 把统计边界编码为带类型标签的 JSON。
func encodeStatValue(v Value) (json.RawMessage, error) {
	if v == nil {
		return nil, nil
	}
	switch x := v.(type) {
	case int64:
		return json.Marshal(struct {
			I64 int64 `json:"i64"`
		}{x})
	case float64:
		return json.Marshal(struct {
			F64 float64 `json:"f64"`
		}{x})
	case string:
		return json.Marshal(struct {
			Str string `json:"str"`
		}{x})
	case bool:
		return json.Marshal(struct {
			B bool `json:"b"`
		}{x})
	}
	return nil, fmt.Errorf("unsupported stat value type %T", v)
}

// decodeStatValue 还原统计边界。trusted=false 表示磁盘上的值无法精确
// 还原为列类型的取值（旧版清单被截断的 float 边界），调用方必须保守处理。
func decodeStatValue(raw json.RawMessage, typ FieldType) (v Value, trusted bool, err error) {
	// 新版：带类型标签的对象。
	var tagged struct {
		I64 *json.Number `json:"i64"`
		F64 *float64     `json:"f64"`
		Str *string      `json:"str"`
		B   *bool        `json:"b"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&tagged); err == nil {
		switch {
		case tagged.I64 != nil:
			n, perr := tagged.I64.Int64()
			if perr != nil {
				return nil, false, nil // 截断的 float 文本：不可信
			}
			return n, true, nil
		case tagged.F64 != nil:
			return *tagged.F64, true, nil
		case tagged.Str != nil:
			return *tagged.Str, true, nil
		case tagged.B != nil:
			return *tagged.B, true, nil
		}
	}

	// 旧版：裸 JSON 值。数字按 json.Number 精确解析。
	var legacy any
	dec = json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&legacy); err != nil {
		return nil, false, err
	}
	switch x := legacy.(type) {
	case json.Number:
		if typ == TypeInt64 {
			n, perr := x.Int64()
			if perr != nil {
				// 旧代码回写过的清单：整数边界已被截断为 float 文本，
				// 真实边界不可知 => 不可信。
				return nil, false, nil
			}
			return n, true, nil
		}
		f, perr := x.Float64()
		if perr != nil {
			return nil, false, nil
		}
		return f, true, nil
	case string:
		return x, true, nil
	case bool:
		return x, true, nil
	}
	return nil, false, fmt.Errorf("unexpected stat value json: %s", string(raw))
}
