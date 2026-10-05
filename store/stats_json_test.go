package store

import (
	"encoding/json"
	"math"
	"testing"
)

// TestColumnStatJSONRoundTrip 统计边界经 JSON 序列化必须无损往返，
// 这是“重启后裁剪判定与重启前一致”的基础。
func TestColumnStatJSONRoundTrip(t *testing.T) {
	cases := []ColumnStat{
		{Type: TypeInt64, Min: int64(1) << 53, Max: int64(1)<<53 + 3},
		{Type: TypeInt64, Min: int64(math.MinInt64), Max: int64(math.MaxInt64)},
		{Type: TypeInt64, Min: int64(-9_000_000_000_000_000_000), Max: int64(-1)},
		{Type: TypeInt64, HasNull: true, Min: int64(5), Max: int64(9)},
		{Type: TypeInt64, HasNull: true}, // 全 NULL：Min/Max 为空
		{Type: TypeFloat64, Min: 1.5, Max: 9.007199254740993e15},
		{Type: TypeString, Min: "apple", Max: "zebra"},
		{Type: TypeBool, Min: false, Max: true},
	}
	for _, want := range cases {
		data, err := json.Marshal(want)
		if err != nil {
			t.Fatalf("marshal %+v: %v", want, err)
		}
		var got ColumnStat
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("unmarshal %s: %v", data, err)
		}
		t.Logf("往返: %+v -> %s -> %+v", want, data, got)
		if got.Untrusted {
			t.Fatalf("%s: 新版编码不应出现不可信边界", data)
		}
		if got.Type != want.Type || got.HasNull != want.HasNull {
			t.Fatalf("%s: type/hasnull 不一致: %+v", data, got)
		}
		if (got.Min == nil) != (want.Min == nil) || (got.Max == nil) != (want.Max == nil) {
			t.Fatalf("%s: nil 形态不一致: %+v", data, got)
		}
		if got.Min != nil && got.Min != want.Min {
			t.Fatalf("%s: Min 精度丢失: got %v (%T), want %v (%T)", data, got.Min, got.Min, want.Min, want.Min)
		}
		if got.Max != nil && got.Max != want.Max {
			t.Fatalf("%s: Max 精度丢失: got %v (%T), want %v (%T)", data, got.Max, got.Max, want.Max, want.Max)
		}
	}
}

// TestColumnStatLegacyManifest 旧版清单（裸 JSON 值）的兼容：
//   - 旧代码写盘的整数文本本身精确，必须无损恢复；
//   - 旧代码重启后回写过的清单（边界已被截断为 float 文本）无法精确还原，
//     必须标记不可信并让裁剪保守不跳过。
func TestColumnStatLegacyManifest(t *testing.T) {
	// 旧版精确整数文本：必须恢复为精确 int64。
	legacy := `{"type":"int64","has_null":false,"min":9007199254740995,"max":9007199254740999}`
	var st ColumnStat
	if err := json.Unmarshal([]byte(legacy), &st); err != nil {
		t.Fatal(err)
	}
	t.Logf("旧版精确整数: %s -> %+v", legacy, st)
	if st.Untrusted || st.Min != int64(9007199254740995) || st.Max != int64(9007199254740999) {
		t.Fatalf("旧版精确整数应无损恢复, got %+v", st)
	}

	// 旧代码回写过的清单：边界已被截断为 float 文本，真实值不可知。
	truncated := `{"type":"int64","has_null":false,"min":9.007199254740992e+15,"max":9.007199254741e+15}`
	var st2 ColumnStat
	if err := json.Unmarshal([]byte(truncated), &st2); err != nil {
		t.Fatal(err)
	}
	t.Logf("旧版截断边界: %s -> %+v", truncated, st2)
	if !st2.Untrusted {
		t.Fatalf("截断的 float 边界必须标记不可信, got %+v", st2)
	}
	// 不可信边界：任何谓词都不得据此跳过片段。
	meta := FragmentMeta{Key: "k", Stats: map[int]ColumnStat{1: st2}}
	bp := boundPred{fieldID: 1, op: OpLt, val: int64(9007199254740996)}
	if !fragmentCanMatch(meta, []boundPred{bp}) {
		t.Fatalf("不可信统计不得用于跳过片段")
	}
	t.Logf("不可信统计的裁剪判定: %s", describePruneDecision(meta, []boundPred{bp}))
}
