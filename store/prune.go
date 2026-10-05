package store

import "fmt"

// fragmentCanMatch 判断片段是否可能满足全部谓词；返回 false 时该片段
// 不可能贡献任何命中行，可以安全跳过（跳过与否查询结果完全一致）。
//
// 正确性要点：
//   - 片段缺列（字段新增前的历史数据，或整理时已删除列）：没有统计 =>
//     该列按全 NULL 处理，任何比较谓词都不可能命中，直接跳过；
//   - 列存在但全 NULL（HasNull 且 Min/Max 为空）：同样跳过任何比较；
//   - 重命名字段：谓词已按快照结构解析为稳定 ID，统计也按 ID 存取；
//   - 谓词字面量与列类型不可比较（如 int64 列与字符串边界）：逐行
//     比较本来就不匹配，跳过是安全的；
//   - Ne 谓词永不跳过（非匹配值之外的行都可能命中）。
func fragmentCanMatch(meta FragmentMeta, preds []boundPred) bool {
	for _, p := range preds {
		st, ok := meta.Stats[p.fieldID]
		if !ok {
			return false // 历史缺列 => 全 NULL
		}
		if !rangeCanMatch(st, p) {
			return false
		}
	}
	return true
}

func rangeCanMatch(st ColumnStat, p boundPred) bool {
	if p.op == OpNe {
		return true
	}
	// 边界不可信（旧清单截断的 float 边界）：拿不准就不跳过，
	// 照常扫描，保证裁剪永不改变查询结果。
	if st.Untrusted {
		return true
	}
	// 全 NULL 列不可能匹配任何比较。
	if st.Min == nil || st.Max == nil {
		return false
	}
	// 边界与列类型不可比较时，逐行比较也必然不匹配。
	lo, loOK := compareValues(st.Min, p.val)
	hi, hiOK := compareValues(st.Max, p.val)
	if !loOK || !hiOK {
		return false
	}
	switch p.op {
	case OpEq:
		// 命中要求边界恰好落在 [Min,Max] 内，且存在非 NULL 取值。
		// lo<=0 表示 Min <= v；hi>=0 表示 Max >= v。
		return lo <= 0 && hi >= 0
	case OpLt:
		// 需要存在 x < v，即 Min < v（lo<0）。
		return lo < 0
	case OpLe:
		// 需要存在 x <= v，即 Min <= v（lo<=0）。
		return lo <= 0
	case OpGt:
		// 需要存在 x > v，即 Max > v（hi>0）。
		return hi > 0
	case OpGe:
		// 需要存在 x >= v，即 Max >= v（hi>=0）。
		return hi >= 0
	}
	// 未知运算符：保守不跳过。
	return true
}

// describePruneDecision 生成可读的裁剪判定（供测试日志输出）。
func describePruneDecision(meta FragmentMeta, preds []boundPred) string {
	for _, p := range preds {
		st, ok := meta.Stats[p.fieldID]
		if !ok {
			return fmt.Sprintf("片段 %s 缺列 id=%d => 跳过(NULL)", meta.Key, p.fieldID)
		}
		if st.Untrusted {
			return fmt.Sprintf("片段 %s 列 id=%d 统计边界不可信 => 保守扫描", meta.Key, p.fieldID)
		}
		if !rangeCanMatch(st, p) {
			return fmt.Sprintf("片段 %s 区段[%v..%v] 与谓词 id=%d op=%d v=%v 不相交 => 跳过",
				meta.Key, st.Min, st.Max, p.fieldID, p.op, p.val)
		}
	}
	return fmt.Sprintf("片段 %s 区段与谓词相交 => 扫描", meta.Key)
}

// fragmentBudgetCost 片段读取在内存预算中占用的字节数：只按实际磁盘
// 字节数计费（被裁剪跳过的片段不读文件、不计费），因此处理少量数据时
// 不会按整表规模占用预算。旧清单缺少 Size 信息时按每行 8 字节保守估算。
func fragmentBudgetCost(meta FragmentMeta) int64 {
	if meta.Size > 0 {
		return meta.Size
	}
	return int64(meta.NumRows) * 8
}
