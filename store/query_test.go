package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// setupQueryTable 建表并写入两个片段，返回存储与字段 ID。
func setupQueryTable(t *testing.T, dir string) (*Store, int, int) {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	sc := mustCreate(t, s, "t",
		Field{Name: "id", Type: TypeInt64},
		Field{Name: "amount", Type: TypeFloat64},
	)
	idID := sc.FieldByName("id").ID
	amtID := sc.FieldByName("amount").ID
	mk := func(key string, ids []int64, amts []float64) {
		cols := map[int][]Value{idID: {}, amtID: {}}
		for i := range ids {
			cols[idID] = append(cols[idID], ids[i])
			cols[amtID] = append(cols[amtID], amts[i])
		}
		if err := s.WriteFragment("t", int(sc.Version), &Fragment{
			Key: key, NumRows: len(ids), Columns: cols,
		}); err != nil {
			t.Fatalf("write %s: %v", key, err)
		}
	}
	mk("p0", []int64{1, 2, 3}, []float64{10, 20, 30})
	mk("p1", []int64{4, 5}, []float64{40, 50})
	return s, idID, amtID
}

// TestQueryFilterAggregate 过滤 + 聚合的正确性。
func TestQueryFilterAggregate(t *testing.T) {
	s, _, _ := setupQueryTable(t, t.TempDir())
	sn, _ := s.Snapshot("t")

	q := &Query{
		Predicates: []Predicate{{Field: "id", Op: OpGt, Value: int64(2)}},
		Aggregates: []Aggregate{
			{Kind: AggSum, Field: "amount"},
			{Kind: AggMin, Field: "amount"},
			{Kind: AggMax, Field: "amount"},
			{Kind: AggCount},
		},
	}
	t.Logf("输入: 过滤 id>2, 聚合 sum/min/max/count(amount)")
	res, err := Execute(context.Background(), sn, q, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("结果: %v", res.Values)
	want := []Value{120.0, 30.0, 50.0, int64(3)}
	for i := range want {
		if res.Values[i] != want[i] {
			t.Fatalf("agg[%d]=%v, want %v", i, res.Values[i], want[i])
		}
	}
}

// TestQueryCorruptFragment 片段文件损坏时返回 CORRUPT_FRAGMENT，
// 而不是静默给出不完整结果。
func TestQueryCorruptFragment(t *testing.T) {
	dir := t.TempDir()
	s, _, _ := setupQueryTable(t, dir)
	sn, _ := s.Snapshot("t")

	// 损坏第二个片段：翻转文件末尾若干字节（破坏 CRC 覆盖区）。
	meta := sn.Fragments()[1]
	path := filepath.Join(dir, "t", meta.File)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xFF
	data[len(data)/2] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: 损坏片段文件 %s", meta.File)

	_, err = Execute(context.Background(), sn, &Query{
		Aggregates: []Aggregate{{Kind: AggSum, Field: "amount"}},
	}, nil)
	t.Logf("错误归一化: code=%q err=%v", CodeOf(err), err)
	if CodeOf(err) != ErrCodeCorruptFragment {
		t.Fatalf("应返回 CORRUPT_FRAGMENT, got %v", err)
	}
}

// TestQueryResourceExhausted 内存预算不足时返回 RESOURCE_EXHAUSTED，
// 且预算足够时同一查询成功 —— 内存占用与命中行数相关。
func TestQueryResourceExhausted(t *testing.T) {
	s, _, _ := setupQueryTable(t, t.TempDir())
	sn, _ := s.Snapshot("t")

	// 命中 3 行的查询：预算 2 行 -> 失败；预算 3 行 -> 成功。
	q := &Query{
		Predicates: []Predicate{{Field: "id", Op: OpGt, Value: int64(2)}},
		Aggregates: []Aggregate{{Kind: AggCount}},
	}
	lim := &MemLimiter{BudgetBytes: 2 * perRowBytes}
	_, err := Execute(context.Background(), sn, q, lim)
	t.Logf("输入: 命中 3 行, 预算 2 行 -> code=%q err=%v", CodeOf(err), err)
	if CodeOf(err) != ErrCodeResourceExhausted {
		t.Fatalf("应返回 RESOURCE_EXHAUSTED, got %v", err)
	}

	lim2 := &MemLimiter{BudgetBytes: 3 * perRowBytes}
	res, err := Execute(context.Background(), sn, q, lim2)
	if err != nil {
		t.Fatalf("预算足够时不应失败: %v", err)
	}
	t.Logf("结果: 预算 3 行 -> count=%v", res.Values[0])
	if res.Values[0] != int64(3) {
		t.Fatalf("count=%v, want 3", res.Values[0])
	}

	// 过滤更严格（命中 1 行）时，小预算即可 —— 内存随命中范围而非整表。
	qNarrow := &Query{
		Predicates: []Predicate{{Field: "id", Op: OpEq, Value: int64(5)}},
		Aggregates: []Aggregate{{Kind: AggCount}},
	}
	lim3 := &MemLimiter{BudgetBytes: 1 * perRowBytes}
	res3, err := Execute(context.Background(), sn, qNarrow, lim3)
	if err != nil {
		t.Fatalf("命中 1 行时小预算应足够: %v", err)
	}
	t.Logf("结果: 命中 1 行, 预算 1 行 -> count=%v (内存与命中范围挂钩)", res3.Values[0])
}

// TestQueryInterrupted 上下文取消时返回 INTERRUPTED。
func TestQueryInterrupted(t *testing.T) {
	s, _, _ := setupQueryTable(t, t.TempDir())
	sn, _ := s.Snapshot("t")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消
	_, err := Execute(ctx, sn, &Query{
		Aggregates: []Aggregate{{Kind: AggCount}},
	}, nil)
	t.Logf("错误归一化: 已取消上下文 -> code=%q err=%v", CodeOf(err), err)
	if CodeOf(err) != ErrCodeInterrupted {
		t.Fatalf("应返回 INTERRUPTED, got %v", err)
	}
}

// TestQueryErrorsAreDistinguishable 同一查询在不同失败模式下
// 返回可区分的错误码。
func TestQueryErrorsAreDistinguishable(t *testing.T) {
	codes := map[ErrCode]bool{}

	// 1) 损坏
	dir1 := t.TempDir()
	s1, _, _ := setupQueryTable(t, dir1)
	sn1, _ := s1.Snapshot("t")
	meta := sn1.Fragments()[0]
	p := filepath.Join(dir1, "t", meta.File)
	d, _ := os.ReadFile(p)
	d[len(d)-2] ^= 0xFF
	os.WriteFile(p, d, 0o644)
	_, err := Execute(context.Background(), sn1, &Query{
		Aggregates: []Aggregate{{Kind: AggCount}}}, nil)
	codes[CodeOf(err)] = true

	// 2) 资源受限
	s2, _, _ := setupQueryTable(t, t.TempDir())
	sn2, _ := s2.Snapshot("t")
	_, err = Execute(context.Background(), sn2, &Query{
		Aggregates: []Aggregate{{Kind: AggCount}}},
		&MemLimiter{BudgetBytes: 0})
	codes[CodeOf(err)] = true

	// 3) 中断
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = Execute(ctx, sn2, &Query{
		Aggregates: []Aggregate{{Kind: AggCount}}}, nil)
	codes[CodeOf(err)] = true

	t.Logf("错误归一化: 三种失败模式 -> %v", codes)
	for _, want := range []ErrCode{ErrCodeCorruptFragment, ErrCodeResourceExhausted, ErrCodeInterrupted} {
		if !codes[want] {
			t.Fatalf("缺少错误码 %s, got %v", want, codes)
		}
	}
}
