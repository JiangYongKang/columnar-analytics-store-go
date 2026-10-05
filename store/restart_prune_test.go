package store

import (
	"context"
	"math"
	"testing"
)

// reopenStore 关闭并重新打开同一目录的存储，模拟进程重启。
func reopenStore(t *testing.T, dir string, s *Store) *Store {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { s2.Close() })
	return s2
}

// TestRestartPruneBigInt 复现：大整数（>2^53）写入后重启，区段统计经
// JSON 落盘再读回，带范围过滤的裁剪查询结果必须与完整扫描一致。
func TestRestartPruneBigInt(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	sc := mustCreate(t, s, "t", Field{Name: "v", Type: TypeInt64})
	fid := sc.FieldByName("v").ID

	// 三个片段：大正数（>2^53，float64 无法精确表示）、大负数、小整数。
	bigPos := []int64{9007199254740993, 9007199254740995, 18014398509481987} // 2^53+1, +3, 2^54+3
	bigNeg := []int64{-9007199254740993, -9007199254740997}
	small := []int64{1, 2, 3}
	write := func(key string, vals ...int64) {
		cols := map[int][]Value{fid: {}}
		for _, v := range vals {
			cols[fid] = append(cols[fid], v)
		}
		if err := s.WriteFragment("t", sc.Version, &Fragment{
			Key: key, NumRows: len(vals), Columns: cols,
		}); err != nil {
			t.Fatal(err)
		}
	}
	write("bigpos", bigPos...)
	write("bigneg", bigNeg...)
	write("small", small...)
	t.Logf("输入: bigpos=%v bigneg=%v small=%v", bigPos, bigNeg, small)

	preds := []Predicate{
		{Field: "v", Op: OpEq, Value: int64(9007199254740993)},
		{Field: "v", Op: OpGe, Value: int64(9007199254740994)},
		{Field: "v", Op: OpLt, Value: int64(-9007199254740993)},
		{Field: "v", Op: OpGe, Value: int64(0)},
	}

	checkAll := func(label string, st *Store) {
		t.Helper()
		sn, err := st.Snapshot("t")
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range preds {
			f := sn.schema.FieldByName(p.Field)
			bp := []boundPred{{fieldID: f.ID, op: p.Op, val: p.Value}}
			for _, m := range sn.Fragments() {
				t.Logf("%s 谓词 v op=%d %v: %s", label, p.Op, p.Value, describePruneDecision(m, bp))
			}
			q := &Query{Predicates: []Predicate{p}, Aggregates: []Aggregate{{Kind: AggCount}}}
			res, err := Execute(context.Background(), sn, q, nil)
			if err != nil {
				t.Fatalf("%s: %v", label, err)
			}
			want := fullScanCount(sn, q.Predicates)
			t.Logf("%s 谓词 v op=%d %v: 裁剪 count=%v 完整扫描=%d", label, p.Op, p.Value, res.Values[0], want)
			if res.Values[0] != want {
				t.Fatalf("%s: 裁剪结果 %v 与完整扫描 %d 不一致", label, res.Values[0], want)
			}
		}
	}

	checkAll("重启前", s)
	s2 := reopenStore(t, dir, s)
	checkAll("重启后", s2)
}

// TestRestartPruneIntBoundaries int64 边界值（MaxInt64/MinInt64/±2^53 附近）
// 在重启前后裁剪判定与结果都必须一致。
func TestRestartPruneIntBoundaries(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	sc := mustCreate(t, s, "t", Field{Name: "v", Type: TypeInt64})
	fid := sc.FieldByName("v").ID

	fragVals := map[string][]int64{
		"max":   {math.MaxInt64, math.MaxInt64 - 1},
		"min":   {math.MinInt64, math.MinInt64 + 1},
		"edge":  {1<<53 - 1, 1 << 53, 1<<53 + 1},
		"small": {0, 7},
	}
	for key, vals := range fragVals {
		cols := map[int][]Value{fid: {}}
		for _, v := range vals {
			cols[fid] = append(cols[fid], v)
		}
		if err := s.WriteFragment("t", sc.Version, &Fragment{
			Key: key, NumRows: len(vals), Columns: cols,
		}); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("输入: %v", fragVals)

	preds := []Predicate{
		{Field: "v", Op: OpEq, Value: int64(math.MaxInt64)},
		{Field: "v", Op: OpEq, Value: int64(math.MinInt64)},
		{Field: "v", Op: OpGt, Value: int64(math.MaxInt64 - 1)},
		{Field: "v", Op: OpLe, Value: int64(math.MinInt64 + 1)},
		{Field: "v", Op: OpEq, Value: int64(1<<53 + 1)},
		{Field: "v", Op: OpGt, Value: int64(1 << 53)},
		{Field: "v", Op: OpGe, Value: int64(0)},
	}
	checkAll := func(label string, st *Store) {
		t.Helper()
		sn, err := st.Snapshot("t")
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range preds {
			q := &Query{Predicates: []Predicate{p}, Aggregates: []Aggregate{
				{Kind: AggCount}, {Kind: AggMin, Field: "v"}, {Kind: AggMax, Field: "v"},
			}}
			res, err := Execute(context.Background(), sn, q, nil)
			if err != nil {
				t.Fatalf("%s: %v", label, err)
			}
			want := fullScanCount(sn, q.Predicates)
			t.Logf("%s 谓词 v op=%d %v: 裁剪 count=%v min=%v max=%v 完整扫描=%d",
				label, p.Op, p.Value, res.Values[0], res.Values[1], res.Values[2], want)
			if res.Values[0] != want {
				t.Fatalf("%s: 裁剪 count=%v 与完整扫描 %d 不一致", label, res.Values[0], want)
			}
		}
	}
	checkAll("重启前", s)
	s2 := reopenStore(t, dir, s)
	checkAll("重启后", s2)
}

// TestRestartPruneSchemaEvolution 缺列（字段新增前的历史数据）、显式 NULL、
// 字段改名/删除后的历史数据，在重启前后裁剪结果与聚合都必须与完整扫描一致。
func TestRestartPruneSchemaEvolution(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	sc := mustCreate(t, s, "t", Field{Name: "id", Type: TypeInt64})
	idID := sc.FieldByName("id").ID

	const (
		bigA = int64(9007199254740997)  // 2^53+5
		bigB = int64(18014398509481987) // 2^54+3
		bigN = int64(-9007199254740999) // -(2^53+7)
	)
	writeFrag := func(ver int, key string, cols map[int][]Value, n int) {
		t.Helper()
		if err := s.WriteFragment("t", ver, &Fragment{Key: key, NumRows: n, Columns: cols}); err != nil {
			t.Fatal(err)
		}
	}
	// 历史片段：只有 id（新字段 n 出现之前写入 => 缺列）。
	writeFrag(sc.Version, "old", map[int][]Value{idID: {bigA, bigB, bigN}}, 3)

	sc2, err := s.EvolveSchema("t", sc.Version, SchemaChange{
		Kind: ChangeAddField, Field: Field{Name: "n", Type: TypeInt64},
	})
	if err != nil {
		t.Fatal(err)
	}
	nID := sc2.FieldByName("n").ID
	// 新片段：n 含大整数与显式 NULL。
	writeFrag(sc2.Version, "new", map[int][]Value{
		idID: {int64(10), int64(20)},
		nID:  {bigA, nil},
	}, 2)

	// 改名 n -> m 后再写一片。
	sc3, err := s.EvolveSchema("t", sc2.Version, SchemaChange{
		Kind: ChangeRenameField, OldName: "n", NewName: "m",
	})
	if err != nil {
		t.Fatal(err)
	}
	writeFrag(sc3.Version, "newer", map[int][]Value{
		idID: {int64(30)},
		nID:  {bigN},
	}, 1)
	t.Logf("输入: old.id=[%d %d %d](缺m列) new=(id=10,m=%d),(id=20,m=NULL) newer=(id=30,m=%d)",
		bigA, bigB, bigN, bigA, bigN)

	type wantAgg struct {
		count int64
		sumID float64
	}
	cases := []struct {
		name string
		pred Predicate
		want wantAgg
	}{
		{"等大整数", Predicate{Field: "m", Op: OpEq, Value: bigA}, wantAgg{1, 10}},
		{"大整数下界", Predicate{Field: "m", Op: OpGe, Value: int64(9007199254740992)}, wantAgg{1, 10}},
		{"负数", Predicate{Field: "m", Op: OpLt, Value: int64(0)}, wantAgg{1, 30}},
		{"大负数上界", Predicate{Field: "m", Op: OpLe, Value: bigN}, wantAgg{1, 30}},
		{"非负(排除NULL)", Predicate{Field: "m", Op: OpGe, Value: int64(0)}, wantAgg{1, 10}},
	}
	check := func(label string, st *Store) {
		t.Helper()
		sn, err := st.Snapshot("t")
		if err != nil {
			t.Fatal(err)
		}
		mF := sn.schema.FieldByName("m")
		for _, c := range cases {
			bp := []boundPred{{fieldID: mF.ID, op: c.pred.Op, val: c.pred.Value}}
			for _, meta := range sn.Fragments() {
				t.Logf("%s %s: %s", label, c.name, describePruneDecision(meta, bp))
			}
			q := &Query{
				Predicates: []Predicate{c.pred},
				Aggregates: []Aggregate{{Kind: AggCount}, {Kind: AggSum, Field: "id"}},
			}
			res, err := Execute(context.Background(), sn, q, nil)
			if err != nil {
				t.Fatalf("%s %s: %v", label, c.name, err)
			}
			wantCnt := fullScanCount(sn, q.Predicates)
			t.Logf("%s %s: 裁剪 count=%v sum(id)=%v 完整扫描=%d 期望=%+v",
				label, c.name, res.Values[0], res.Values[1], wantCnt, c.want)
			if res.Values[0] != wantCnt || res.Values[0] != c.want.count {
				t.Fatalf("%s %s: count=%v 完整扫描=%d 期望=%d",
					label, c.name, res.Values[0], wantCnt, c.want.count)
			}
			if res.Values[1] != c.want.sumID {
				t.Fatalf("%s %s: sum(id)=%v 期望=%v", label, c.name, res.Values[1], c.want.sumID)
			}
		}
	}
	check("重启前", s)
	s2 := reopenStore(t, dir, s)
	check("重启后", s2)

	// 删除字段 m：历史数据保留但不可见；对剩余列的过滤在重启前后仍一致。
	sc4, err := s2.EvolveSchema("t", sc3.Version, SchemaChange{
		Kind: ChangeDropField, OldName: "m",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = sc4
	checkID := func(label string, st *Store) {
		t.Helper()
		sn, err := st.Snapshot("t")
		if err != nil {
			t.Fatal(err)
		}
		q := &Query{
			Predicates: []Predicate{{Field: "id", Op: OpGe, Value: int64(9007199254740992)}},
			Aggregates: []Aggregate{{Kind: AggCount}, {Kind: AggMax, Field: "id"}},
		}
		res, err := Execute(context.Background(), sn, q, nil)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		wantCnt := fullScanCount(sn, q.Predicates)
		t.Logf("%s 删列后 id>=2^53: 裁剪 count=%v max=%v 完整扫描=%d",
			label, res.Values[0], res.Values[1], wantCnt)
		if res.Values[0] != wantCnt || wantCnt != 2 {
			t.Fatalf("%s: count=%v 完整扫描=%d (期望 2)", label, res.Values[0], wantCnt)
		}
		if res.Values[1] != float64(bigB) {
			t.Fatalf("%s: max(id)=%v 期望 %v", label, res.Values[1], float64(bigB))
		}
	}
	checkID("删列后重启前", s2)
	s3 := reopenStore(t, dir, s2)
	checkID("删列后重启后", s3)

	// 已删除字段上的谓词必须明确失败（重启前后一致），不能静默返回空结果。
	sn, _ := s3.Snapshot("t")
	_, err = Execute(context.Background(), sn, &Query{
		Predicates: []Predicate{{Field: "m", Op: OpEq, Value: int64(1)}},
		Aggregates: []Aggregate{{Kind: AggCount}},
	}, nil)
	t.Logf("删列后查询 m: code=%q", CodeOf(err))
	if CodeOf(err) != ErrCodeSchemaIncompatible {
		t.Fatalf("删列后查询应返回 SCHEMA_INCOMPATIBLE, got %v", err)
	}
}
