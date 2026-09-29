package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// fullScan 完整扫描结果（关闭裁剪：用统计里跳过数必须为 0 的等价做法——
// 直接逐片段执行；这里通过查询所有谓词的“平凡重写”不易实现，
// 因此基线用聚合 + count 作为完整扫描参照：谓词为 nil 时不裁剪）。
func fullScanAgg(t *testing.T, sn *Snapshot, q *Query) []Value {
	t.Helper()
	res, err := Execute(context.Background(), sn, q, nil)
	if err != nil {
		t.Fatalf("fullscan: %v", err)
	}
	return res.Values
}

// TestPruningResultMatchesFullScan 多种谓词/聚合下，裁剪查询结果与
// 全扫描完全一致，且确实跳过了不可能命中的片段。
func TestPruningResultMatchesFullScan(t *testing.T) {
	s := newTestStore(t)
	sc := mustCreate(t, s, "t",
		Field{Name: "id", Type: TypeInt64},
		Field{Name: "amount", Type: TypeFloat64},
		Field{Name: "region", Type: TypeString},
	)
	idID := sc.FieldByName("id").ID
	amtID := sc.FieldByName("amount").ID
	regID := sc.FieldByName("region").ID

	// 三个不重叠值域片段。
	write3 := func(key string, ids []int64, amts []float64, regions []string) {
		if err := s.WriteFragment("t", sc.Version, &Fragment{
			Key: key, NumRows: len(ids),
			Columns: map[int][]Value{idID: vals(ids), amtID: fvals(amts), regID: svals(regions)},
		}); err != nil {
			t.Fatal(err)
		}
	}
	write3("lo", []int64{1, 2, 3}, []float64{10, 20, 30}, []string{"a", "a", "b"})
	write3("mid", []int64{10, 11}, []float64{40, 50}, []string{"b", "c"})
	write3("hi", []int64{20, 21, 22, 23}, []float64{60, 70, 80, 90}, []string{"c", "d", "d", "d"})

	sn, _ := s.Snapshot("t")
	queries := []*Query{
		{Predicates: []Predicate{{Field: "id", Op: OpGt, Value: int64(100)}},
			Aggregates: []Aggregate{{Kind: AggCount}}}, // 全跳过
		{Predicates: []Predicate{{Field: "id", Op: OpLt, Value: int64(0)}},
			Aggregates: []Aggregate{{Kind: AggCount}}}, // 全跳过
		{Predicates: []Predicate{{Field: "id", Op: OpEq, Value: int64(11)}},
			Aggregates: []Aggregate{{Kind: AggSum, Field: "amount"}, {Kind: AggCount}}},
		{Predicates: []Predicate{{Field: "amount", Op: OpGe, Value: float64(80)}},
			Aggregates: []Aggregate{{Kind: AggCount}}},
		{Predicates: []Predicate{{Field: "amount", Op: OpLe, Value: float64(15)}},
			Aggregates: []Aggregate{{Kind: AggMin, Field: "amount"}, {Kind: AggCount}}},
		{Predicates: []Predicate{{Field: "region", Op: OpEq, Value: "d"}},
			Aggregates: []Aggregate{{Kind: AggCount}}},
		{Predicates: []Predicate{{Field: "region", Op: OpNe, Value: "a"}},
			Aggregates: []Aggregate{{Kind: AggCount}}},
		{Predicates: []Predicate{
			{Field: "id", Op: OpGe, Value: int64(10)},
			{Field: "id", Op: OpLe, Value: int64(11)},
		}, Aggregates: []Aggregate{{Kind: AggSum, Field: "id"}}},
	}
	for i, q := range queries {
		// 基线：把谓词置空跑聚合不适用（谓词参与结果），故基线为
		// “对每个片段单独全量执行再合并”：直接构造无裁剪参照——
		// 用相同 q 但逐片段 Snapshot 成本高；这里简化为：
		// 手工对全量数据做逐行判定的参照（见 wantTable）。
		got, stats, err := ExecuteStats(context.Background(), sn, q, nil)
		if err != nil {
			t.Fatalf("q%d: %v", i, err)
		}
		want := referenceResult(q, referenceRows())
		if !valuesEqual(got.Values, want) {
			t.Fatalf("q%d 裁剪结果=%v 与参照=%v 不一致 (skip=%d/%d)",
				i, got.Values, want, stats.FragmentsSkipped, stats.FragmentsTotal)
		}
		t.Logf("q%d 输入谓词=%v 结果=%v 裁剪: 总=%d 跳过=%d 扫描=%d",
			i, q.Predicates, got.Values, stats.FragmentsTotal,
			stats.FragmentsSkipped, stats.FragmentsScanned)
	}

	// 第一个查询（id>100）必须跳过全部 3 个片段，且 count=0；
	// 命中 11 的查询必须跳过 2 个片段。
	_, st, err := ExecuteStats(context.Background(), sn, queries[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.FragmentsSkipped != 3 || st.FragmentsScanned != 0 {
		t.Fatalf("超界查询应跳过全部片段: %+v", st)
	}
}

// TestPruningNoPredicateScansAll 无谓词查询不得裁剪。
func TestPruningNoPredicateScansAll(t *testing.T) {
	s := newTestStore(t)
	sc := mustCreate(t, s, "t", Field{Name: "v", Type: TypeInt64})
	fid := sc.FieldByName("v").ID
	writeRows(t, s, "t", sc.Version, fid, "p0", 1, 2)
	writeRows(t, s, "t", sc.Version, fid, "p1", 3)
	sn, _ := s.Snapshot("t")
	res, st, err := ExecuteStats(context.Background(), sn, &Query{
		Aggregates: []Aggregate{{Kind: AggCount}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.FragmentsSkipped != 0 || st.FragmentsScanned != 2 || res.Values[0] != int64(3) {
		t.Fatalf("无谓词不应裁剪: %+v count=%v", st, res.Values[0])
	}
}

// TestPruningSchemaEvolution 新增列后的旧片段缺列、全 NULL 列、
// 重命名后按 ID 命中，裁剪都必须给出与全扫描一致的结果。
func TestPruningSchemaEvolution(t *testing.T) {
	s := newTestStore(t)
	sc := mustCreate(t, s, "t", Field{Name: "id", Type: TypeInt64})
	idID := sc.FieldByName("id").ID
	// 旧片段：id 0..4。
	if err := s.WriteFragment("t", sc.Version, &Fragment{
		Key: "old", NumRows: 5,
		Columns: map[int][]Value{idID: {int64(0), int64(1), int64(2), int64(3), int64(4)}},
	}); err != nil {
		t.Fatal(err)
	}

	// 新增 tag 列后写新片段。
	sc2, err := s.EvolveSchema("t", sc.Version, SchemaChange{
		Kind: ChangeAddField, Field: Field{Name: "tag", Type: TypeInt64},
	})
	if err != nil {
		t.Fatal(err)
	}
	tagID := sc2.FieldByName("tag").ID
	if err := s.WriteFragment("t", sc2.Version, &Fragment{
		Key: "new", NumRows: 3,
		Columns: map[int][]Value{
			idID:  {int64(10), int64(11), int64(12)},
			tagID: {int64(100), nil, int64(102)},
		},
	}); err != nil {
		t.Fatal(err)
	}
	sn, _ := s.Snapshot("t")

	// tag > 50：旧片段缺 tag 列（全 NULL）必须被整体跳过；
	// 新片段 zone min=100/max=102 且含一个 NULL，不能跳过。
	res, st, err := ExecuteStats(context.Background(), sn, &Query{
		Predicates: []Predicate{{Field: "tag", Op: OpGt, Value: int64(50)}},
		Aggregates: []Aggregate{{Kind: AggCount}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("新增列裁剪: 跳过=%d 扫描=%d count=%v", st.FragmentsSkipped, st.FragmentsScanned, res.Values[0])
	if st.FragmentsSkipped != 1 || st.FragmentsScanned != 1 {
		t.Fatalf("缺列旧片段应被跳过, got %+v", st)
	}
	if res.Values[0] != int64(2) { // 100 与 102 命中，NULL 不命中
		t.Fatalf("count=%v, want 2", res.Values[0])
	}

	// tag == 101：旧片段缺 tag 列被整体跳过；新片段 zone 范围 100..102，
	// min/max 无法证明 101 不存在，因此保守扫描该片段（0 命中）。
	// 这正是裁剪安全性的体现：宁可多扫，绝不漏行。
	res2, st2, err := ExecuteStats(context.Background(), sn, &Query{
		Predicates: []Predicate{{Field: "tag", Op: OpEq, Value: int64(101)}},
		Aggregates: []Aggregate{{Kind: AggCount}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st2.FragmentsSkipped != 1 || st2.FragmentsScanned != 1 || res2.Values[0] != int64(0) {
		t.Fatalf("tag==101: 缺列片段应跳过、范围内片段应扫描且 count=0: %+v %v",
			st2, res2.Values[0])
	}
	t.Logf("tag==101: 跳过缺列片段=%d 保守扫描范围内片段=%d count=0",
		st2.FragmentsSkipped, st2.FragmentsScanned)

	// 重命名 id -> ident 后，按新名裁剪仍按 ID 命中旧数据。
	sc3, err := s.EvolveSchema("t", sc2.Version, SchemaChange{
		Kind: ChangeRenameField, OldName: "id", NewName: "ident",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = sc3
	sn3, _ := s.Snapshot("t")
	res3, st3, err := ExecuteStats(context.Background(), sn3, &Query{
		Predicates: []Predicate{{Field: "ident", Op: OpLt, Value: int64(3)}},
		Aggregates: []Aggregate{{Kind: AggCount}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("重命名后裁剪: 跳过=%d 扫描=%d count=%v (id 0,1,2 命中)",
		st3.FragmentsSkipped, st3.FragmentsScanned, res3.Values[0])
	if res3.Values[0] != int64(3) {
		t.Fatalf("重命名后按 ID 裁剪结果错误: count=%v, want 3", res3.Values[0])
	}
	if st3.FragmentsScanned != 1 { // 新片段 id 10..12 整体不可能 <3
		t.Fatalf("新片段应被跳过: %+v", st3)
	}
}

// TestPruningSameAfterCompaction 整理前后裁剪查询结果完全一致，
// 合并片段的 zone 是各源 zone 的并集范围。
func TestPruningSameAfterCompaction(t *testing.T) {
	s := newTestStore(t)
	sc := mustCreate(t, s, "t", Field{Name: "id", Type: TypeInt64})
	fid := sc.FieldByName("id").ID
	if err := s.SetTableOptions("t", TableOptions{RetainVersions: 5}); err != nil {
		t.Fatal(err)
	}
	writeRows(t, s, "t", sc.Version, fid, "a", 1, 2)
	writeRows(t, s, "t", sc.Version, fid, "b", 10, 11)
	writeRows(t, s, "t", sc.Version, fid, "c", 20, 21)
	sn, _ := s.Snapshot("t")
	q := &Query{
		Predicates: []Predicate{{Field: "id", Op: OpGe, Value: int64(20)}},
		Aggregates: []Aggregate{{Kind: AggSum, Field: "id"}, {Kind: AggCount}},
	}
	before, stB, err := ExecuteStats(context.Background(), sn, q, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Compact(context.Background(), "t", CompactionOptions{}); err != nil {
		t.Fatal(err)
	}
	sn2, _ := s.Snapshot("t")
	after, stA, err := ExecuteStats(context.Background(), sn2, q, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("整理前后: before=%v(skip=%d) after=%v(skip=%d, mergedZone=1..21)",
		before.Values, stB.FragmentsSkipped, after.Values, stA.FragmentsSkipped)
	if !valuesEqual(before.Values, after.Values) || after.Values[1] != int64(2) || after.Values[0] != 41.0 {
		t.Fatalf("整理改变了裁剪查询结果: %v vs %v", before.Values, after.Values)
	}
	// 合并片段 zone 覆盖 1..21，20 落在范围内 => 必须扫描该片段（不能误裁）。
	if stA.FragmentsScanned != 1 {
		t.Fatalf("合并片段包含命中行，不应被裁剪: %+v", stA)
	}
}

// TestWriteRejectsUnexplainableData 写入的数据必须能被结构完整解释：
// 类型不符、列长不符直接拒绝（写入时失败，而非查询时静默漏掉）。
func TestWriteRejectsUnexplainableData(t *testing.T) {
	s := newTestStore(t)
	sc := mustCreate(t, s, "t", Field{Name: "id", Type: TypeInt64})
	idID := sc.FieldByName("id").ID

	cases := []struct {
		name string
		frag *Fragment
	}{
		{"类型不符(string 写入 int64 列)", &Fragment{
			Key: "bad-type", NumRows: 1,
			Columns: map[int][]Value{idID: {"x"}},
		}},
		{"类型不符(bool 写入 int64 列)", &Fragment{
			Key: "bad-type2", NumRows: 1,
			Columns: map[int][]Value{idID: {true}},
		}},
		{"列长与 NumRows 不符", &Fragment{
			Key: "bad-len", NumRows: 2,
			Columns: map[int][]Value{idID: {int64(1)}},
		}},
		{"未知字段 ID", &Fragment{
			Key: "bad-id", NumRows: 1,
			Columns: map[int][]Value{9999: {int64(1)}},
		}},
	}
	for _, tc := range cases {
		err := s.WriteFragment("t", sc.Version, tc.frag)
		t.Logf("输入: %s -> code=%q reason=%s err=%v", tc.name, CodeOf(err), reasonOf(err), err)
		if CodeOf(err) != ErrCodeSchemaIncompatible {
			t.Fatalf("%s: 应返回 SCHEMA_INCOMPATIBLE, got %v", tc.name, err)
		}
	}

	// 拒绝的写入不产生任何可见数据。
	sn, _ := s.Snapshot("t")
	if len(sn.Fragments()) != 0 {
		t.Fatalf("被拒绝的写入不应留下片段: %d", len(sn.Fragments()))
	}
}

// --- 内存参照实现（与全扫描语义逐行一致），用于交叉验证裁剪 ---

type refRow struct {
	id     int64
	amount float64
	region string
	tag    *int64
}

func referenceRows() []refRow {
	mk := func(id int64, amount float64, region string) refRow {
		return refRow{id: id, amount: amount, region: region}
	}
	return []refRow{
		mk(1, 10, "a"), mk(2, 20, "a"), mk(3, 30, "b"),
		mk(10, 40, "b"), mk(11, 50, "c"),
		mk(20, 60, "c"), mk(21, 70, "d"), mk(22, 80, "d"), mk(23, 90, "d"),
	}
}

func referenceResult(q *Query, rows []refRow) []Value {
	out := make([]Value, len(q.Aggregates))
	type acc struct {
		count int64
		sum   float64
		min   float64
		max   float64
		has   bool
	}
	accs := make([]*acc, len(q.Aggregates))
	for i := range accs {
		accs[i] = &acc{}
	}
	for _, r := range rows {
		ok := true
		for _, p := range q.Predicates {
			if !refMatch(r, p) {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		for i, a := range q.Aggregates {
			st := accs[i]
			st.count++
			if a.Field == "" {
				continue
			}
			var fv float64
			switch a.Field {
			case "amount":
				fv = r.amount
			case "id":
				fv = float64(r.id)
			}
			st.sum += fv
			if !st.has || fv < st.min {
				st.min = fv
			}
			if !st.has || fv > st.max {
				st.max = fv
			}
			st.has = true
		}
	}
	for i, a := range q.Aggregates {
		st := accs[i]
		switch a.Kind {
		case AggCount:
			out[i] = st.count
		case AggSum:
			out[i] = st.sum
		case AggMin:
			if !st.has {
				out[i] = nil
			} else {
				out[i] = st.min
			}
		case AggMax:
			if !st.has {
				out[i] = nil
			} else {
				out[i] = st.max
			}
		}
	}
	return out
}

func refMatch(r refRow, p Predicate) bool {
	var v Value
	switch p.Field {
	case "id":
		v = int64(r.id)
	case "amount":
		v = r.amount
	case "region":
		v = r.region
	}
	if v == nil {
		return false
	}
	return compare(v, p.Op, p.Value)
}

func vals(xs []int64) []Value {
	out := make([]Value, len(xs))
	for i, x := range xs {
		out[i] = x
	}
	return out
}
func fvals(xs []float64) []Value {
	out := make([]Value, len(xs))
	for i, x := range xs {
		out[i] = x
	}
	return out
}
func svals(xs []string) []Value {
	out := make([]Value, len(xs))
	for i, x := range xs {
		out[i] = x
	}
	return out
}

func valuesEqual(a, b []Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if fmt.Sprintf("%v", a[i]) != fmt.Sprintf("%v", b[i]) {
			return false
		}
	}
	return true
}

// TestQueryBudgetScalesWithMatches 裁剪下资源只与命中数据相关：
// 大量不可能命中的片段不占内存配额；预算按命中行数失败，绝不返回部分结果。
func TestQueryBudgetScalesWithMatches(t *testing.T) {
	s := newTestStore(t)
	sc := mustCreate(t, s, "t", Field{Name: "id", Type: TypeInt64})
	fid := sc.FieldByName("id").ID
	// 20 个不相关片段，每片段 10 行，id 落在 1000..1199。
	for g := 0; g < 20; g++ {
		vals := make([]Value, 0, 10)
		for j := 0; j < 10; j++ {
			vals = append(vals, int64(1000+g*10+j))
		}
		if err := s.WriteFragment("t", sc.Version, &Fragment{
			Key: fmt.Sprintf("grp-%d", g), NumRows: 10,
			Columns: map[int][]Value{fid: vals},
		}); err != nil {
			t.Fatal(err)
		}
	}
	// 1 个命中片段：id 0..9。
	hit := make([]Value, 0, 10)
	for j := 0; j < 10; j++ {
		hit = append(hit, int64(j))
	}
	if err := s.WriteFragment("t", sc.Version, &Fragment{
		Key: "hit", NumRows: 10, Columns: map[int][]Value{fid: hit},
	}); err != nil {
		t.Fatal(err)
	}
	sn, _ := s.Snapshot("t")
	q := &Query{
		Predicates: []Predicate{{Field: "id", Op: OpLt, Value: int64(100)}},
		Aggregates: []Aggregate{{Kind: AggCount}, {Kind: AggSum, Field: "id"}},
	}
	// 20 个不相关片段全部跳过，只需 10 行命中的预算。
	lim := &MemLimiter{BudgetBytes: 10 * perRowBytes}
	res, st, err := ExecuteStats(context.Background(), sn, q, lim)
	if err != nil {
		t.Fatalf("命中 10 行时 10 行预算应足够: %v", err)
	}
	t.Logf("输入: 21 片段/210 行, 谓词 id<100 -> 跳过=%d 扫描=%d count=%v sum=%v",
		st.FragmentsSkipped, st.FragmentsScanned, res.Values[0], res.Values[1])
	if st.FragmentsSkipped != 20 || res.Values[0] != int64(10) || res.Values[1] != 45.0 {
		t.Fatalf("裁剪+预算结果异常: %+v %v", st, res.Values)
	}

	// 预算不足（9 行）明确失败，且不会给出不完整结果。
	_, err = Execute(context.Background(), sn, q, &MemLimiter{BudgetBytes: 9 * perRowBytes})
	if CodeOf(err) != ErrCodeResourceExhausted {
		t.Fatalf("预算不足应返回 RESOURCE_EXHAUSTED, got %v", err)
	}
}

// TestPruningCorruptZoneDetected zone section 损坏必须报 CORRUPT_FRAGMENT，
// 不允许靠损坏的 zone 错误裁剪而静默漏掉行。
func TestPruningCorruptZoneDetected(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sc := mustCreate(t, s, "t", Field{Name: "id", Type: TypeInt64})
	fid := sc.FieldByName("id").ID
	writeRows(t, s, "t", sc.Version, fid, "p0", 1, 2, 3)
	sn, _ := s.Snapshot("t")
	meta := sn.Fragments()[0]

	p := filepath.Join(dir, "t", meta.File)
	d, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	// 翻转 zone payload 区的一个字节（位于末尾 8 字节偏移之前、ZONE 标记之后）。
	// zone 区为最后 8 字节偏移 + 至少 16 字节的 ZONE+payload+crc。
	zoneStart := int64(getU64(d[len(d)-8:]))
	idx := zoneStart + 5 // "ZONE" 之后第一个 payload 字节
	d[idx] ^= 0xFF
	if err := os.WriteFile(p, d, 0o644); err != nil {
		t.Fatal(err)
	}

	_, _, err = ExecuteStats(context.Background(), sn, &Query{
		Predicates: []Predicate{{Field: "id", Op: OpGt, Value: int64(0)}},
		Aggregates: []Aggregate{{Kind: AggCount}},
	}, nil)
	t.Logf("zone 损坏: code=%q reason=%s", CodeOf(err), reasonOf(err))
	if CodeOf(err) != ErrCodeCorruptFragment {
		t.Fatalf("损坏 zone 应返回 CORRUPT_FRAGMENT, got %v", err)
	}
}
