package store

import (
	"context"
	"math"
	"testing"
)

// TestPruneConsistentAfterRestartBigInt 复现：大整数（>2^53）写入后重启，
// 区段统计经清单 JSON 往返精度丢失，导致重启后裁剪错误跳过片段。
// 重启前后、裁剪与完整扫描的结果必须完全一致。
func TestPruneConsistentAfterRestartBigInt(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	sc := mustCreate(t, s, "t", Field{Name: "id", Type: TypeInt64})
	idID := sc.FieldByName("id").ID
	// 2^53 附近无法被 float64 精确表示的取值。
	const base = int64(1) << 53
	vals := []int64{base + 3, base + 5, base + 7}
	if err := s.WriteFragment("t", sc.Version, &Fragment{
		Key: "big", NumRows: len(vals),
		Columns: map[int][]Value{idID: {vals[0], vals[1], vals[2]}},
	}); err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: %v", vals)

	pred := Predicate{Field: "id", Op: OpLt, Value: base + 4} // 命中 base+3
	check := func(label string, s *Store) {
		t.Helper()
		sn, err := s.Snapshot("t")
		if err != nil {
			t.Fatal(err)
		}
		f := sn.Schema().FieldByName("id")
		bp := boundPred{fieldID: f.ID, op: pred.Op, val: pred.Value}
		for _, m := range sn.Fragments() {
			t.Logf("%s 裁剪判定: %s (stats=%+v)", label, describePruneDecision(m, []boundPred{bp}), m.Stats)
		}
		res, err := Execute(context.Background(), sn, &Query{
			Predicates: []Predicate{pred}, Aggregates: []Aggregate{{Kind: AggCount}},
		}, nil)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		want := fullScanCount(sn, []Predicate{pred})
		t.Logf("%s 结果: 裁剪 count=%v 完整扫描=%d", label, res.Values[0], want)
		if res.Values[0] != want || want != 1 {
			t.Fatalf("%s: 裁剪结果 %v 与完整扫描 %d 不一致", label, res.Values[0], want)
		}
	}
	check("重启前", s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	check("重启后", s2)
}

// fullScanAggs 不做任何裁剪的参考聚合结果（逐片段逐行计算）。
func fullScanAggs(sn *Snapshot, preds []Predicate, aggs []Aggregate) []Value {
	bound := make([]boundPred, 0, len(preds))
	for _, p := range preds {
		f := sn.schema.FieldByName(p.Field)
		bound = append(bound, boundPred{fieldID: f.ID, op: p.Op, val: p.Value})
	}
	states := make([]*aggState, len(aggs))
	for i, a := range aggs {
		st := &aggState{kind: a.Kind, fieldID: -1}
		if a.Kind != AggCount || a.Field != "" {
			st.fieldID = sn.schema.FieldByName(a.Field).ID
		}
		states[i] = st
	}
	for _, meta := range sn.fragments {
		frag, err := sn.store.loadFragment(sn.table, meta)
		if err != nil {
			panic(err)
		}
		for r := 0; r < frag.NumRows; r++ {
			if !matchAll(frag, bound, r) {
				continue
			}
			for _, st := range states {
				st.add(frag, r)
			}
		}
	}
	out := make([]Value, len(states))
	for i, st := range states {
		out[i] = st.final()
	}
	return out
}

// logPruneDecisions 输出每个片段的裁剪判定与依据（测试日志）。
func logPruneDecisions(t *testing.T, label string, sn *Snapshot, preds []Predicate) {
	t.Helper()
	bound := make([]boundPred, 0, len(preds))
	for _, p := range preds {
		f := sn.schema.FieldByName(p.Field)
		bound = append(bound, boundPred{fieldID: f.ID, op: p.Op, val: p.Value})
	}
	for _, m := range sn.Fragments() {
		t.Logf("%s 裁剪判定: %s", label, describePruneDecision(m, bound))
	}
}

// TestPruneConsistentAfterRestartIntBoundaries 覆盖大正数、大负数与
// int64 边界值：重启前后，裁剪执行的过滤+聚合结果必须与完整扫描一致。
func TestPruneConsistentAfterRestartIntBoundaries(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	sc := mustCreate(t, s, "t", Field{Name: "id", Type: TypeInt64})
	idID := sc.FieldByName("id").ID
	const p53 = int64(1) << 53
	fragVals := map[string][]int64{
		"bigpos": {p53 + 3, p53 + 5, 9_000_000_000_000_000_000, 1_000_000_000_000_000_001},
		"bigneg": {-p53 - 3, -p53 - 5, -9_000_000_000_000_000_000},
		"limits": {math.MaxInt64, math.MinInt64, 0},
		"small":  {1, 2, 3},
	}
	for key, vals := range fragVals {
		cols := map[int][]Value{idID: {}}
		for _, v := range vals {
			cols[idID] = append(cols[idID], v)
		}
		if err := s.WriteFragment("t", sc.Version, &Fragment{
			Key: key, NumRows: len(vals), Columns: cols,
		}); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("输入: %v", fragVals)

	preds := []Predicate{
		{Field: "id", Op: OpEq, Value: int64(9_000_000_000_000_000_000)},
		{Field: "id", Op: OpLt, Value: p53 + 4},
		{Field: "id", Op: OpLe, Value: -p53 - 3},
		{Field: "id", Op: OpGt, Value: -p53 - 4},
		{Field: "id", Op: OpGe, Value: int64(9_000_000_000_000_000_000)},
		{Field: "id", Op: OpEq, Value: int64(math.MaxInt64)},
		{Field: "id", Op: OpEq, Value: int64(math.MinInt64)},
		{Field: "id", Op: OpLt, Value: int64(math.MinInt64 + 1)},
		{Field: "id", Op: OpGt, Value: int64(math.MaxInt64 - 1)},
		{Field: "id", Op: OpGe, Value: p53 + 5},
		{Field: "id", Op: OpLe, Value: int64(2)},
	}
	aggs := []Aggregate{
		{Kind: AggCount},
		{Kind: AggSum, Field: "id"},
		{Kind: AggMin, Field: "id"},
		{Kind: AggMax, Field: "id"},
	}

	check := func(label string, st *Store) {
		t.Helper()
		sn, err := st.Snapshot("t")
		if err != nil {
			t.Fatal(err)
		}
		for _, pred := range preds {
			logPruneDecisions(t, label, sn, []Predicate{pred})
			res, err := Execute(context.Background(), sn, &Query{
				Predicates: []Predicate{pred}, Aggregates: aggs,
			}, nil)
			if err != nil {
				t.Fatalf("%s pred=%+v: %v", label, pred, err)
			}
			want := fullScanAggs(sn, []Predicate{pred}, aggs)
			t.Logf("%s pred=%+v 裁剪=%v 完整扫描=%v", label, pred, res.Values, want)
			for i := range want {
				if res.Values[i] != want[i] {
					t.Fatalf("%s pred=%+v: 裁剪聚合[%d]=%v 与完整扫描 %v 不一致",
						label, pred, i, res.Values[i], want[i])
				}
			}
		}
	}
	check("重启前", s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	check("重启后", s2)
}

// TestPruneConsistentAfterRestartSchemaEvolution 缺列（字段新增前的历史数据）、
// 显式 NULL、改名/删除后的历史回读：重启前后裁剪结果都必须与完整扫描一致。
func TestPruneConsistentAfterRestartSchemaEvolution(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	sc := mustCreate(t, s, "t", Field{Name: "id", Type: TypeInt64})
	idID := sc.FieldByName("id").ID
	const p53 = int64(1) << 53

	write := func(key string, ver int, cols map[int][]Value, n int) {
		t.Helper()
		if err := s.WriteFragment("t", ver, &Fragment{Key: key, NumRows: n, Columns: cols}); err != nil {
			t.Fatal(err)
		}
	}
	// 字段新增前的历史片段：只有 id 列（缺 region 列），含大整数。
	write("hist", sc.Version, map[int][]Value{idID: {p53 + 3, p53 + 5}}, 2)

	sc2, err := s.EvolveSchema("t", sc.Version, SchemaChange{
		Kind: ChangeAddField, Field: Field{Name: "region", Type: TypeString},
	})
	if err != nil {
		t.Fatal(err)
	}
	regionID := sc2.FieldByName("region").ID
	// 新片段：region 含 "us"/显式 NULL，id 含大负数。
	write("new1", sc2.Version, map[int][]Value{
		idID:     {-p53 - 3, int64(7)},
		regionID: {"us", nil},
	}, 2)
	// id 全 NULL 的片段（列存在但无取值）。
	write("nulls", sc2.Version, map[int][]Value{
		idID:     {nil, nil},
		regionID: {"eu", "us"},
	}, 2)

	// 改名 region -> zone，记录改名前版本用于历史回读。
	snPre, err := s.Snapshot("t")
	if err != nil {
		t.Fatal(err)
	}
	preRenameV := snPre.Version()
	sc3, err := s.EvolveSchema("t", sc2.Version, SchemaChange{
		Kind: ChangeRenameField, OldName: "region", NewName: "zone",
	})
	if err != nil {
		t.Fatal(err)
	}

	queries := []struct {
		label string
		q     *Query
	}{
		{"大整数等值", &Query{
			Predicates: []Predicate{{Field: "id", Op: OpEq, Value: p53 + 3}},
			Aggregates: []Aggregate{{Kind: AggCount}, {Kind: AggSum, Field: "id"}},
		}},
		{"大负数范围", &Query{
			Predicates: []Predicate{{Field: "id", Op: OpLe, Value: -p53 - 3}},
			Aggregates: []Aggregate{{Kind: AggCount}, {Kind: AggMin, Field: "id"}},
		}},
		{"改名后字段等值", &Query{
			Predicates: []Predicate{{Field: "zone", Op: OpEq, Value: "us"}},
			Aggregates: []Aggregate{{Kind: AggCount}},
		}},
		{"组合谓词", &Query{
			Predicates: []Predicate{
				{Field: "zone", Op: OpEq, Value: "us"},
				{Field: "id", Op: OpGt, Value: int64(0)},
			},
			Aggregates: []Aggregate{{Kind: AggCount}, {Kind: AggMax, Field: "id"}},
		}},
	}

	// checkPreDrop 校验删列前的全部查询（含历史回读）在裁剪与完整扫描下一致。
	checkPreDrop := func(label string, st *Store) {
		t.Helper()
		sn, err := st.Snapshot("t")
		if err != nil {
			t.Fatal(err)
		}
		for _, qc := range queries {
			logPruneDecisions(t, label+"/"+qc.label, sn, qc.q.Predicates)
			res, err := Execute(context.Background(), sn, qc.q, nil)
			if err != nil {
				t.Fatalf("%s/%s: %v", label, qc.label, err)
			}
			want := fullScanAggs(sn, qc.q.Predicates, qc.q.Aggregates)
			t.Logf("%s/%s 裁剪=%v 完整扫描=%v", label, qc.label, res.Values, want)
			for i := range want {
				if res.Values[i] != want[i] {
					t.Fatalf("%s/%s: 裁剪聚合[%d]=%v 与完整扫描 %v 不一致",
						label, qc.label, i, res.Values[i], want[i])
				}
			}
		}
		// 历史回读：改名前版本按旧名 region 查询，结果同样一致。
		snOld, err := st.SnapshotAt("t", preRenameV)
		if err != nil {
			t.Fatalf("%s 历史回读: %v", label, err)
		}
		qOld := &Query{
			Predicates: []Predicate{{Field: "region", Op: OpEq, Value: "us"}},
			Aggregates: []Aggregate{{Kind: AggCount}},
		}
		resOld, err := Execute(context.Background(), snOld, qOld, nil)
		if err != nil {
			t.Fatalf("%s 历史回读执行: %v", label, err)
		}
		wantOld := fullScanAggs(snOld, qOld.Predicates, qOld.Aggregates)
		t.Logf("%s 历史回读 region=us 裁剪=%v 完整扫描=%v", label, resOld.Values, wantOld)
		for i := range wantOld {
			if resOld.Values[i] != wantOld[i] {
				t.Fatalf("%s 历史回读: 裁剪=%v 完整扫描=%v 不一致", label, resOld.Values, wantOld)
			}
		}
	}

	// checkPostDrop 校验删列后：查询被删字段明确失败；按 id 的过滤仍一致。
	checkPostDrop := func(label string, st *Store) {
		t.Helper()
		sn, err := st.Snapshot("t")
		if err != nil {
			t.Fatal(err)
		}
		_, err = Execute(context.Background(), sn, &Query{
			Predicates: []Predicate{{Field: "zone", Op: OpEq, Value: "us"}},
			Aggregates: []Aggregate{{Kind: AggCount}},
		}, nil)
		t.Logf("%s 删除字段后查询 zone -> code=%q", label, CodeOf(err))
		if CodeOf(err) != ErrCodeSchemaIncompatible {
			t.Fatalf("%s 删除字段查询应返回 SCHEMA_INCOMPATIBLE, got %v", label, err)
		}
		qID := &Query{
			Predicates: []Predicate{{Field: "id", Op: OpGe, Value: p53}},
			Aggregates: []Aggregate{{Kind: AggCount}},
		}
		logPruneDecisions(t, label+"/删列后id过滤", sn, qID.Predicates)
		resID, err := Execute(context.Background(), sn, qID, nil)
		if err != nil {
			t.Fatalf("%s 删列后 id 查询: %v", label, err)
		}
		wantID := fullScanAggs(sn, qID.Predicates, qID.Aggregates)
		t.Logf("%s 删列后 id>=2^53 裁剪=%v 完整扫描=%v", label, resID.Values, wantID)
		for i := range wantID {
			if resID.Values[i] != wantID[i] {
				t.Fatalf("%s 删列后: 裁剪=%v 完整扫描=%v 不一致", label, resID.Values, wantID)
			}
		}
	}

	checkPreDrop("重启前", s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	checkPreDrop("重启后", s2)

	// 删除字段（重启后删），再次重启后语义保持不变。
	if _, err := s2.EvolveSchema("t", sc3.Version, SchemaChange{
		Kind: ChangeDropField, OldName: "zone",
	}); err != nil {
		t.Fatal(err)
	}
	checkPostDrop("删列后", s2)
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	checkPostDrop("删列并重启后", s3)
}
