package store

import (
	"context"
	"fmt"
	"testing"
)

// fullScan 不做裁剪的参考结果（通过让谓词不可能按区段跳过实现，
// 这里直接逐片段逐行统计，作为“完整扫描”基准）。
func fullScanCount(sn *Snapshot, preds []Predicate) int64 {
	var n int64
	bound := make([]boundPred, 0, len(preds))
	for _, p := range preds {
		f := sn.schema.FieldByName(p.Field)
		bound = append(bound, boundPred{fieldID: f.ID, op: p.Op, val: p.Value})
	}
	for _, meta := range sn.fragments {
		frag, err := sn.store.loadFragment(sn.table, meta)
		if err != nil {
			panic(err)
		}
		for r := 0; r < frag.NumRows; r++ {
			if matchAll(frag, bound, r) {
				n++
			}
		}
	}
	return n
}

// TestPruneSkipsDisjointRanges 区段不相交的片段被跳过，
// 结果与完整扫描一致，并验证日志级的跳过判定。
func TestPruneSkipsDisjointRanges(t *testing.T) {
	s, sc := compactTable(t)
	writeRows(t, s, sc, "low", []int64{1, 2, 3})
	writeRows(t, s, sc, "mid", []int64{10, 11, 12})
	writeRows(t, s, sc, "high", []int64{100, 200})

	sn, _ := s.Snapshot("t")
	metas := sn.Fragments()
	pred := Predicate{Field: "id", Op: OpGe, Value: int64(50)}
	f := sn.schema.FieldByName("id")
	bp := boundPred{fieldID: f.ID, op: pred.Op, val: pred.Value}
	skipped := 0
	for _, m := range metas {
		can := fragmentCanMatch(m, []boundPred{bp})
		t.Logf("裁剪判定: %s", describePruneDecision(m, []boundPred{bp}))
		if !can {
			skipped++
		}
	}
	if skipped != 2 {
		t.Fatalf("应跳过 low/mid 两个片段, got %d", skipped)
	}

	q := &Query{Predicates: []Predicate{pred}, Aggregates: []Aggregate{{Kind: AggCount}}}
	res, err := Execute(context.Background(), sn, q, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := fullScanCount(sn, []Predicate{pred})
	t.Logf("结果: 裁剪执行 count=%v 完整扫描 count=%d", res.Values[0], want)
	if res.Values[0] != want || want != 2 {
		t.Fatalf("裁剪结果与完整扫描不一致: %v vs %d", res.Values[0], want)
	}
}

// TestPruneAcrossSchemaEvolution 新增/改名/删除字段、缺列、NULL
// 与整理前后，裁剪结果必须始终与完整扫描一致。
func TestPruneAcrossSchemaEvolution(t *testing.T) {
	s := newTestStore(t)
	sc := mustCreate(t, s, "t", Field{Name: "id", Type: TypeInt64})
	idID := sc.FieldByName("id").ID
	writeID := func(key string, ids ...int64) {
		cols := map[int][]Value{idID: {}}
		for _, v := range ids {
			cols[idID] = append(cols[idID], v)
		}
		if err := s.WriteFragment("t", sc.Version, &Fragment{
			Key: key, NumRows: len(ids), Columns: cols,
		}); err != nil {
			t.Fatal(err)
		}
	}
	writeID("old1", 1, 2) // 新字段出现前的历史数据（缺列）

	sc2, err := s.EvolveSchema("t", sc.Version, SchemaChange{
		Kind: ChangeAddField, Field: Field{Name: "region", Type: TypeString},
	})
	if err != nil {
		t.Fatal(err)
	}
	regionID := sc2.FieldByName("region").ID
	writeBoth := func(key string, regions []string, ids ...int64) {
		cols := map[int][]Value{idID: {}, regionID: {}}
		for i, v := range ids {
			cols[idID] = append(cols[idID], v)
			if i < len(regions) {
				cols[regionID] = append(cols[regionID], regions[i])
			} else {
				cols[regionID] = append(cols[regionID], nil) // 显式 NULL
			}
		}
		if err := s.WriteFragment("t", sc2.Version, &Fragment{
			Key: key, NumRows: len(ids), Columns: cols,
		}); err != nil {
			t.Fatal(err)
		}
	}
	writeBoth("new1", []string{"us", "eu"}, 10, 11)
	writeBoth("new2", []string{"us"}, 20) // 第二行 region 显式 NULL

	check := func(label, field string) {
		t.Helper()
		sn, _ := s.Snapshot("t")
		q := &Query{
			Predicates: []Predicate{{Field: field, Op: OpEq, Value: "us"}},
			Aggregates: []Aggregate{{Kind: AggCount}},
		}
		res, err := Execute(context.Background(), sn, q, nil)
		if err != nil {
			t.Fatalf("%s: execute: %v", label, err)
		}
		want := fullScanCount(sn, q.Predicates)
		// field="us" 只有 new1 第 1 行与 new2 第 1 行 => 2；
		// 历史片段缺列与显式 NULL 行都不能被算入。
		if res.Values[0] != want {
			t.Fatalf("%s: 裁剪 count=%v 与完整扫描 %d 不一致", label, res.Values[0], want)
		}
		t.Logf("%s: %s=us 裁剪 count=%v 完整扫描=%d（缺列/NULL 均未误算）",
			label, field, res.Values[0], want)
	}
	check("整理前", "region")

	// 改名 region -> zone：统计按稳定 ID 存取，裁剪仍应正确。
	sc3, err := s.EvolveSchema("t", sc2.Version, SchemaChange{
		Kind: ChangeRenameField, OldName: "region", NewName: "zone",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = sc3
	snRename, _ := s.Snapshot("t")
	qRename := &Query{
		Predicates: []Predicate{{Field: "zone", Op: OpEq, Value: "eu"}},
		Aggregates: []Aggregate{{Kind: AggCount}},
	}
	res, err := Execute(context.Background(), snRename, qRename, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := fullScanCount(snRename, qRename.Predicates)
	t.Logf("改名后: zone=eu 裁剪 count=%v 完整扫描=%d", res.Values[0], want)
	if res.Values[0] != want || want != 1 {
		t.Fatalf("改名后裁剪错误: %v vs %d", res.Values[0], want)
	}

	// 整理后：合并片段的统计仍正确，结果不变。
	if _, err := s.Compact(context.Background(), "t", nil); err != nil {
		t.Fatal(err)
	}
	check("整理后", "zone")

	// 删除字段后查询该字段应明确失败（而不是静默漏数据）。
	if _, err := s.EvolveSchema("t", sc3.Version, SchemaChange{
		Kind: ChangeDropField, OldName: "zone",
	}); err != nil {
		t.Fatal(err)
	}
	snDrop, _ := s.Snapshot("t")
	_, err = Execute(context.Background(), snDrop, &Query{
		Predicates: []Predicate{{Field: "zone", Op: OpEq, Value: "x"}},
		Aggregates: []Aggregate{{Kind: AggCount}},
	}, nil)
	t.Logf("删除字段: 查询 zone -> code=%q", CodeOf(err))
	if CodeOf(err) != ErrCodeSchemaIncompatible {
		t.Fatalf("删除字段查询应返回 SCHEMA_INCOMPATIBLE, got %v", err)
	}
}

// TestPruneSavesFragmentBudget 被裁剪的片段不占字节预算：
// 只查询小区间时，低到无法读取任何无关片段的预算仍可成功；
// 一旦查询必须触及大/多片段，预算不足明确失败，不返回半截结果。
func TestPruneSavesFragmentBudget(t *testing.T) {
	s, sc := compactTable(t)
	writeRows(t, s, sc, "low", []int64{1, 2, 3, 4})
	writeRows(t, s, sc, "high", []int64{1000, 2000, 3000, 4000})
	sn, _ := s.Snapshot("t")
	metas := sn.Fragments()
	var highSize int64
	var lowSize int64
	for _, m := range metas {
		if m.Key == "high" {
			highSize = m.Size
		} else {
			lowSize = m.Size
		}
	}
	t.Logf("输入: low=%d字节 high=%d字节", lowSize, highSize)

	// 只命中 high 片段：预算给到仅够一个片段（< 两个片段之和）。
	budget := highSize
	qHigh := &Query{
		Predicates: []Predicate{{Field: "id", Op: OpGe, Value: int64(500)}},
		Aggregates: []Aggregate{{Kind: AggCount}},
	}
	res, err := Execute(context.Background(), sn, qHigh, nil, QueryOptions{FragmentBytesBudget: budget})
	if err != nil {
		t.Fatalf("裁剪后单片段预算应足够: %v", err)
	}
	want := fullScanCount(sn, qHigh.Predicates)
	if res.Values[0] != want {
		t.Fatalf("裁剪查询 count=%v, want %d", res.Values[0], want)
	}
	t.Logf("裁剪省预算: 预算=%d 仅读 high 片段 count=%v", budget, res.Values[0])

	// 无过滤查询无法裁剪：同一预算不足以读完两个片段，必须明确失败。
	qAll := &Query{Aggregates: []Aggregate{{Kind: AggCount}}}
	_, err = Execute(context.Background(), sn, qAll, nil, QueryOptions{FragmentBytesBudget: budget})
	t.Logf("预算不足: 全表扫描预算=%d -> code=%q err=%v", budget, CodeOf(err), err)
	if CodeOf(err) != ErrCodeResourceExhausted {
		t.Fatalf("应返回 RESOURCE_EXHAUSTED, got %v", err)
	}
}

// TestAutoCompactThresholds 按片段数与体积阈值自动触发，
// 同步模式下写入返回时整理已完成，且结果与手动整理一致。
func TestAutoCompactThresholds(t *testing.T) {
	t.Run("片段数阈值", func(t *testing.T) {
		s, err := OpenWithOptions(t.TempDir(), Options{
			AutoCompact: AutoCompactConfig{MinFragments: 3, Synchronous: true},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		sc := mustCreate(t, s, "t", Field{Name: "id", Type: TypeInt64})
		idID := sc.FieldByName("id").ID
		put := func(k string, v int64) {
			if err := s.WriteFragment("t", sc.Version, &Fragment{
				Key: k, NumRows: 1, Columns: map[int][]Value{idID: {v}},
			}); err != nil {
				t.Fatal(err)
			}
		}
		put("p0", 1)
		put("p1", 2)
		if sn, _ := s.Snapshot("t"); len(sn.Fragments()) != 2 {
			t.Fatalf("未达阈值不应整理, frags=%d", len(sn.Fragments()))
		}
		put("p2", 3) // 达到 3 个片段 -> 同步自动整理
		sn, _ := s.Snapshot("t")
		if len(sn.Fragments()) != 1 {
			t.Fatalf("达阈值后应自动整理为 1 个片段, got %d", len(sn.Fragments()))
		}
		r, _ := Execute(context.Background(), sn, &Query{Aggregates: []Aggregate{{Kind: AggCount}}}, nil)
		t.Logf("自动整理: 片段数达到 3 -> 合并为 1, count=%v", r.Values[0])
		if r.Values[0] != int64(3) {
			t.Fatalf("自动整理后 count=%v, want 3", r.Values[0])
		}
		// 整理后继续写入不冲突、不丢失。
		put("p3", 4)
		sn2, _ := s.Snapshot("t")
		r2, _ := Execute(context.Background(), sn2, &Query{Aggregates: []Aggregate{{Kind: AggCount}}}, nil)
		if r2.Values[0] != int64(4) || len(sn2.Fragments()) != 2 {
			t.Fatalf("整理后写入 count=%v frags=%d", r2.Values[0], len(sn2.Fragments()))
		}
	})

	t.Run("体积阈值/后台模式", func(t *testing.T) {
		s, err := OpenWithOptions(t.TempDir(), Options{
			AutoCompact: AutoCompactConfig{MinBytes: 1, Synchronous: false},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		sc := mustCreate(t, s, "t", Field{Name: "id", Type: TypeInt64})
		idID := sc.FieldByName("id").ID
		var failed error
		s.options.OnAutoCompactError = func(table string, e error) { failed = e }
		for i := 0; i < 4; i++ {
			if err := s.WriteFragment("t", sc.Version, &Fragment{
				Key: fmt.Sprintf("p%d", i), NumRows: 1,
				Columns: map[int][]Value{idID: {int64(i)}},
			}); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.WaitAutoCompact(context.Background()); err != nil {
			t.Fatal(err)
		}
		if failed != nil {
			t.Fatalf("后台整理失败: %v", failed)
		}
		sn, _ := s.Snapshot("t")
		r, _ := Execute(context.Background(), sn, &Query{Aggregates: []Aggregate{{Kind: AggCount}}}, nil)
		t.Logf("后台自动整理: 体积阈值触发, 等待结束后 count=%v frags=%d",
			r.Values[0], len(sn.Fragments()))
		if r.Values[0] != int64(4) {
			t.Fatalf("后台整理后 count=%v, want 4", r.Values[0])
		}
		if len(sn.Fragments()) != 1 {
			t.Fatalf("后台整理后 frags=%d, want 1", len(sn.Fragments()))
		}
	})
}
