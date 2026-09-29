package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// compactTable 建表（id:int64, amount:float64）并写入若干小片段。
func compactTable(t *testing.T) (*Store, *Schema) {
	t.Helper()
	s := newTestStore(t)
	sc := mustCreate(t, s, "t",
		Field{Name: "id", Type: TypeInt64},
		Field{Name: "amount", Type: TypeFloat64},
	)
	return s, sc
}

func writeRows(t *testing.T, s *Store, sc *Schema, key string, ids []int64) {
	t.Helper()
	idID := sc.FieldByName("id").ID
	amtID := sc.FieldByName("amount").ID
	cols := map[int][]Value{idID: {}, amtID: {}}
	for _, v := range ids {
		cols[idID] = append(cols[idID], v)
		cols[amtID] = append(cols[amtID], float64(v)*10)
	}
	if err := s.WriteFragment("t", sc.Version, &Fragment{
		Key: key, NumRows: len(ids), Columns: cols,
	}); err != nil {
		t.Fatalf("write %s: %v", key, err)
	}
}

func sumAndCount(t *testing.T, sn *Snapshot, pred *Predicate) []Value {
	t.Helper()
	q := &Query{Aggregates: []Aggregate{
		{Kind: AggSum, Field: "amount"}, {Kind: AggCount},
	}}
	if pred != nil {
		q.Predicates = []Predicate{*pred}
	}
	res, err := Execute(context.Background(), sn, q, nil)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	return res.Values
}

// TestCompactEquivalence 整理前后同一过滤+聚合查询结果完全一致。
func TestCompactEquivalence(t *testing.T) {
	s, sc := compactTable(t)
	writeRows(t, s, sc, "p0", []int64{1, 2, 3})
	writeRows(t, s, sc, "p1", []int64{4, 5})
	writeRows(t, s, sc, "p2", []int64{6, 7, 8, 9})

	snBefore, _ := s.Snapshot("t")
	pred := &Predicate{Field: "id", Op: OpGe, Value: int64(4)}
	before := sumAndCount(t, snBefore, pred)
	t.Logf("输入: 3 个片段共 9 行, 谓词 id>=4 -> 整理前 sum=%v count=%v", before[0], before[1])

	res, err := s.Compact(context.Background(), "t", nil)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	t.Logf("整理: 合并 %d 个片段 -> %s, 提交版本=%d, 行数=%d",
		res.InputFragments, res.OutputFile, res.Version, res.Rows)

	snAfter, _ := s.Snapshot("t")
	after := sumAndCount(t, snAfter, pred)
	t.Logf("窗口判定: 整理后可见片段数=%d, 查询 sum=%v count=%v",
		len(snAfter.Fragments()), after[0], after[1])
	if len(snAfter.Fragments()) != 1 {
		t.Fatalf("整理后应有 1 个可见片段, got %d", len(snAfter.Fragments()))
	}
	if after[0] != before[0] || after[1] != before[1] {
		t.Fatalf("整理前后结果不一致: before=%v after=%v", before, after)
	}
	// 全量聚合也应一致。
	if all := sumAndCount(t, snAfter, nil); all[1] != int64(9) || all[0] != 450.0 {
		t.Fatalf("整理后全量聚合错误: %v", all)
	}
}

// TestCompactInterleavedWithWrites 整理提交前有新写入：整理自动重试，
// 新写入不丢、不重复、不出现合并到一半的可见状态。
func TestCompactInterleavedWithWrites(t *testing.T) {
	s, sc := compactTable(t)
	writeRows(t, s, sc, "p0", []int64{1, 2})
	writeRows(t, s, sc, "p1", []int64{3})

	var once sync.Once
	compactHookBeforeCommit = func(table string, rows int) {
		once.Do(func() {
			t.Logf("交错: 整理输出已构建(rows=%d)但未提交，此时写入 p2", rows)
			writeRows(t, s, sc, "p2", []int64{4, 5})
		})
	}
	t.Cleanup(func() { compactHookBeforeCommit = nil })

	res, err := s.Compact(context.Background(), "t", nil)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	t.Logf("整理结果: retried=%v 输入片段=%d 行数=%d 版本=%d",
		res.Retried, res.InputFragments, res.Rows, res.Version)
	if !res.Retried {
		t.Fatal("整理期间有写入，应当发生一次内部重试")
	}
	if res.Rows != 5 || res.InputFragments != 3 {
		t.Fatalf("重试后应合并 3 个片段 5 行, got %d 片段 %d 行", res.InputFragments, res.Rows)
	}
	sn, _ := s.Snapshot("t")
	if len(sn.Fragments()) != 1 {
		t.Fatalf("整理后应只有 1 个可见片段, got %d", len(sn.Fragments()))
	}
	all := sumAndCount(t, sn, nil)
	t.Logf("结果: 整理与写入交错后 sum=%v count=%v (应=150/5, 无重复无丢失)", all[0], all[1])
	if all[0] != 150.0 || all[1] != int64(5) {
		t.Fatalf("交错写入后数据错误: %v", all)
	}
}

// TestCompactDuringSchemaEvolution 整理期间发生结构演进：放弃旧输出重试，
// 新结构下合并结果仍与逐片段扫描一致，历史缺列按 NULL 处理。
func TestCompactDuringSchemaEvolution(t *testing.T) {
	s, sc := compactTable(t)
	writeRows(t, s, sc, "p0", []int64{1, 2})
	writeRows(t, s, sc, "p1", []int64{3})

	var once sync.Once
	compactHookBeforeCommit = func(table string, rows int) {
		once.Do(func() {
			if _, err := s.EvolveSchema("t", sc.Version, SchemaChange{
				Kind: ChangeAddField, Field: Field{Name: "tag", Type: TypeString},
			}); err != nil {
				t.Errorf("evolve during compact: %v", err)
			}
		})
	}
	t.Cleanup(func() { compactHookBeforeCommit = nil })

	res, err := s.Compact(context.Background(), "t", nil)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	t.Logf("整理: 与结构演进交错后 retried=%v 版本=%d 结构版本=%d",
		res.Retried, res.Version, res.Version)
	if !res.Retried {
		t.Fatal("结构演进先行提交，整理应当重试")
	}
	sn, _ := s.Snapshot("t")
	all := sumAndCount(t, sn, nil)
	if all[0] != 60.0 || all[1] != int64(3) {
		t.Fatalf("演进后整理结果错误: %v", all)
	}
	// 新列对历史数据为 NULL：等值谓词不命中任何行。
	q := &Query{
		Predicates: []Predicate{{Field: "tag", Op: OpEq, Value: "x"}},
		Aggregates: []Aggregate{{Kind: AggCount}},
	}
	r, err := Execute(context.Background(), sn, q, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Values[0] != int64(0) {
		t.Fatalf("历史行新列应为 NULL, 命中=%v want 0", r.Values[0])
	}
}

// TestCompactInterrupted 整理被取消时不产生任何可见变更，随后可重试成功。
func TestCompactInterrupted(t *testing.T) {
	s, sc := compactTable(t)
	writeRows(t, s, sc, "p0", []int64{1})
	writeRows(t, s, sc, "p1", []int64{2})
	headBefore, _ := s.Snapshot("t")

	ctx, cancel := context.WithCancel(context.Background())
	compactHookBeforeReadInputs = func(table string) { cancel() }
	t.Cleanup(func() { compactHookBeforeReadInputs = nil })

	_, err := s.Compact(ctx, "t", nil)
	t.Logf("中断: 取消整理 -> code=%q err=%v", CodeOf(err), err)
	if CodeOf(err) != ErrCodeInterrupted {
		t.Fatalf("应返回 INTERRUPTED, got %v", err)
	}
	sn, _ := s.Snapshot("t")
	if sn.Version() != headBefore.Version() || len(sn.Fragments()) != 2 {
		t.Fatalf("中断后状态被改变: version=%d frags=%d", sn.Version(), len(sn.Fragments()))
	}
	// 磁盘上不应残留任何临时文件。
	if entries, _ := os.ReadDir(filepath.Join(s.dir, "t")); countTmp(entries) != 0 {
		t.Fatalf("中断后残留临时文件: %v", entries)
	}

	// 重新整理成功，结果一致。
	res, err := s.Compact(context.Background(), "t", nil)
	if err != nil {
		t.Fatalf("retry compact: %v", err)
	}
	sn2, _ := s.Snapshot("t")
	all := sumAndCount(t, sn2, nil)
	t.Logf("重试: 版本=%d 文件=%s sum=%v count=%v", res.Version, res.OutputFile, all[0], all[1])
	if all[0] != 30.0 || all[1] != int64(2) {
		t.Fatalf("中断后重试结果错误: %v", all)
	}
}

func countTmp(entries []os.DirEntry) int {
	n := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			n++
		}
	}
	return n
}

// TestCompactBudgetExhausted 整理预算不足时明确失败且无可见变更。
func TestCompactBudgetExhausted(t *testing.T) {
	s, sc := compactTable(t)
	writeRows(t, s, sc, "p0", []int64{1, 2, 3})
	writeRows(t, s, sc, "p1", []int64{4, 5, 6})
	headBefore, _ := s.Snapshot("t")

	_, err := s.Compact(context.Background(), "t", &CompactOptions{BudgetBytes: 1})
	t.Logf("预算: 1 字节整理 6 行 -> code=%q err=%v", CodeOf(err), err)
	if CodeOf(err) != ErrCodeResourceExhausted {
		t.Fatalf("应返回 RESOURCE_EXHAUSTED, got %v", err)
	}
	sn, _ := s.Snapshot("t")
	if sn.Version() != headBefore.Version() || len(sn.Fragments()) != 2 {
		t.Fatalf("预算失败不应产生可见变更")
	}

	// 预算放宽后成功。
	res, err := s.Compact(context.Background(), "t", &CompactOptions{BudgetBytes: 1 << 20})
	if err != nil {
		t.Fatalf("预算足够时应成功: %v", err)
	}
	t.Logf("预算放宽: 整理成功版本=%d 行数=%d", res.Version, res.Rows)
}

// TestCompactCleansUpAfterCrash 模拟“输出文件已写但清单未提交”的崩溃：
// 重启后不可达文件被清理，存储可继续读写，且结果不重不漏。
func TestCompactCleansUpAfterCrash(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenWithOptions(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	sc := mustCreate(t, s, "t", Field{Name: "id", Type: TypeInt64})
	idID := sc.FieldByName("id").ID
	writeOne := func(key string, v int64) {
		if err := s.WriteFragment("t", sc.Version, &Fragment{
			Key: key, NumRows: 1, Columns: map[int][]Value{idID: {v}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	writeOne("p0", 1)
	writeOne("p1", 2)
	// 伪造崩溃残留：临时文件 + 未被清单引用的整理输出。
	tdir := filepath.Join(dir, "t")
	if err := os.WriteFile(filepath.Join(tdir, "frag-9-x.bin.tmp"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tdir, "frag-comp-77.bin"), []byte("orphan"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	entries, _ := os.ReadDir(tdir)
	for _, e := range entries {
		n := e.Name()
		if n == "frag-comp-77.bin" || filepath.Ext(n) == ".tmp" {
			t.Fatalf("崩溃残留未清理: %s", n)
		}
	}
	sn, _ := s2.Snapshot("t")
	r, err := Execute(context.Background(), sn, &Query{Aggregates: []Aggregate{{Kind: AggCount}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("重启清理: 残留文件已删除, 可见行数=%v (应=2)", r.Values[0])
	if r.Values[0] != int64(2) {
		t.Fatalf("重启后行数=%v, want 2", r.Values[0])
	}
	// 清理后可继续整理。
	if _, err := s2.Compact(context.Background(), "t", nil); err != nil {
		t.Fatalf("post-crash compact: %v", err)
	}
}

// TestCompactRejectsReservedKey 用户写入不得占用整理保留键前缀。
func TestCompactRejectsReservedKey(t *testing.T) {
	s, sc := compactTable(t)
	idID := sc.FieldByName("id").ID
	err := s.WriteFragment("t", sc.Version, &Fragment{
		Key: reservedKeyPrefix + "seq-1", NumRows: 1,
		Columns: map[int][]Value{idID: {int64(1)}},
	})
	t.Logf("冲突判定: 保留键前缀写入 -> code=%q err=%v", CodeOf(err), err)
	if CodeOf(err) != ErrCodeConflict {
		t.Fatalf("应返回 CONFLICT, got %v", err)
	}
}

// TestWriteRejectsBadType 写入与结构不符的值必须当场失败，
// 不能“写入成功、查询时静默算错”。
func TestWriteRejectsBadType(t *testing.T) {
	s, sc := compactTable(t)
	idID := sc.FieldByName("id").ID
	amtID := sc.FieldByName("amount").ID
	err := s.WriteFragment("t", sc.Version, &Fragment{
		Key: "bad", NumRows: 1,
		Columns: map[int][]Value{
			idID:  {int64(1)},
			amtID: {"not-a-float"}, // 声明 float64，实际给 string
		},
	})
	t.Logf("可解释性: 类型不符写入 -> code=%q err=%v", CodeOf(err), err)
	if CodeOf(err) != ErrCodeSchemaIncompatible {
		t.Fatalf("应返回 SCHEMA_INCOMPATIBLE, got %v", err)
	}
	// 列长度不符同样拒绝。
	err = s.WriteFragment("t", sc.Version, &Fragment{
		Key:     "bad2",
		NumRows: 2,
		Columns: map[int][]Value{
			idID:  {int64(1)}, // 只有 1 个值
			amtID: {1.0, 2.0},
		},
	})
	if CodeOf(err) != ErrCodeSchemaIncompatible {
		t.Fatalf("列长度不符应返回 SCHEMA_INCOMPATIBLE, got %v", err)
	}
	sn, _ := s.Snapshot("t")
	r, _ := Execute(context.Background(), sn, &Query{Aggregates: []Aggregate{{Kind: AggCount}}}, nil)
	if r.Values[0] != int64(0) {
		t.Fatalf("被拒写入不应留下任何行, got %v", r.Values[0])
	}
	fmt.Println("可解释性: 非法写入均被拒绝，表内无残留行")
}

// TestCompactConcurrentWritersStress 多个写者持续写入，同时多次手动整理，
// 最终行数、总和精确正确：不重、不漏、无部分可见状态。
func TestCompactConcurrentWritersStress(t *testing.T) {
	s, sc := compactTable(t)
	idID := sc.FieldByName("id").ID
	amtID := sc.FieldByName("amount").ID

	const writers = 4
	const perWriter = 10
	var wg sync.WaitGroup
	start := make(chan struct{})
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < perWriter; i++ {
				key := fmt.Sprintf("w%d-p%d", w, i)
				v := int64(w*1000 + i)
				if err := s.WriteFragment("t", sc.Version, &Fragment{
					Key: key, NumRows: 1,
					Columns: map[int][]Value{idID: {v}, amtID: {float64(v)}},
				}); err != nil {
					t.Errorf("write %s: %v", key, err)
					return
				}
			}
		}(w)
	}

	// 并发整理者：反复整理，遇到“已有整理在跑”属正常。
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for c := 0; c < 12; c++ {
			_, err := s.Compact(context.Background(), "t", nil)
			if err != nil && CodeOf(err) != ErrCodeConflict {
				t.Errorf("compact %d: %v", c, err)
				return
			}
		}
	}()
	close(start)
	wg.Wait()

	// 收尾再整理一次，确保最终合并成功。
	res, err := s.Compact(context.Background(), "t", nil)
	if err != nil && CodeOf(err) != ErrCodeConflict {
		t.Fatalf("final compact: %v", err)
	}
	t.Logf("整理交错: %d 写者 x%d 行, 最终整理 retried=%v 版本=%d",
		writers, perWriter, res != nil && res.Retried, res.Version)

	sn, _ := s.Snapshot("t")
	all := sumAndCount(t, sn, nil)
	wantCount := int64(writers * perWriter)
	// 期望值总和。
	var wantSum float64
	for w := 0; w < writers; w++ {
		for i := 0; i < perWriter; i++ {
			wantSum += float64(w*1000 + i)
		}
	}
	t.Logf("结果: count=%v/%d sum=%v/%.0f, 可见片段数=%d",
		all[1], wantCount, all[0], wantSum, len(sn.Fragments()))
	if all[1] != wantCount {
		t.Fatalf("行数错误: got %d want %d（存在丢失或重复）", all[1], wantCount)
	}
	if all[0] != wantSum {
		t.Fatalf("总和错误: got %v want %.0f", all[0], wantSum)
	}
	// 整理后应只剩一个片段（整理期间写入会触发重试，最终一致）。
	if _, err := s.Compact(context.Background(), "t", nil); err != nil && CodeOf(err) != ErrCodeConflict {
		t.Fatal(err)
	}
	sn2, _ := s.Snapshot("t")
	if len(sn2.Fragments()) != 1 {
		t.Fatalf("最终应有 1 个片段, got %d", len(sn2.Fragments()))
	}
}
