package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// writeRows 用当前结构版本写入若干单 int64 列片段。
func writeRows(t *testing.T, s *Store, table string, schemaV int, fid int, key string, vals ...int64) {
	t.Helper()
	cols := map[int][]Value{fid: {}}
	for _, v := range vals {
		cols[fid] = append(cols[fid], v)
	}
	if err := s.WriteFragment(table, schemaV, &Fragment{
		Key: key, NumRows: len(vals), Columns: cols,
	}); err != nil {
		t.Fatalf("write %s: %v", key, err)
	}
}

func sumCount(t *testing.T, sn *Snapshot) (float64, int64) {
	t.Helper()
	res, err := Execute(context.Background(), sn, &Query{
		Aggregates: []Aggregate{{Kind: AggSum, Field: "v"}, {Kind: AggCount}},
	}, nil)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	return res.Values[0].(float64), res.Values[1].(int64)
}

// TestCompactManualResultIdentical 整理前后相同查询结果完全一致，
// 片段数变少；整理产生新版本，旧版本仍可回读。
func TestCompactManualResultIdentical(t *testing.T) {
	s := newTestStore(t)
	sc := mustCreate(t, s, "t", Field{Name: "v", Type: TypeInt64})
	fid := sc.FieldByName("v").ID
	if err := s.SetTableOptions("t", TableOptions{RetainVersions: 5}); err != nil {
		t.Fatal(err)
	}

	const frags = 6
	var wantSum float64
	for i := 0; i < frags; i++ {
		writeRows(t, s, "t", sc.Version, fid, fmt.Sprintf("p%d", i), int64(i*10+1), int64(i*10+2))
		wantSum += float64(i*10+1) + float64(i*10+2)
	}
	snBefore, _ := s.Snapshot("t")
	vBefore := snBefore.Version()
	sumB, cntB := sumCount(t, snBefore)
	t.Logf("输入: %d 个片段 版本=%d sum=%.0f count=%d", frags, vBefore, sumB, cntB)

	res, err := s.Compact(context.Background(), "t", CompactionOptions{})
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if !res.Compacted || res.MergedFragments != frags || res.Rows != frags*2 {
		t.Fatalf("整理结果异常: %+v", res)
	}
	t.Logf("整理: 版本 %d->%d 合并片段=%d 行数=%d 字节=%d",
		res.BeforeVersion, res.AfterVersion, res.MergedFragments, res.Rows, res.Bytes)

	snAfter, _ := s.Snapshot("t")
	if len(snAfter.Fragments()) != 1 {
		t.Fatalf("整理后片段数=%d, want 1", len(snAfter.Fragments()))
	}
	sumA, cntA := sumCount(t, snAfter)
	t.Logf("结果一致性: 整理前 sum=%.0f/count=%d 整理后 sum=%.0f/count=%d", sumB, cntB, sumA, cntA)
	if sumA != sumB || cntA != cntB || sumA != wantSum {
		t.Fatalf("整理改变了查询结果: before=%.0f/%d after=%.0f/%d want=%.0f", sumB, cntB, sumA, cntA, wantSum)
	}

	// 整理前版本仍可回读，且内容相同、片段集合是旧的多个片段。
	old, err := s.SnapshotAt("t", vBefore)
	if err != nil {
		t.Fatalf("旧版本应仍可读: %v", err)
	}
	if len(old.Fragments()) != frags {
		t.Fatalf("旧版本片段数=%d, want %d", len(old.Fragments()), frags)
	}
	sumO, cntO := sumCount(t, old)
	if sumO != sumB || cntO != cntB {
		t.Fatalf("旧版本回读不一致: %.0f/%d vs %.0f/%d", sumO, cntO, sumB, cntB)
	}
	t.Logf("历史版本: v%d 仍可回读 sum=%.0f count=%d 片段数=%d", vBefore, sumO, cntO, len(old.Fragments()))

	// 再整理一次（只有 1 个片段）应是空操作，不产生新版本。
	noop, err := s.Compact(context.Background(), "t", CompactionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if noop.Compacted {
		t.Fatalf("单片段整理应为空操作: %+v", noop)
	}
}

// TestCompactConcurrentWrites 整理期间并发写入不丢、不重复，
// 整理后行集 = 整理计划内行 + 期间新写入。
func TestCompactConcurrentWrites(t *testing.T) {
	s := newTestStore(t)
	sc := mustCreate(t, s, "t", Field{Name: "v", Type: TypeInt64})
	fid := sc.FieldByName("v").ID

	const base = 10
	for i := 0; i < base; i++ {
		writeRows(t, s, "t", sc.Version, fid, fmt.Sprintf("base-%d", i), int64(i))
	}

	var wg sync.WaitGroup
	const writers = 8
	const perWriter = 5
	wg.Add(1)
	go func() { // 先启动整理
		defer wg.Done()
		res, err := s.Compact(context.Background(), "t", CompactionOptions{})
		if err != nil {
			t.Errorf("compact: %v", err)
			return
		}
		t.Logf("整理完成: 版本 %d->%d 合并=%d", res.BeforeVersion, res.AfterVersion, res.MergedFragments)
	}()
	// 与整理并发追加新片段（每个 key 唯一，避免重叠冲突）。
	var ww sync.WaitGroup
	for w := 0; w < writers; w++ {
		ww.Add(1)
		go func(w int) {
			defer ww.Done()
			for j := 0; j < perWriter; j++ {
				key := fmt.Sprintf("new-%d-%d", w, j)
				if err := s.WriteFragment("t", sc.Version, &Fragment{
					Key: key, NumRows: 1,
					Columns: map[int][]Value{fid: {int64(1000 + w*perWriter + j)}},
				}); err != nil {
					t.Errorf("write %s: %v", key, err)
					return
				}
			}
		}(w)
	}
	ww.Wait()
	wg.Wait()

	sn, _ := s.Snapshot("t")
	_, cnt := sumCount(t, sn)
	want := int64(base + writers*perWriter)
	t.Logf("并发交错结果: count=%d want=%d 可见片段数=%d", cnt, want, len(sn.Fragments()))
	if cnt != want {
		t.Fatalf("整理与写入交错后行数=%d, want %d（丢行或重复行）", cnt, want)
	}
}

// TestCompactInterruptedAndRetry 整理被取消后存储仍可正常读写；
// 重新整理成功且结果正确，磁盘上不留半成品临时文件。
func TestCompactInterruptedAndRetry(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sc := mustCreate(t, s, "t", Field{Name: "v", Type: TypeInt64})
	fid := sc.FieldByName("v").ID
	for i := 0; i < 5; i++ {
		writeRows(t, s, "t", sc.Version, fid, fmt.Sprintf("p%d", i), int64(i))
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = s.Compact(ctx, "t", CompactionOptions{})
	t.Logf("中断整理: code=%q err=%v", CodeOf(err), err)
	if CodeOf(err) != ErrCodeInterrupted {
		t.Fatalf("应返回 INTERRUPTED, got %v", err)
	}

	// 存储仍可写、可读。
	writeRows(t, s, "t", sc.Version, fid, "after-cancel", 999)
	sn, _ := s.Snapshot("t")
	_, cnt := sumCount(t, sn)
	if cnt != 6 {
		t.Fatalf("中断后行数=%d, want 6", cnt)
	}

	// 重试整理成功。
	res, err := s.Compact(context.Background(), "t", CompactionOptions{})
	if err != nil {
		t.Fatalf("重试整理失败: %v", err)
	}
	if !res.Compacted {
		t.Fatalf("重试整理应成功")
	}
	sn2, _ := s.Snapshot("t")
	_, cnt2 := sumCount(t, sn2)
	if cnt2 != 6 || len(sn2.Fragments()) != 1 {
		t.Fatalf("重试整理后 count=%d 片段数=%d", cnt2, len(sn2.Fragments()))
	}

	// 无半成品临时文件残留（merged 已提交；失败的 .merge-tmp 已清理）。
	entries, _ := os.ReadDir(filepath.Join(dir, "t"))
	for _, e := range entries {
		name := e.Name()
		if filepath.Ext(name) == ".tmp" || filepath.Ext(name) == ".merge-tmp" {
			t.Fatalf("发现残留临时文件: %s", name)
		}
	}
}

// TestCompactBudget 整理内存预算不足时明确失败且不改变可见状态。
func TestCompactBudget(t *testing.T) {
	s := newTestStore(t)
	sc := mustCreate(t, s, "t", Field{Name: "v", Type: TypeInt64})
	fid := sc.FieldByName("v").ID
	for i := 0; i < 3; i++ {
		writeRows(t, s, "t", sc.Version, fid, fmt.Sprintf("p%d", i), int64(i))
	}
	sn, _ := s.Snapshot("t")
	before := sn.Version()

	_, err := s.Compact(context.Background(), "t", CompactionOptions{MemBudget: 1})
	t.Logf("预算不足: code=%q err=%v", CodeOf(err), err)
	if CodeOf(err) != ErrCodeResourceExhausted {
		t.Fatalf("应返回 RESOURCE_EXHAUSTED, got %v", err)
	}
	sn2, _ := s.Snapshot("t")
	if sn2.Version() != before || len(sn2.Fragments()) != 3 {
		t.Fatalf("失败整理不应改变可见状态: 版本=%d 片段数=%d", sn2.Version(), len(sn2.Fragments()))
	}

	// 预算放开后成功。
	res, err := s.Compact(context.Background(), "t", CompactionOptions{MemBudget: 1 << 20})
	if err != nil {
		t.Fatalf("预算足够时应成功: %v", err)
	}
	if !res.Compacted {
		t.Fatalf("应完成整理")
	}
}

// TestCompactReproducibleAfterReopen 整理 -> 重启：结果可复现，
// 孤儿文件（含失败整理的残留）被回收。
func TestCompactReproducibleAfterReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	sc := mustCreate(t, s, "t", Field{Name: "v", Type: TypeInt64})
	fid := sc.FieldByName("v").ID
	var wantSum float64
	for i := 0; i < 4; i++ {
		writeRows(t, s, "t", sc.Version, fid, fmt.Sprintf("p%d", i), int64(i+1))
		wantSum += float64(i + 1)
	}
	if _, err := s.Compact(context.Background(), "t", CompactionOptions{}); err != nil {
		t.Fatal(err)
	}
	// 留一个垃圾文件，验证启动回收。
	if err := os.WriteFile(filepath.Join(dir, "t", "frag-999-garbage.bin"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	sn, err := s2.Snapshot("t")
	if err != nil {
		t.Fatal(err)
	}
	sumA, cntA := sumCount(t, sn)
	t.Logf("重启复现: sum=%.0f count=%d 片段数=%d", sumA, cntA, len(sn.Fragments()))
	if sumA != wantSum || cntA != 4 || len(sn.Fragments()) != 1 {
		t.Fatalf("重启后不一致: sum=%.0f want=%.0f count=%d frags=%d", sumA, wantSum, cntA, len(sn.Fragments()))
	}
	if _, err := os.Stat(filepath.Join(dir, "t", "frag-999-garbage.bin")); !os.IsNotExist(err) {
		t.Fatalf("孤儿文件应在启动时被回收")
	}
}

// TestCompactAutoPolicy 按片段数量阈值自动触发整理；写入不被阻塞，
// 最终片段被合并且无数据丢失。
func TestCompactAutoPolicy(t *testing.T) {
	s := newTestStore(t)
	sc := mustCreate(t, s, "t", Field{Name: "v", Type: TypeInt64})
	fid := sc.FieldByName("v").ID
	if err := s.SetTableOptions("t", TableOptions{
		RetainVersions: 3,
		AutoCompact:    CompactionPolicy{MaxFragments: 4},
	}); err != nil {
		t.Fatal(err)
	}

	var want int64
	for i := 0; i < 9; i++ {
		writeRows(t, s, "t", sc.Version, fid, fmt.Sprintf("p%d", i), int64(i+1))
		want += int64(i + 1)
	}

	// 轮询等待后台自动整理把片段数压下来。
	deadline := time.Now().Add(5 * time.Second)
	var fragCount int
	for time.Now().Before(deadline) {
		sn, _ := s.Snapshot("t")
		fragCount = len(sn.Fragments())
		res, err := Execute(context.Background(), sn, &Query{
			Aggregates: []Aggregate{{Kind: AggSum, Field: "v"}},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if fragCount < 4 && res.Values[0].(float64) == float64(want) {
			t.Logf("自动整理完成: 片段数=%d sum=%.0f", fragCount, res.Values[0].(float64))
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("自动整理未在时限内完成: 片段数=%d", fragCount)
}

// TestCompactAutoSizePolicy 按体积阈值自动触发。
func TestCompactAutoSizePolicy(t *testing.T) {
	s := newTestStore(t)
	sc := mustCreate(t, s, "t",
		Field{Name: "v", Type: TypeInt64}, Field{Name: "s", Type: TypeString})
	vid := sc.FieldByName("v").ID
	sid := sc.FieldByName("s").ID
	big := make([]byte, 256)
	for i := range big {
		big[i] = 'x'
	}
	pol := CompactionPolicy{MaxTotalBytes: 1} // 任意写入即触发
	if err := s.SetTableOptions("t", TableOptions{RetainVersions: 2, AutoCompact: pol}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := s.WriteFragment("t", sc.Version, &Fragment{
			Key: fmt.Sprintf("p%d", i), NumRows: 1,
			Columns: map[int][]Value{
				vid: {int64(i)},
				sid: {string(big)},
			},
		}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		sn, _ := s.Snapshot("t")
		if len(sn.Fragments()) == 1 {
			t.Logf("体积阈值触发自动整理: 片段数=1")
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("体积阈值自动整理未触发")
}

// TestCompactWithDroppedColumn 字段删除后整理：已删除列的数据保留在文件
// 中但不再可见，整理后现存字段查询结果不变；按已删除名查询仍报错。
func TestCompactWithDroppedColumn(t *testing.T) {
	s := newTestStore(t)
	sc := mustCreate(t, s, "t",
		Field{Name: "id", Type: TypeInt64},
		Field{Name: "secret", Type: TypeInt64},
	)
	idID := sc.FieldByName("id").ID
	secID := sc.FieldByName("secret").ID
	for i := 0; i < 3; i++ {
		if err := s.WriteFragment("t", sc.Version, &Fragment{
			Key: fmt.Sprintf("p%d", i), NumRows: 2,
			Columns: map[int][]Value{
				idID:  {int64(i*2 + 1), int64(i*2 + 2)},
				secID: {int64(100 + i), int64(200 + i)},
			},
		}); err != nil {
			t.Fatal(err)
		}
	}
	sc2, err := s.EvolveSchema("t", sc.Version, SchemaChange{
		Kind: ChangeDropField, OldName: "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.Compact(context.Background(), "t", CompactionOptions{})
	if err != nil {
		t.Fatalf("删除字段后整理: %v", err)
	}
	t.Logf("删除 secret 后整理: 合并=%d 行=%d", res.MergedFragments, res.Rows)
	sn, _ := s.Snapshot("t")
	r, err := Execute(context.Background(), sn, &Query{
		Aggregates: []Aggregate{{Kind: AggSum, Field: "id"}, {Kind: AggCount}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Values[0] != 21.0 || r.Values[1] != int64(6) {
		t.Fatalf("现存字段结果错误: %v/%v want 21/6", r.Values[0], r.Values[1])
	}
	_, err = Execute(context.Background(), sn, &Query{
		Aggregates: []Aggregate{{Kind: AggSum, Field: "secret"}},
	}, nil)
	if CodeOf(err) != ErrCodeSchemaIncompatible {
		t.Fatalf("已删除字段应 SCHEMA_INCOMPATIBLE, got %v", err)
	}
	// schema 版本保持演进后的版本。
	if sn.Schema().Version != sc2.Version {
		t.Fatalf("快照结构版本=%d want %d", sn.Schema().Version, sc2.Version)
	}
}
