package store

import (
	"context"
	"testing"
)

// TestSnapshotAtHistoryReadable 保留窗口内的旧版本可回读，
// 且看不到该版本之后（包括整理）才写入的数据。
func TestSnapshotAtHistoryReadable(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenWithOptions(dir, Options{RetainVersions: 0}) // 全部保留
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sc := mustCreate(t, s, "t", Field{Name: "id", Type: TypeInt64})
	idID := sc.FieldByName("id").ID

	put := func(key string, v int64) {
		if err := s.WriteFragment("t", sc.Version, &Fragment{
			Key: key, NumRows: 1, Columns: map[int][]Value{idID: {v}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	countAt := func(version uint64) int64 {
		sn, err := s.SnapshotAt("t", version)
		if err != nil {
			t.Fatalf("snapshot-at %d: %v", version, err)
		}
		r, err := Execute(context.Background(), sn, &Query{
			Aggregates: []Aggregate{{Kind: AggCount}},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return r.Values[0].(int64)
	}

	put("p0", 1)
	v2, _ := s.Snapshot("t") // 版本 2：1 行
	put("p1", 2)
	put("p2", 3)
	beforeCompact, _ := s.Snapshot("t") // 版本 4：3 行
	if _, err := s.Compact(context.Background(), "t", nil); err != nil {
		t.Fatal(err)
	}
	put("p3", 4) // 整理之后再写入
	head, _ := s.Snapshot("t")

	t.Logf("版本关系: v2=%d 整理前=%d 整理后head=%d, 片段数 2->1->2",
		v2.Version(), beforeCompact.Version(), head.Version())

	if c := countAt(v2.Version()); c != 1 {
		t.Fatalf("旧版本 v%d 回读 count=%d, want 1", v2.Version(), c)
	}
	if c := countAt(beforeCompact.Version()); c != 3 {
		t.Fatalf("整理前版本 v%d 回读 count=%d, want 3", beforeCompact.Version(), c)
	}
	if c := countAt(head.Version()); c != 4 {
		t.Fatalf("最新版本 count=%d, want 4", c)
	}
	t.Logf("窗口判定: v%d 只见 1 行, v%d 见 3 行, 整理后 head v%d 见 4 行（旧版本看不到整理后写入）",
		v2.Version(), beforeCompact.Version(), head.Version())
}

// TestRetentionWindowBoundary 保留最近 N 个版本：窗口内可读，
// 窗口外明确返回 VERSION_UNAVAILABLE/VERSION_TRIMMED，不返回最新或空结果。
func TestRetentionWindowBoundary(t *testing.T) {
	s, err := OpenWithOptions(t.TempDir(), Options{RetainVersions: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sc := mustCreate(t, s, "t", Field{Name: "id", Type: TypeInt64})
	idID := sc.FieldByName("id").ID
	for i := 0; i < 5; i++ {
		if err := s.WriteFragment("t", sc.Version, &Fragment{
			Key:     "p" + string(rune('0'+i)),
			NumRows: 1,
			Columns: map[int][]Value{idID: {int64(i)}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	// 建表 v1 + 5 次写入 v2..v6；保留 3 个 => 可回读 v4,v5,v6。
	oldest, err := s.OldestVersion("t")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("窗口判定: head=6 retain=3 -> 最旧可回读版本=%d", oldest)
	if oldest != 4 {
		t.Fatalf("oldest=%d, want 4", oldest)
	}

	// 窗口内版本可读，且各自数据正确（不是最新、不是空）。
	sn4, err := s.SnapshotAt("t", 4)
	if err != nil {
		t.Fatalf("v4 应可读: %v", err)
	}
	r4, _ := Execute(context.Background(), sn4, &Query{Aggregates: []Aggregate{{Kind: AggCount}}}, nil)
	if r4.Values[0] != int64(3) {
		t.Fatalf("v4 count=%v, want 3", r4.Values[0])
	}

	// 窗口外版本必须明确失败。
	for _, v := range []uint64{1, 2, 3} {
		_, err := s.SnapshotAt("t", v)
		t.Logf("窗口判定: 请求 v%d -> code=%q reason=%v", v, CodeOf(err), err)
		if CodeOf(err) != ErrCodeVersionUnavailable {
			t.Fatalf("v%d 应返回 VERSION_UNAVAILABLE, got %v", v, err)
		}
	}

	// 尚未提交的版本同样明确失败。
	_, err = s.SnapshotAt("t", 100)
	t.Logf("窗口判定: 请求未提交 v100 -> code=%q", CodeOf(err))
	if CodeOf(err) != ErrCodeVersionUnavailable {
		t.Fatalf("未提交版本应返回 VERSION_UNAVAILABLE, got %v", err)
	}
}

// TestSetRetentionShrinksWindow 运行期调小保留窗口立即裁剪，
// 且不影响最新快照读取；整理回收旧片段文件后被裁版本不可恢复。
func TestSetRetentionShrinksWindow(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenWithOptions(dir, Options{RetainVersions: 0})
	if err != nil {
		t.Fatal(err)
	}
	sc := mustCreate(t, s, "t", Field{Name: "id", Type: TypeInt64})
	idID := sc.FieldByName("id").ID
	for i := 0; i < 4; i++ {
		if err := s.WriteFragment("t", sc.Version, &Fragment{
			Key:     "p" + string(rune('0'+i)),
			NumRows: 1,
			Columns: map[int][]Value{idID: {int64(i)}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	// 全部保留时 v2 可读。
	if _, err := s.SnapshotAt("t", 2); err != nil {
		t.Fatalf("裁剪前 v2 应可读: %v", err)
	}
	// 先整理，再把窗口缩到 1：旧片段文件应被回收。
	if _, err := s.Compact(context.Background(), "t", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.SetRetention("t", 1); err != nil {
		t.Fatal(err)
	}
	r, err := s.Retention("t")
	if err != nil || r != 1 {
		t.Fatalf("retention=%d err=%v", r, err)
	}
	_, err = s.SnapshotAt("t", 2)
	t.Logf("窗口判定: 缩窗后请求 v2 -> code=%q err=%v", CodeOf(err), err)
	if CodeOf(err) != ErrCodeVersionUnavailable {
		t.Fatalf("缩窗后 v2 应不可用, got %v", err)
	}
	// 最新快照仍正确。
	sn, _ := s.Snapshot("t")
	res, _ := Execute(context.Background(), sn, &Query{Aggregates: []Aggregate{{Kind: AggCount}}}, nil)
	if res.Values[0] != int64(4) {
		t.Fatalf("缩窗后最新快照 count=%v, want 4", res.Values[0])
	}

	// 重启后裁剪结果持久：旧文件已物理删除，不会复活。
	s.Close()
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if _, err := s2.SnapshotAt("t", 2); CodeOf(err) != ErrCodeVersionUnavailable {
		t.Fatalf("重启后被裁版本应仍不可用, got %v", err)
	}
	sn2, _ := s2.Snapshot("t")
	res2, _ := Execute(context.Background(), sn2, &Query{Aggregates: []Aggregate{{Kind: AggCount}}}, nil)
	t.Logf("重启复现: 最新 count=%v (应=4), v2 仍不可用", res2.Values[0])
	if res2.Values[0] != int64(4) {
		t.Fatalf("重启后 count=%v, want 4", res2.Values[0])
	}
}

// TestHistorySurvivesReopen 重启后保留窗口与历史版本内容可复现，
// 包括“整理后旧版本仍能完整回读”。
func TestHistorySurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenWithOptions(dir, Options{RetainVersions: 10})
	if err != nil {
		t.Fatal(err)
	}
	sc := mustCreate(t, s, "t", Field{Name: "id", Type: TypeInt64})
	idID := sc.FieldByName("id").ID
	put := func(key string, v int64) {
		if err := s.WriteFragment("t", sc.Version, &Fragment{
			Key: key, NumRows: 1, Columns: map[int][]Value{idID: {v}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	put("p0", 1)
	put("p1", 2)
	preCompact, _ := s.Snapshot("t")
	if _, err := s.Compact(context.Background(), "t", nil); err != nil {
		t.Fatal(err)
	}
	put("p2", 3)
	s.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	sn, err := s2.SnapshotAt("t", preCompact.Version())
	if err != nil {
		t.Fatalf("重启后旧版本不可读: %v", err)
	}
	r, err := Execute(context.Background(), sn, &Query{Aggregates: []Aggregate{{Kind: AggCount}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("重启复现: 整理前版本 v%d count=%v (应=2，文件在保留窗口内未被回收)",
		preCompact.Version(), r.Values[0])
	if r.Values[0] != int64(2) {
		t.Fatalf("重启后旧版本 count=%v, want 2", r.Values[0])
	}
}
