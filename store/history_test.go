package store

import (
	"context"
	"fmt"
	"testing"
)

// TestSnapshotAtRetainedVersions 保留窗口内的历史版本可完整回读；
// 整理后的旧版本视图仍是整理前的片段集合，看不到整理之后的新数据。
func TestSnapshotAtRetainedVersions(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sc := mustCreate(t, s, "t", Field{Name: "v", Type: TypeInt64})
	fid := sc.FieldByName("v").ID
	if err := s.SetTableOptions("t", TableOptions{RetainVersions: 10}); err != nil {
		t.Fatal(err)
	}

	// v2..v5：4 个片段，每个一行。
	var versions []uint64
	for i := 1; i <= 4; i++ {
		writeRows(t, s, "t", sc.Version, fid, fmt.Sprintf("p%d", i), int64(i))
		sn, _ := s.Snapshot("t")
		versions = append(versions, sn.Version())
	}
	compactV := versions[len(versions)-1]

	// v6：整理提交（4 -> 1）。
	res, err := s.Compact(context.Background(), "t", CompactionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	compactedAt := res.AfterVersion
	t.Logf("版本关系: 写入版本=%v 整理前=%d 整理提交=%d", versions, compactV, compactedAt)

	// v7：整理之后的新写入。
	writeRows(t, s, "t", sc.Version, fid, "post", 100)
	head, _ := s.Snapshot("t")
	t.Logf("整理后写入版本=%d 可见片段=%v", head.Version(), keys(head.Fragments()))

	// 整理前最后一个版本 v5：看到 4 个旧片段，sum=10，看不到 post 的 100。
	old, err := s.SnapshotAt("t", compactV)
	if err != nil {
		t.Fatalf("整理前版本应在保留窗口内: %v", err)
	}
	if len(old.Fragments()) != 4 {
		t.Fatalf("旧版本应看到整理前 4 个片段, got %d", len(old.Fragments()))
	}
	r, err := Execute(context.Background(), old, &Query{
		Aggregates: []Aggregate{{Kind: AggSum, Field: "v"}, {Kind: AggCount}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("历史回读 v%d: sum=%v count=%v (不含整理后写入的 100)", compactV, r.Values[0], r.Values[1])
	if r.Values[0] != 10.0 || r.Values[1] != int64(4) {
		t.Fatalf("旧版本被整理污染: sum=%v count=%v, want 10/4", r.Values[0], r.Values[1])
	}

	// 整理提交版本 v6：1 个合并片段 + 4 行。
	atCompact, err := s.SnapshotAt("t", compactedAt)
	if err != nil {
		t.Fatal(err)
	}
	if len(atCompact.Fragments()) != 1 {
		t.Fatalf("整理版本片段数=%d, want 1", len(atCompact.Fragments()))
	}
	r2, _ := Execute(context.Background(), atCompact, &Query{
		Aggregates: []Aggregate{{Kind: AggSum, Field: "v"}},
	}, nil)
	if r2.Values[0] != 10.0 {
		t.Fatalf("整理版本 sum=%v, want 10", r2.Values[0])
	}

	// 重启后窗口内版本仍可回读（合并片段与旧片段文件都在）。
	s.Close()
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s2.Close() }()
	old2, err := s2.SnapshotAt("t", compactV)
	if err != nil {
		t.Fatalf("重启后旧版本应仍可读: %v", err)
	}
	r3, err := Execute(context.Background(), old2, &Query{
		Aggregates: []Aggregate{{Kind: AggSum, Field: "v"}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("重启后历史回读 v%d: sum=%v 片段数=%d", compactV, r3.Values[0], len(old2.Fragments()))
	if r3.Values[0] != 10.0 {
		t.Fatalf("重启后旧版本 sum=%v, want 10", r3.Values[0])
	}
}

// TestRetentionWindowBoundary 超出保留窗口返回 VERSION_UNAVAILABLE，
// 绝不悄悄返回最新或空；缩小窗口后旧版本立即不可用。
func TestRetentionWindowBoundary(t *testing.T) {
	s := newTestStore(t)
	sc := mustCreate(t, s, "t", Field{Name: "v", Type: TypeInt64})
	fid := sc.FieldByName("v").ID
	if err := s.SetTableOptions("t", TableOptions{RetainVersions: 3}); err != nil {
		t.Fatal(err)
	}
	var versions []uint64
	for i := 0; i < 5; i++ {
		writeRows(t, s, "t", sc.Version, fid, fmt.Sprintf("p%d", i), int64(i))
		sn, _ := s.Snapshot("t")
		versions = append(versions, sn.Version())
	}
	t.Logf("输入: 提交版本=%v 保留最近 3 个", versions)

	// 窗口内应可用。
	for _, v := range versions[len(versions)-3:] {
		if _, err := s.SnapshotAt("t", v); err != nil {
			t.Fatalf("窗口内版本 %d 应可用: %v", v, err)
		}
	}

	// 窗口外应明确失败。
	for _, v := range versions[:len(versions)-3] {
		_, err := s.SnapshotAt("t", v)
		t.Logf("窗口判定: 请求 v%d -> code=%q reason=%v", v, CodeOf(err), reasonOf(err))
		if CodeOf(err) != ErrCodeVersionUnavailable {
			t.Fatalf("v%d 应返回 VERSION_UNAVAILABLE, got %v", v, err)
		}
	}

	// 从未存在过的版本号也返回同一分类码（NO_SUCH_VERSION）。
	_, err := s.SnapshotAt("t", 99999)
	if CodeOf(err) != ErrCodeVersionUnavailable {
		t.Fatalf("不存在版本应返回 VERSION_UNAVAILABLE, got %v", err)
	}
	t.Logf("窗口判定: 请求 v99999 -> reason=%s", reasonOf(err))

	// 缩小窗口到 1：只剩最新版本，旧版本立即不可用，最新版本正常。
	if err := s.SetTableOptions("t", TableOptions{RetainVersions: 1}); err != nil {
		t.Fatal(err)
	}
	head, _ := s.Snapshot("t")
	if _, err := s.SnapshotAt("t", head.Version()); err != nil {
		t.Fatalf("最新版本应可用: %v", err)
	}
	if _, err := s.SnapshotAt("t", versions[len(versions)-2]); CodeOf(err) != ErrCodeVersionUnavailable {
		t.Fatalf("缩小窗口后旧版本应不可用, got %v", err)
	}
	t.Logf("缩小窗口: 仅 v%d 可用", head.Version())
}

// TestRetentionAfterCompactionFilesKept 保留窗口要求下，整理产生的
// 新版本与整理前版本都能回读：GC 不能提前删掉窗口内引用的源文件。
func TestRetentionAfterCompactionFilesKept(t *testing.T) {
	s := newTestStore(t)
	sc := mustCreate(t, s, "t", Field{Name: "v", Type: TypeInt64})
	fid := sc.FieldByName("v").ID
	if err := s.SetTableOptions("t", TableOptions{RetainVersions: 5}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		writeRows(t, s, "t", sc.Version, fid, fmt.Sprintf("p%d", i), int64(i+1))
	}
	sn, _ := s.Snapshot("t")
	beforeV := sn.Version()
	if _, err := s.Compact(context.Background(), "t", CompactionOptions{}); err != nil {
		t.Fatal(err)
	}
	// 再写入并再整理，让中间版本也可能成为被替换对象。
	writeRows(t, s, "t", sc.Version, fid, "x", 42)
	if _, err := s.Compact(context.Background(), "t", CompactionOptions{}); err != nil {
		t.Fatal(err)
	}

	old, err := s.SnapshotAt("t", beforeV)
	if err != nil {
		t.Fatalf("整理多次后窗口内旧版本仍应可读: %v", err)
	}
	r, err := Execute(context.Background(), old, &Query{
		Aggregates: []Aggregate{{Kind: AggSum, Field: "v"}, {Kind: AggCount}},
	}, nil)
	if err != nil {
		t.Fatalf("执行旧版本查询: %v", err)
	}
	t.Logf("多轮整理后历史 v%d: sum=%v count=%v", beforeV, r.Values[0], r.Values[1])
	if r.Values[0] != 6.0 || r.Values[1] != int64(3) {
		t.Fatalf("旧版本结果错误: %v/%v, want 6/3", r.Values[0], r.Values[1])
	}
}

func keys(ms []FragmentMeta) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.Key
	}
	return out
}
