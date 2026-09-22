package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// TestSnapshotIsolation 快照只能看到其创建时的已提交状态，
// 之后的并发提交不可见（无脏读、无部分可见）。
func TestSnapshotIsolation(t *testing.T) {
	s := newTestStore(t)
	sc := mustCreate(t, s, "t", Field{Name: "v", Type: TypeInt64})
	fid := sc.FieldByName("v").ID

	write := func(key string, vals ...int64) {
		t.Helper()
		cols := map[int][]Value{fid: {}}
		for _, v := range vals {
			cols[fid] = append(cols[fid], v)
		}
		if err := s.WriteFragment("t", int(sc.Version), &Fragment{
			Key: key, NumRows: len(vals), Columns: cols,
		}); err != nil {
			t.Fatalf("write %s: %v", key, err)
		}
	}
	write("p0", 1, 2, 3)

	// 取快照后再提交两个片段。
	sn, err := s.Snapshot("t")
	if err != nil {
		t.Fatal(err)
	}
	write("p1", 10, 20)
	write("p2", 100)
	t.Logf("输入: 快照版本=%d 后又提交 p1,p2", sn.Version())

	res, err := Execute(context.Background(), sn, &Query{
		Aggregates: []Aggregate{{Kind: AggSum, Field: "v"}, {Kind: AggCount}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("结果: 旧快照 sum=%v count=%v (应只见 p0)", res.Values[0], res.Values[1])
	if res.Values[0] != 6.0 || res.Values[1] != int64(3) {
		t.Fatalf("快照隔离被破坏: sum=%v count=%v, want 6/3", res.Values[0], res.Values[1])
	}

	// 新快照可见全部。
	sn2, _ := s.Snapshot("t")
	res2, err := Execute(context.Background(), sn2, &Query{
		Aggregates: []Aggregate{{Kind: AggSum, Field: "v"}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("结果: 新快照 sum=%v (应见全部)", res2.Values[0])
	if res2.Values[0] != 136.0 {
		t.Fatalf("新快照 sum=%v, want 136", res2.Values[0])
	}
}

// TestConcurrentWritersConflict 并发写同一片段键：恰好一个成功，
// 其余得到 FRAGMENT_OVERLAP 冲突，结果可复现。
func TestConcurrentWritersConflict(t *testing.T) {
	for run := 0; run < 5; run++ {
		s := newTestStore(t)
		sc := mustCreate(t, s, "t", Field{Name: "v", Type: TypeInt64})
		fid := sc.FieldByName("v").ID

		const writers = 8
		var wg sync.WaitGroup
		var mu sync.Mutex
		var oks, overlaps, others int
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				err := s.WriteFragment("t", int(sc.Version), &Fragment{
					Key: "same-key", NumRows: 1,
					Columns: map[int][]Value{fid: {int64(w)}},
				})
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err == nil:
					oks++
				case CodeOf(err) == ErrCodeConflict:
					if e := err.(*Error); e.Reason == string(ConflictFragmentOverlap) {
						overlaps++
					} else {
						others++
						t.Logf("冲突判定: writer=%d 非预期冲突原因 %s", w, e.Reason)
					}
				default:
					others++
					t.Logf("冲突判定: writer=%d 非预期错误 %v", w, err)
				}
			}(w)
		}
		wg.Wait()
		t.Logf("run=%d 输入: %d 个并发写同一键 -> 成功=%d 键重叠冲突=%d 其他=%d",
			run, writers, oks, overlaps, others)
		if oks != 1 || overlaps != writers-1 || others != 0 {
			t.Fatalf("run=%d: 成功=%d 重叠=%d 其他=%d, want 1/%d/0",
				run, oks, overlaps, others, writers-1)
		}

		// 最终状态一致：表里只有一行。
		sn, _ := s.Snapshot("t")
		res, err := Execute(context.Background(), sn, &Query{
			Aggregates: []Aggregate{{Kind: AggCount}},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if res.Values[0] != int64(1) {
			t.Fatalf("run=%d: 最终行数=%v, want 1", run, res.Values[0])
		}
	}
}

// TestConcurrentReadWriteConsistency 并发读写期间，每次查询结果
// 都等于某个已提交版本的结果（不会出现部分片段的中间态）。
func TestConcurrentReadWriteConsistency(t *testing.T) {
	s := newTestStore(t)
	sc := mustCreate(t, s, "t", Field{Name: "v", Type: TypeInt64})
	fid := sc.FieldByName("v").ID

	const fragments = 20
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < fragments; i++ {
			err := s.WriteFragment("t", int(sc.Version), &Fragment{
				Key: fmt.Sprintf("p%d", i), NumRows: 1,
				Columns: map[int][]Value{fid: {int64(1)}},
			})
			if err != nil {
				t.Errorf("write p%d: %v", i, err)
				return
			}
		}
	}()

	// 并发读：count 必须是 0..fragments 的整数（每个片段一行，原子可见）。
	reads := 0
	for {
		select {
		case <-done:
			// 最终再确认一次完整状态。
			sn, _ := s.Snapshot("t")
			res, err := Execute(context.Background(), sn, &Query{
				Aggregates: []Aggregate{{Kind: AggCount}},
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("结果: 最终 count=%v, 期间完成一致读 %d 次", res.Values[0], reads)
			if res.Values[0] != int64(fragments) {
				t.Fatalf("最终 count=%v, want %d", res.Values[0], fragments)
			}
			return
		default:
		}
		sn, err := s.Snapshot("t")
		if err != nil {
			t.Fatal(err)
		}
		res, err := Execute(context.Background(), sn, &Query{
			Aggregates: []Aggregate{{Kind: AggCount}},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		c := res.Values[0].(int64)
		if c < 0 || c > int64(fragments) {
			t.Fatalf("读到不一致状态 count=%d", c)
		}
		reads++
	}
}

// TestStaleSchemaVersionWriteRejected 结构演进后，基于旧版本的
// 写入被拒绝并标记为 STALE_SCHEMA_VERSION。
func TestStaleSchemaVersionWriteRejected(t *testing.T) {
	s := newTestStore(t)
	sc := mustCreate(t, s, "t", Field{Name: "v", Type: TypeInt64})
	fid := sc.FieldByName("v").ID

	if _, err := s.EvolveSchema("t", sc.Version, SchemaChange{
		Kind: ChangeAddField, Field: Field{Name: "w", Type: TypeInt64},
	}); err != nil {
		t.Fatal(err)
	}

	err := s.WriteFragment("t", int(sc.Version), &Fragment{
		Key: "p0", NumRows: 1, Columns: map[int][]Value{fid: {int64(1)}},
	})
	t.Logf("冲突判定: 基于旧版本写入 -> code=%q err=%v", CodeOf(err), err)
	if CodeOf(err) != ErrCodeConflict {
		t.Fatalf("应返回 CONFLICT, got %v", err)
	}
	if e := err.(*Error); e.Reason != string(ConflictSchemaVersion) {
		t.Fatalf("冲突原因应为 STALE_SCHEMA_VERSION, got %s", e.Reason)
	}
}
