package colstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

// setupSimpleStore 建立表 (id int64 NOT NULL default 0, v int64 null)。
func setupSimpleStore(t *testing.T, opts Options) (*Store, *ColumnDef, *ColumnDef) {
	ctx := context.Background()
	s := testStore(t, opts)
	if _, err := s.AddColumn(ctx, "id", TypeInt64, ColumnAddOptions{Nullable: false, HasDefault: true, Default: iv(0)}); err != nil {
		t.Fatalf("add id: %v", err)
	}
	if _, err := s.AddColumn(ctx, "v", TypeInt64, ColumnAddOptions{Nullable: true}); err != nil {
		t.Fatalf("add v: %v", err)
	}
	sc := s.Snapshot().Schema()
	idCol, _ := sc.columnByName("id")
	vCol, _ := sc.columnByName("v")
	return s, idCol, vCol
}

// TestConcurrentWritersConflict 多个并发写事务：恰好一个成功，其余必须以
// concurrent_commit 被拒绝；冲突分类稳定可复现。
func TestConcurrentWritersConflict(t *testing.T) {
	ctx := context.Background()
	s, idCol, vCol := setupSimpleStore(t, Options{})

	const n = 8
	var wg sync.WaitGroup
	results := make([]error, n)
	gens := make([]int, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tx := s.BeginWrite()
			if err := tx.Add(Row{idCol.ID: iv(int64(i + 1)), vCol.ID: iv(int64(i * 10))}); err != nil {
				results[i] = err
				return
			}
			<-start // 同时冲向提交点，制造真实竞争
			g, err := tx.Commit(ctx)
			gens[i] = g
			results[i] = err
		}(i)
	}
	close(start)
	wg.Wait()

	success := 0
	for i, err := range results {
		ce := Normalize(err)
		if err == nil {
			success++
			logf(t, "CONFLICT writer %d: COMMITTED generation=%d", i, gens[i])
			continue
		}
		if ce.Category != CatConflict || ce.Reason != string(ConflictConcurrentCommit) {
			t.Fatalf("writer %d: want conflict/concurrent_commit, got %v", i, ce)
		}
		logf(t, "CONFLICT writer %d: REJECTED category=%s reason=%s normalized={%s}",
			i, ce.Category, ce.Reason, ce.Error())
	}
	if success != 1 {
		t.Fatalf("exactly one writer must succeed, got %d", success)
	}
	snap := s.Snapshot()
	if snap.SegmentCount() != 1 {
		t.Fatalf("rejected txns must leave no segments, got %d", snap.SegmentCount())
	}
	logf(t, "CONFLICT final state: generation=%d segments=%d", snap.Generation(), snap.SegmentCount())
}

// TestSnapshotIsolation 快照必须固定：拿到快照后并发写入，旧快照看不到新数据。
func TestSnapshotIsolation(t *testing.T) {
	ctx := context.Background()
	s, idCol, vCol := setupSimpleStore(t, Options{})

	commitRows := func(ids ...int64) int {
		tx := s.BeginWrite()
		rows := make([]Row, 0, len(ids))
		for i, x := range ids {
			rows = append(rows, Row{idCol.ID: iv(x), vCol.ID: iv(int64(i))})
		}
		if err := tx.Add(rows...); err != nil {
			t.Fatalf("add: %v", err)
		}
		g, err := tx.Commit(ctx)
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
		return g
	}

	commitRows(1)
	snap1 := s.Snapshot() // 固定在 1 个片段
	commitRows(2, 3)
	snap2 := s.Snapshot() // 固定在 2 个片段

	r1, err := snap1.Query(ctx, Query{})
	if err != nil {
		t.Fatalf("snap1 query: %v", err)
	}
	r2, err := snap2.Query(ctx, Query{})
	if err != nil {
		t.Fatalf("snap2 query: %v", err)
	}
	logf(t, "SNAPSHOT old gen=%d sees rowsScanned=%d; new gen=%d sees rowsScanned=%d",
		snap1.Generation(), r1.RowsScanned, snap2.Generation(), r2.RowsScanned)
	if r1.RowsScanned != 1 || r2.RowsScanned != 3 {
		t.Fatalf("snapshot isolation broken: %d vs %d", r1.RowsScanned, r2.RowsScanned)
	}
}

// TestConcurrentReadWhileWrite 读写并发：读者永不报错、永不脏读，
// 只能看到“提交前”或“提交后”的完整状态（行数只有 0 / 100 两种，不会是中间值）。
func TestConcurrentReadWhileWrite(t *testing.T) {
	ctx := context.Background()
	s, idCol, vCol := setupSimpleStore(t, Options{})

	var wg sync.WaitGroup
	const readers = 6
	const rowN = 100
	var obsMu sync.Mutex
	allowed := map[int64]int{}
	stop := make(chan struct{})

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				res, err := s.Snapshot().Query(ctx, Query{})
				if err != nil {
					t.Errorf("reader got unexpected error: %v", Normalize(err))
					return
				}
				obsMu.Lock()
				allowed[res.RowsScanned]++
				obsMu.Unlock()
			}
		}()
	}

	tx := s.BeginWrite()
	rows := make([]Row, 0, rowN)
	for i := 0; i < rowN; i++ {
		rows = append(rows, Row{idCol.ID: iv(int64(i + 1)), vCol.ID: iv(int64(i))})
	}
	if err := tx.Add(rows...); err != nil {
		t.Fatalf("add: %v", err)
	}
	g, err := tx.Commit(ctx)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	logf(t, "READWRITE writer committed %d rows at generation=%d", rowN, g)
	close(stop)
	wg.Wait()

	for n := range allowed {
		if n != 0 && n != rowN {
			t.Fatalf("dirty/partial read observed: %d rows (want 0 or %d)", n, rowN)
		}
	}
	logf(t, "READWRITE observations {0:before-commit, %d:after-commit}=%v", rowN, allowed)
	if len(allowed) == 0 {
		t.Fatal("no reader observations recorded")
	}
}

// TestSchemaConflictDuringWrite 写事务进行中发生 DDL，提交必须以
// schema_modified 被拒绝，且与普通并发提交原因可区分。
func TestSchemaConflictDuringWrite(t *testing.T) {
	ctx := context.Background()
	s, idCol, vCol := setupSimpleStore(t, Options{})

	tx := s.BeginWrite()
	if err := tx.Add(Row{idCol.ID: iv(1), vCol.ID: iv(9)}); err != nil {
		t.Fatalf("add: %v", err)
	}
	// 另一路先提交 DML 再提交 DDL，使代际与结构版本都推进。
	other := s.BeginWrite()
	if err := other.Add(Row{idCol.ID: iv(2), vCol.ID: iv(8)}); err != nil {
		t.Fatalf("other add: %v", err)
	}
	if _, err := other.Commit(ctx); err != nil {
		t.Fatalf("other commit: %v", err)
	}
	if _, err := s.AddColumn(ctx, "tag", TypeString, ColumnAddOptions{Nullable: true}); err != nil {
		t.Fatalf("ddl: %v", err)
	}

	g, err := tx.Commit(ctx)
	ce := Normalize(err)
	if ce == nil || ce.Category != CatConflict || ce.Reason != string(ConflictSchemaModified) {
		t.Fatalf("want conflict/schema_modified, got gen=%d err=%v", g, ce)
	}
	logf(t, "CONFLICT DDL-vs-DML: REJECTED category=%s reason=%s detail=%s",
		ce.Category, ce.Reason, ce.Error())
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("errors.Is(err, ErrConflict) must hold")
	}
}

// TestStaleSnapshotsRepeatedReproducible 重复运行结论一致：多次重置同一序列，
// 冲突计数与最终片段数确定。
func TestStaleSnapshotsRepeatedReproducible(t *testing.T) {
	for round := 0; round < 3; round++ {
		ctx := context.Background()
		s, idCol, vCol := setupSimpleStore(t, Options{})
		var conflicts, oks int
		var mu sync.Mutex
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				tx := s.BeginWrite()
				_ = tx.Add(Row{idCol.ID: iv(int64(i + 1)), vCol.ID: iv(int64(i))})
				<-start
				_, err := tx.Commit(ctx)
				mu.Lock()
				if err == nil {
					oks++
				} else if Normalize(err).Reason == string(ConflictConcurrentCommit) {
					conflicts++
				}
				mu.Unlock()
			}(i)
		}
		close(start)
		wg.Wait()
		logf(t, "REPRO round=%d committed=%d conflicts=%d finalSegments=%d",
			round, oks, conflicts, s.Snapshot().SegmentCount())
		if oks != 1 || conflicts != 4 || s.Snapshot().SegmentCount() != 1 {
			t.Fatalf("round %d non-reproducible: oks=%d conflicts=%d segs=%d",
				round, oks, conflicts, s.Snapshot().SegmentCount())
		}
		_ = fmt.Sprintf
	}
}
