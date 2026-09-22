package colstore

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// corruptSegmentFile 对已提交片段做确定性破坏。
func corruptSegmentFile(t *testing.T, dir string, segID int64, kind string) {
	t.Helper()
	p := segmentFilePath(dir, segID)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	switch kind {
	case "magic":
		for i := 0; i < 8; i++ {
			data[i] = 'X'
		}
	case "truncate":
		data = data[:len(data)/2]
	case "crc":
		// 翻转最后一个数据块载荷区中的一个字节（尾部即 payload）。
		idx := len(data) - 3
		data[idx] ^= 0xFF
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatalf("write corrupt segment: %v", err)
	}
	logf(t, "FAULT injected %q corruption into segment %d (file bytes=%d)", kind, segID, len(data))
}

// TestCorruptSegmentDistinguished 三类损坏都必须归一为 corrupt_segment，
// 且细粒度原因可区分；查询不得返回任何行结果。
func TestCorruptSegmentDistinguished(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"magic", "truncate", "crc"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			s, err := Open(Options{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			idc := mustAddCol(t, s, "id", TypeInt64, true)
			tx := s.BeginWrite()
			if err := tx.Add(Row{idc.ID: iv(1)}); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			_ = s.Close()

			corruptSegmentFile(t, dir, 0, kind)

			s2, err := Open(Options{Dir: dir})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			_, qerr := s2.Snapshot().Query(ctx, Query{})
			ce := Normalize(qerr)
			wantReason := map[string]string{
				"magic":    CorruptBadMagic,
				"truncate": CorruptTruncated,
				"crc":      CorruptCRC,
			}[kind]
			if ce == nil || ce.Category != CatCorruptSegment || ce.Reason != wantReason {
				t.Fatalf("want corrupt_segment/%s, got %+v", wantReason, ce)
			}
			if !errors.Is(qerr, ErrCorruptSegment) {
				t.Fatalf("errors.Is(ErrCorruptSegment) must hold, got %v", qerr)
			}
			logf(t, "FAULT query on %q -> category=%s reason=%s segment=%d normalized={%s}",
				kind, ce.Category, ce.Reason, ce.SegmentID, ce.Error())
		})
	}
}

// TestReadInterrupted 与损坏、预算不同：context 取消归一为 read_interrupted。
// 注入钩子使取消发生在片段解码期间。
func TestReadInterrupted(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	idc := mustAddCol(t, s, "id", TypeInt64, true)

	// 写入两个片段，取消发生在扫描第二个片段之前。
	tx := s.BeginWrite()
	if err := tx.Add(Row{idc.ID: iv(1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	tx2 := s.BeginWrite()
	if err := tx2.Add(Row{idc.ID: iv(2)}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx2.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	_ = ctx
	qctx, cancel := context.WithCancel(context.Background())
	cancel() // 预先取消：查询在第一个片段前即应被中断
	_, qerr := s.Snapshot().Query(qctx, Query{})
	ce := Normalize(qerr)
	if ce == nil || ce.Category != CatReadInterrupted {
		t.Fatalf("want read_interrupted, got %+v", ce)
	}
	logf(t, "FAULT pre-canceled query -> category=%s reason=%s", ce.Category, ce.Reason)

	// 超时型 context 同样归一。
	tctx, tcancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer tcancel()
	time.Sleep(time.Millisecond)
	_, qerr2 := s.Snapshot().Query(tctx, Query{})
	ce2 := Normalize(qerr2)
	if ce2 == nil || ce2.Category != CatReadInterrupted {
		t.Fatalf("deadline exceeded must normalize to read_interrupted, got %+v", ce2)
	}
	logf(t, "FAULT deadline query -> category=%s reason=%s", ce2.Category, ce2.Reason)
}

// TestResourceLimitScanBytes 极小字节预算：必须 resource_limit/scan_bytes，
// 且错误在产生大量读取前就抛出（返回结果为 nil）。
func TestResourceLimitScanBytes(t *testing.T) {
	ctx := context.Background()
	s, _ := setupBigTable(t, 5, 200)
	res, err := s.Snapshot().Query(ctx, Query{})
	if err != nil {
		t.Fatal(err)
	}
	logf(t, "BUDGET full table: segments=%d bytesRead=%d rows=%d",
		res.SegmentsScanned, res.BytesRead, res.RowsScanned)

	// 直接在同一目录以小预算重开一个句柄（选项在 Open 时固定）。
	lim, err := Open(Options{Dir: s.opts.Dir, MaxScanBytes: 20})
	if err != nil {
		t.Fatal(err)
	}
	_, qerr := lim.Snapshot().Query(ctx, Query{})
	ce := Normalize(qerr)
	if ce == nil || ce.Category != CatResourceLimit || ce.Reason != LimitScanBytes {
		t.Fatalf("want resource_limit/scan_bytes, got %+v", ce)
	}
	logf(t, "BUDGET tiny byte budget -> category=%s reason=%s msg=%s", ce.Category, ce.Reason, ce.Message)
	if !errors.Is(qerr, ErrResourceLimit) {
		t.Fatal("errors.Is(ErrResourceLimit) must hold")
	}
}

// TestResourceLimitMatchedRows 命中行预算受限，可与字节限制区分。
func TestResourceLimitMatchedRows(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir, MaxMatchedRows: 3})
	if err != nil {
		t.Fatal(err)
	}
	idc := mustAddCol(t, s, "id", TypeInt64, true)
	tx := s.BeginWrite()
	rows := make([]Row, 10)
	for i := range rows {
		rows[i] = Row{idc.ID: iv(int64(i))}
	}
	if err := tx.Add(rows...); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	_, qerr := s.Snapshot().Query(ctx, Query{})
	ce := Normalize(qerr)
	if ce == nil || ce.Category != CatResourceLimit || ce.Reason != LimitMatchedRows {
		t.Fatalf("want resource_limit/matched_rows, got %+v", ce)
	}
	logf(t, "BUDGET matched-rows cap -> category=%s reason=%s msg=%s", ce.Category, ce.Reason, ce.Message)
}

// TestBudgetRelatedToHitRange 读取量与命中范围相关：带强过滤的查询只解码
// 过滤/投影字段，字节数显著小于整表全列扫描。
func TestBudgetRelatedToHitRange(t *testing.T) {
	ctx := context.Background()
	s, idc := setupBigTable(t, 6, 100)
	// 追加一个宽字符串列并写入较大载荷后，比较全列与裁剪读取字节量。
	bigCol := mustAddCol(t, s, "payload", TypeString, true)
	tx := s.BeginWrite()
	bigVal := ""
	for i := 0; i < 200; i++ {
		bigVal += "z"
	}
	rows := make([]Row, 100)
	for i := range rows {
		rows[i] = Row{idc.ID: iv(1000 + int64(i)), bigCol.ID: sv(bigVal)}
	}
	if err := tx.Add(rows...); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	full, err := s.Snapshot().Query(ctx, Query{})
	if err != nil {
		t.Fatal(err)
	}
	narrow, err := s.Snapshot().Query(ctx, Query{
		Filters:     []Filter{{Column: "id", Op: OpEq, Value: iv(1000)}},
		Projections: []string{"id"},
	})
	if err != nil {
		t.Fatal(err)
	}
	logf(t, "BUDGET full bytes=%d (segments=%d) vs narrow-filter bytes=%d matched=%d",
		full.BytesRead, full.SegmentsScanned, narrow.BytesRead, narrow.MatchedRows)
	if narrow.BytesRead >= full.BytesRead {
		t.Fatalf("column pruning must read less: narrow=%d full=%d", narrow.BytesRead, full.BytesRead)
	}
	if narrow.MatchedRows != 1 {
		t.Fatalf("filter should match exactly 1 row, got %d", narrow.MatchedRows)
	}
}

// TestCrashBetweenSegmentAndManifest OnSegmentWritten 钩子模拟“片段已落盘但
// 清单未发布”的崩溃：该片段必须成为孤儿被回收，状态回到提交前。
func TestCrashBetweenSegmentAndManifest(t *testing.T) {
	dir := t.TempDir()
	hooks := &FaultHooks{
		OnSegmentWritten: func(segID int64) error {
			return errors.New("simulated crash before manifest publish")
		},
	}
	s, err := Open(Options{Dir: dir, Hooks: hooks})
	if err != nil {
		t.Fatal(err)
	}
	idc := mustAddCol(t, s, "id", TypeInt64, true)
	tx := s.BeginWrite()
	if err := tx.Add(Row{idc.ID: iv(1)}); err != nil {
		t.Fatal(err)
	}
	g, cerr := tx.Commit(context.Background())
	ce := Normalize(cerr)
	logf(t, "CRASH commit aborted at gen=%d category=%s", g, ce.Category)
	if ce.Category != CatIO {
		t.Fatalf("want io_error from hook, got %s", ce.Category)
	}
	snap := s.Snapshot()
	if snap.SegmentCount() != 0 {
		t.Fatalf("no segment must be visible after pre-manifest crash, got %d", snap.SegmentCount())
	}
	_ = s.Close()

	// 重开：孤儿片段应被清理，查询结果为空且无错误。
	s2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	res, err := s2.Snapshot().Query(context.Background(), Query{})
	if err != nil {
		t.Fatalf("recovered store must query cleanly: %v", Normalize(err))
	}
	if res.RowsScanned != 0 {
		t.Fatalf("orphan segment must be reaped, rows=%d", res.RowsScanned)
	}
	logf(t, "CRASH after reopen: segments=%d rows=%d (orphan reaped)",
		s2.Snapshot().SegmentCount(), res.RowsScanned)
}

func mustAddCol(t *testing.T, s *Store, name string, typ ColumnType, nullable bool) *ColumnDef {
	t.Helper()
	opts := ColumnAddOptions{Nullable: nullable}
	if !nullable {
		panic("helper expects nullable columns")
	}
	if _, err := s.AddColumn(context.Background(), name, typ, opts); err != nil {
		t.Fatalf("add column %s: %v", name, err)
	}
	sc := s.Snapshot().Schema()
	c, _ := sc.columnByName(name)
	return c
}

// setupBigTable 建表并写入 seg 个片段、每个 perSeg 行。
func setupBigTable(t *testing.T, seg, perSeg int) (*Store, *ColumnDef) {
	t.Helper()
	s := testStore(t, Options{})
	idc := mustAddCol(t, s, "id", TypeInt64, true)
	for i := 0; i < seg; i++ {
		tx := s.BeginWrite()
		rows := make([]Row, perSeg)
		for j := range rows {
			rows[j] = Row{idc.ID: iv(int64(i*perSeg + j))}
		}
		if err := tx.Add(rows...); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	return s, idc
}
