package colstore

import (
	"context"
	"path/filepath"
	"testing"
)

func logf(t *testing.T, format string, args ...any) {
	t.Helper()
	t.Logf(format, args...)
}

func testStore(t *testing.T, opts Options) *Store {
	t.Helper()
	if opts.Dir == "" {
		opts.Dir = t.TempDir()
	} else {
		opts.Dir = filepath.Join(t.TempDir(), opts.Dir)
	}
	s, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func iv(x int64) Value   { return Value{Type: TypeInt64, Int: x} }
func fv(x float64) Value { return Value{Type: TypeFloat64, Float: x} }
func sv(x string) Value  { return Value{Type: TypeString, Str: x} }
func bv(x bool) Value    { return Value{Type: TypeBool, Bool: x} }

// TestBasicWriteAndSnapshotQuery 覆盖主路径：写入 -> 快照查询 -> 聚合。
func TestBasicWriteAndSnapshotQuery(t *testing.T) {
	ctx := context.Background()
	s := testStore(t, Options{})

	gen, err := s.AddColumn(ctx, "id", TypeInt64, ColumnAddOptions{Nullable: false, HasDefault: true, Default: iv(0)})
	if err != nil {
		t.Fatalf("add id: %v", err)
	}
	if _, err := s.AddColumn(ctx, "name", TypeString, ColumnAddOptions{Nullable: true}); err != nil {
		t.Fatalf("add name: %v", err)
	}
	if _, err := s.AddColumn(ctx, "score", TypeFloat64, ColumnAddOptions{Nullable: true}); err != nil {
		t.Fatalf("add score: %v", err)
	}
	logf(t, "INPUT schema after DDL generation=%d schemaVersion=%d columns=%v",
		gen, s.Snapshot().SchemaVersion(), colNames(s.Snapshot().Schema()))

	snap := s.Snapshot()
	schema := snap.Schema()
	idCol, _ := schema.columnByName("id")
	nameCol, _ := schema.columnByName("name")
	scoreCol, _ := schema.columnByName("score")

	tx := s.BeginWrite()
	err = tx.Add(
		Row{idCol.ID: iv(1), nameCol.ID: sv("alice"), scoreCol.ID: fv(10)},
		Row{idCol.ID: iv(2), nameCol.ID: sv("bob"), scoreCol.ID: fv(20)},
		Row{idCol.ID: iv(3), nameCol.ID: sv("carol"), scoreCol.ID: fv(30)},
	)
	if err != nil {
		t.Fatalf("add rows: %v", err)
	}
	g1, err := tx.Commit(ctx)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	logf(t, "INPUT committed first segment at generation=%d", g1)

	// 过滤 + 投影
	res, err := s.Snapshot().Query(ctx, Query{
		Filters:     []Filter{{Column: "id", Op: OpGe, Value: iv(2)}},
		Projections: []string{"name", "score"},
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	logf(t, "RESULT filter id>=2: segments=%d rowsScanned=%d matched=%d rows=%v",
		res.SegmentsScanned, res.RowsScanned, res.MatchedRows, formatRows(res.Columns, res.Rows))
	if res.MatchedRows != 2 || len(res.Rows) != 2 {
		t.Fatalf("want 2 matched rows, got %d", res.MatchedRows)
	}
	if res.Columns[0] != "name" || res.Rows[0][0].Str != "bob" {
		t.Fatalf("unexpected first row: %v", res.Rows[0])
	}

	// 聚合
	agg, err := s.Snapshot().Query(ctx, Query{
		Filters:    []Filter{{Column: "id", Op: OpGe, Value: iv(2)}},
		Aggregates: []AggSpec{{Func: AggCount}, {Func: AggSum, Column: "score"}, {Func: AggAvg, Column: "score"}, {Func: AggMin, Column: "score"}, {Func: AggMax, Column: "score"}},
	})
	if err != nil {
		t.Fatalf("agg query: %v", err)
	}
	for _, a := range agg.Aggregates {
		logf(t, "AGG %s(%s) = %s count=%d", a.Func, a.Column, a.Value.String(), a.Count)
	}
	if agg.Aggregates[0].Value.Int != 2 {
		t.Fatalf("count want 2 got %s", agg.Aggregates[0].Value)
	}
	if agg.Aggregates[1].Value.Float != 50 {
		t.Fatalf("sum want 50 got %s", agg.Aggregates[1].Value)
	}
	if agg.Aggregates[2].Value.Float != 25 {
		t.Fatalf("avg want 25 got %s", agg.Aggregates[2].Value)
	}
}

func colNames(sc Schema) []string {
	var out []string
	for _, c := range sc.Columns {
		state := "live"
		if c.Dropped {
			state = "dropped"
		}
		out = append(out, string(c.Name)+":"+state)
	}
	return out
}

func formatRows(cols []string, rows [][]Value) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		cells := make([]string, len(r))
		for i, v := range r {
			cells[i] = cols[i] + "=" + v.String()
		}
		out = append(out, join(cells, ","))
	}
	return out
}

func join(ss []string, sep string) string {
	out := ""
	for i, s := range ss {
		if i > 0 {
			out += sep
		}
		out += s
	}
	return out
}
