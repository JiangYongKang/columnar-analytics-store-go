package colstore

import (
	"context"
	"errors"
	"testing"
)

// TestNormalizeDeterminism 同一错误多次归一化，大类/原因完全一致（可复现）。
func TestNormalizeDeterminism(t *testing.T) {
	ctx := context.Background()
	s, idc, _ := setupSimpleStore(t, Options{})

	// 1) 未知列 -> invalid_query/unknown_column
	_, err := s.Snapshot().Query(ctx, Query{Filters: []Filter{{Column: "ghost", Op: OpEq, Value: iv(1)}}})
	ce := Normalize(err)
	if ce.Category != CatInvalidQuery || ce.Reason != "unknown_column" {
		t.Fatalf("want invalid_query/unknown_column, got %+v", ce)
	}
	if !errors.Is(err, ErrInvalidQuery) {
		t.Fatal("errors.Is(ErrInvalidQuery) must hold")
	}
	logf(t, "NORMALIZE unknown-column filter -> %s/%s", ce.Category, ce.Reason)

	// 2) 非法聚合（对字符串求和）-> invalid_query/agg_type
	sc := s.Snapshot().Schema()
	nameC, _ := sc.columnByName("v")
	_ = nameC
	_, err = s.Snapshot().Query(ctx, Query{Aggregates: []AggSpec{{Func: AggSum, Column: "v"}}})
	if err != nil {
		t.Fatalf("sum over int64 must be allowed: %v", err)
	}

	// 3) 写非法输入：未知 FieldID -> invalid_input
	tx := s.BeginWrite()
	err = tx.Add(Row{FieldID(99999): iv(1)})
	ce = Normalize(err)
	if ce.Category != CatInvalidInput {
		t.Fatalf("want invalid_input, got %+v", ce)
	}
	logf(t, "NORMALIZE unknown-field write -> %s/%s", ce.Category, ce.Reason)
	tx.Rollback()

	// 4) 冲突错误的稳定分类
	tx1 := s.BeginWrite()
	tx2 := s.BeginWrite()
	if err := tx1.Add(Row{idc.ID: iv(1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx1.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Add(Row{idc.ID: iv(2)}); err != nil {
		t.Fatal(err)
	}
	_, cerr := tx2.Commit(ctx)
	ce1 := Normalize(cerr)
	ce2 := Normalize(cerr)
	if ce1.Category != CatConflict || ce1.Reason != string(ConflictConcurrentCommit) {
		t.Fatalf("want conflict/concurrent_commit, got %+v", ce1)
	}
	if ce1.Category != ce2.Category || ce1.Reason != ce2.Reason {
		t.Fatal("normalization must be deterministic")
	}
	logf(t, "NORMALIZE conflict -> %s/%s (repeat: %s/%s)",
		ce1.Category, ce1.Reason, ce2.Category, ce2.Reason)

	// 5) nil 归一为 nil
	if Normalize(nil) != nil {
		t.Fatal("Normalize(nil) must be nil")
	}
}

// TestQueryValidationCases 覆盖各类非法查询，均不得产生部分结果。
func TestQueryValidationCases(t *testing.T) {
	ctx := context.Background()
	s, _, _ := setupSimpleStore(t, Options{})
	cases := []struct {
		name   string
		q      Query
		reason string
	}{
		{"projection unknown", Query{Projections: []string{"nope"}}, "unknown_column"},
		{"bad operator", Query{Filters: []Filter{{Column: "id", Op: FilterOp("~"), Value: iv(1)}}}, "bad_operator"},
		{"type mismatch filter", Query{Filters: []Filter{{Column: "id", Op: OpEq, Value: sv("x")}}}, "type_mismatch"},
		{"unknown aggregate", Query{Aggregates: []AggSpec{{Func: AggFunc("median"), Column: "id"}}}, "bad_aggregate"},
		{"sum without column", Query{Aggregates: []AggSpec{{Func: AggSum}}}, "agg_requires_column"},
	}
	for _, tc := range cases {
		res, err := s.Snapshot().Query(ctx, tc.q)
		ce := Normalize(err)
		if ce == nil || ce.Category != CatInvalidQuery || ce.Reason != tc.reason {
			t.Fatalf("%s: want invalid_query/%s got %+v", tc.name, tc.reason, ce)
		}
		if res != nil {
			t.Fatalf("%s: invalid query must return nil result", tc.name)
		}
		logf(t, "NORMALIZE INPUT %-22q -> %s/%s", tc.name, ce.Category, ce.Reason)
	}
}

// TestEmptyStoreQuery 空表查询合法，返回空结果而非错误。
func TestEmptyStoreQuery(t *testing.T) {
	s := testStore(t, Options{})
	res, err := s.Snapshot().Query(context.Background(), Query{})
	if err != nil {
		t.Fatal(err)
	}
	if res.RowsScanned != 0 || res.SegmentsScanned != 0 {
		t.Fatalf("empty store must scan nothing, got %+v", res)
	}
	agg, err := s.Snapshot().Query(context.Background(), Query{Aggregates: []AggSpec{{Func: AggCount}}})
	if err != nil {
		t.Fatal(err)
	}
	if agg.Aggregates[0].Value.Int != 0 {
		t.Fatalf("count on empty table must be 0, got %s", agg.Aggregates[0].Value)
	}
	logf(t, "EMPTY count()=%s", agg.Aggregates[0].Value)
}
