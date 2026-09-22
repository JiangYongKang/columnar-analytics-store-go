package store

import (
	"context"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func mustCreate(t *testing.T, s *Store, name string, fields ...Field) *Schema {
	t.Helper()
	sc := &Schema{Fields: fields}
	if err := s.CreateTable(name, sc); err != nil {
		t.Fatalf("create table: %v", err)
	}
	sn, err := s.Snapshot(name)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	return sn.Schema()
}

// TestSchemaEvolutionAddDropRename 覆盖新增/删除/重命名字段后
// 历史数据的读取语义：新增列在历史片段为 NULL，重命名按 ID 命中，
// 删除列不再可见但历史数据保留。
func TestSchemaEvolutionAddDropRename(t *testing.T) {
	s := newTestStore(t)
	sc := mustCreate(t, s, "t",
		Field{Name: "id", Type: TypeInt64},
		Field{Name: "price", Type: TypeFloat64},
	)
	idID := sc.FieldByName("id").ID
	priceID := sc.FieldByName("price").ID
	t.Logf("输入: 初始结构 id=%d price=%d", idID, priceID)

	// v1 写入历史片段（只有 id/price 两列）。
	err := s.WriteFragment("t", sc.Version, &Fragment{
		Key: "part-0", NumRows: 2,
		Columns: map[int][]Value{
			idID:    {int64(1), int64(2)},
			priceID: {9.5, 7.5},
		},
	})
	if err != nil {
		t.Fatalf("write v1 fragment: %v", err)
	}

	// 新增字段 tag。
	sc2, err := s.EvolveSchema("t", sc.Version, SchemaChange{
		Kind: ChangeAddField, Field: Field{Name: "tag", Type: TypeString},
	})
	if err != nil {
		t.Fatalf("add field: %v", err)
	}
	tagID := sc2.FieldByName("tag").ID
	t.Logf("变更: add tag -> id=%d, 结构版本 %d", tagID, sc2.Version)

	// v2 写入新片段（含 tag 列）。
	err = s.WriteFragment("t", sc2.Version, &Fragment{
		Key: "part-1", NumRows: 1,
		Columns: map[int][]Value{
			idID:    {int64(3)},
			priceID: {1.0},
			tagID:   {"new"},
		},
	})
	if err != nil {
		t.Fatalf("write v2 fragment: %v", err)
	}

	// 重命名 price -> amount。
	sc3, err := s.EvolveSchema("t", sc2.Version, SchemaChange{
		Kind: ChangeRenameField, OldName: "price", NewName: "amount",
	})
	if err != nil {
		t.Fatalf("rename: %v", err)
	}
	t.Logf("变更: rename price->amount, 结构版本 %d", sc3.Version)

	// 删除 tag。
	sc4, err := s.EvolveSchema("t", sc3.Version, SchemaChange{
		Kind: ChangeDropField, OldName: "tag",
	})
	if err != nil {
		t.Fatalf("drop: %v", err)
	}
	t.Logf("变更: drop tag, 结构版本 %d", sc4.Version)

	// 在最新快照上按新名 amount 聚合：历史片段的 price 列应按 ID 命中。
	sn, err := s.Snapshot("t")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	q := &Query{
		Aggregates: []Aggregate{
			{Kind: AggSum, Field: "amount"},
			{Kind: AggCount},
		},
	}
	res, err := Execute(context.Background(), sn, q, nil)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	t.Logf("结果: sum(amount)=%v count=%v", res.Values[0], res.Values[1])
	if res.Values[0] != 18.0 {
		t.Fatalf("sum(amount) = %v, want 18.0 (历史 price 列应按 ID 命中)", res.Values[0])
	}
	if res.Values[1] != int64(3) {
		t.Fatalf("count = %v, want 3", res.Values[1])
	}

	// 已删除字段 tag 不可再查询。
	q2 := &Query{Aggregates: []Aggregate{{Kind: AggCount, Field: "tag"}}}
	_, err = Execute(context.Background(), sn, q2, nil)
	t.Logf("错误归一化: 查询已删除字段 -> code=%q err=%v", CodeOf(err), err)
	if CodeOf(err) != ErrCodeSchemaIncompatible {
		t.Fatalf("查询已删除字段应返回 SCHEMA_INCOMPATIBLE, got %v", err)
	}
}

// TestSchemaEvolutionRejectsIncompatible 不兼容变更必须被明确拒绝。
func TestSchemaEvolutionRejectsIncompatible(t *testing.T) {
	s := newTestStore(t)
	sc := mustCreate(t, s, "t", Field{Name: "id", Type: TypeInt64})

	cases := []struct {
		name   string
		change SchemaChange
	}{
		{"重名新增", SchemaChange{Kind: ChangeAddField, Field: Field{Name: "id", Type: TypeBool}}},
		{"未知类型", SchemaChange{Kind: ChangeAddField, Field: Field{Name: "x", Type: "decimal"}}},
		{"删除不存在字段", SchemaChange{Kind: ChangeDropField, OldName: "nope"}},
		{"重命名不存在字段", SchemaChange{Kind: ChangeRenameField, OldName: "nope", NewName: "y"}},
	}
	for _, tc := range cases {
		_, err := s.EvolveSchema("t", sc.Version, tc.change)
		t.Logf("输入: %s -> code=%q err=%v", tc.name, CodeOf(err), err)
		if CodeOf(err) != ErrCodeSchemaIncompatible {
			t.Fatalf("%s: 应返回 SCHEMA_INCOMPATIBLE, got %v", tc.name, err)
		}
	}

	// 过期版本提交结构变更 -> 冲突而非不兼容。
	if _, err := s.EvolveSchema("t", sc.Version, SchemaChange{
		Kind: ChangeAddField, Field: Field{Name: "a", Type: TypeInt64},
	}); err != nil {
		t.Fatalf("first evolve: %v", err)
	}
	_, err := s.EvolveSchema("t", sc.Version, SchemaChange{
		Kind: ChangeAddField, Field: Field{Name: "b", Type: TypeInt64},
	})
	t.Logf("冲突判定: 过期版本演进 -> code=%q reason=%v", CodeOf(err), err)
	if CodeOf(err) != ErrCodeConflict {
		t.Fatalf("过期版本演进应返回 CONFLICT, got %v", err)
	}
	if e, ok := err.(*Error); !ok || e.Reason != string(ConflictSchemaVersion) {
		t.Fatalf("冲突原因应为 STALE_SCHEMA_VERSION, got %v", err)
	}
}

// TestEvolutionReproducibleAfterReopen 重启后结构演进历史可复现。
func TestEvolutionReproducibleAfterReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	sc := mustCreate(t, s, "t", Field{Name: "id", Type: TypeInt64})
	idID := sc.FieldByName("id").ID
	if err := s.WriteFragment("t", sc.Version, &Fragment{
		Key: "p0", NumRows: 1, Columns: map[int][]Value{idID: {int64(42)}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EvolveSchema("t", sc.Version, SchemaChange{
		Kind: ChangeAddField, Field: Field{Name: "v", Type: TypeFloat64},
	}); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	sn, err := s2.Snapshot("t")
	if err != nil {
		t.Fatal(err)
	}
	res, err := Execute(context.Background(), sn, &Query{
		Aggregates: []Aggregate{{Kind: AggSum, Field: "id"}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("重启后复现: sum(id)=%v schemaVersion=%d", res.Values[0], sn.Schema().Version)
	if res.Values[0] != 42.0 {
		t.Fatalf("重启后 sum(id)=%v, want 42", res.Values[0])
	}
}
