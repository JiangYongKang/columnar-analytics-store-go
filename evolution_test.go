package colstore

import (
	"context"
	"testing"
)

// TestSchemaEvolutionAddRenameDrop 覆盖新增/重命名/删除后历史数据的确定语义。
func TestSchemaEvolutionAddRenameDrop(t *testing.T) {
	ctx := context.Background()
	s := testStore(t, Options{})

	if _, err := s.AddColumn(ctx, "id", TypeInt64, ColumnAddOptions{Nullable: false, HasDefault: true, Default: iv(0)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddColumn(ctx, "name", TypeString, ColumnAddOptions{Nullable: true}); err != nil {
		t.Fatal(err)
	}
	sc := s.Snapshot().Schema()
	idCol, _ := sc.columnByName("id")
	nameCol, _ := sc.columnByName("name")

	// 片段1：只有 id/name
	tx := s.BeginWrite()
	if err := tx.Add(Row{idCol.ID: iv(1), nameCol.ID: sv("alice")}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// 新增带默认值的 level 字段（历史行应为默认 5）
	if _, err := s.AddColumn(ctx, "level", TypeInt64, ColumnAddOptions{Nullable: false, HasDefault: true, Default: iv(5)}); err != nil {
		t.Fatal(err)
	}
	// 新增无默认值的 tag（可空，历史行应为 NULL）
	if _, err := s.AddColumn(ctx, "tag", TypeString, ColumnAddOptions{Nullable: true}); err != nil {
		t.Fatal(err)
	}

	sc2 := s.Snapshot().Schema()
	levelCol, _ := sc2.columnByName("level")
	tagCol, _ := sc2.columnByName("tag")
	tx2 := s.BeginWrite()
	if err := tx2.Add(Row{idCol.ID: iv(2), nameCol.ID: sv("bob"), levelCol.ID: iv(9), tagCol.ID: sv("vip")}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx2.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	res, err := s.Snapshot().Query(ctx, Query{Projections: []string{"id", "name", "level", "tag"}})
	if err != nil {
		t.Fatal(err)
	}
	logf(t, "EVOLUTION after add: scanned=%d rows=%v", res.RowsScanned, formatRows(res.Columns, res.Rows))
	if len(res.Rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(res.Rows))
	}
	// 历史行：level=默认5，tag=NULL
	if res.Rows[0][2].Int != 5 || !res.Rows[0][3].Null {
		t.Fatalf("historical row default semantics wrong: level=%s tag=%s", res.Rows[0][2], res.Rows[0][3])
	}
	// 新行：9 / vip
	if res.Rows[1][2].Int != 9 || res.Rows[1][3].Str != "vip" {
		t.Fatalf("new row semantics wrong: %s %s", res.Rows[1][2], res.Rows[1][3])
	}

	// 重命名 name -> display_name，FieldID 不变，历史/新行均可读
	if _, err := s.RenameColumn(ctx, "name", "display_name"); err != nil {
		t.Fatal(err)
	}
	res2, err := s.Snapshot().Query(ctx, Query{Projections: []string{"id", "display_name", "level"}})
	if err != nil {
		t.Fatal(err)
	}
	logf(t, "EVOLUTION after rename: rows=%v", formatRows(res2.Columns, res2.Rows))
	if res2.Rows[0][1].Str != "alice" || res2.Rows[1][1].Str != "bob" {
		t.Fatalf("rename broke historical data: %v", res2.Rows)
	}
	// 旧名字查询必须报错（不静默错列）
	if _, err := s.Snapshot().Query(ctx, Query{Projections: []string{"name"}}); err == nil {
		t.Fatal("querying old column name after rename must fail")
	} else {
		logf(t, "EVOLUTION old name rejected: %s", Normalize(err).Error())
	}

	// 删除 tag：查询不可见，但历史片段文件仍在（片段数不变）
	if _, err := s.DropColumn(ctx, "tag"); err != nil {
		t.Fatal(err)
	}
	if s.Snapshot().SegmentCount() != 2 {
		t.Fatalf("drop must not delete segment files, segs=%d", s.Snapshot().SegmentCount())
	}
	res3, err := s.Snapshot().Query(ctx, Query{})
	if err != nil {
		t.Fatal(err)
	}
	logf(t, "EVOLUTION after drop: visible columns=%v rowsScanned=%d", res3.Columns, res3.RowsScanned)
	for _, c := range res3.Columns {
		if c == "tag" {
			t.Fatal("dropped column must not be visible")
		}
	}
	if _, err := s.DropColumn(ctx, "tag"); err == nil {
		t.Fatal("dropping an already-dropped column must be rejected")
	} else {
		logf(t, "EVOLUTION double-drop rejected: category=%s reason=%s",
			Normalize(err).Category, Normalize(err).Reason)
	}
}

// TestSchemaEvolutionIncompatible 不兼容变更必须被明确拒绝且不推进版本。
func TestSchemaEvolutionIncompatible(t *testing.T) {
	ctx := context.Background()
	s := testStore(t, Options{})
	g0, _ := s.AddColumn(ctx, "a", TypeInt64, ColumnAddOptions{Nullable: true})

	cases := []struct {
		name   string
		fn     func() (int, error)
		reason SchemaChangeReason
	}{
		{"duplicate column", func() (int, error) {
			return s.AddColumn(ctx, "a", TypeString, ColumnAddOptions{Nullable: true})
		}, SchemaDuplicateColumn},
		{"rename to existing", func() (int, error) {
			g, _ := s.AddColumn(ctx, "b", TypeInt64, ColumnAddOptions{Nullable: true})
			_ = g
			return s.RenameColumn(ctx, "a", "b")
		}, SchemaRenameTargetExists},
		{"rename missing", func() (int, error) {
			return s.RenameColumn(ctx, "ghost", "x")
		}, SchemaColumnNotFound},
		{"not null without default", func() (int, error) {
			return s.AddColumn(ctx, "c", TypeInt64, ColumnAddOptions{Nullable: false})
		}, SchemaNotNullWithoutValue},
		{"default type mismatch", func() (int, error) {
			return s.AddColumn(ctx, "d", TypeInt64, ColumnAddOptions{Nullable: true, HasDefault: true, Default: sv("x")})
		}, SchemaTypeMismatch},
	}
	for _, tc := range cases {
		_, err := tc.fn()
		ce := Normalize(err)
		if ce == nil {
			t.Fatalf("%s: expected rejection, got nil", tc.name)
		}
		if ce.Category != CatIncompatibleSchema || ce.Reason != string(tc.reason) {
			t.Fatalf("%s: want incompatible_schema/%s got %s/%s",
				tc.name, tc.reason, ce.Category, ce.Reason)
		}
		logf(t, "EVOLUTION INPUT %-28q -> REJECT category=%s reason=%s", tc.name, ce.Category, ce.Reason)
	}
	if s.Snapshot().Generation() != g0+1 { // 仅 a,b 两次成功推进
		t.Fatalf("rejected changes must not advance generation: got %d", s.Snapshot().Generation())
	}
	logf(t, "EVOLUTION final generation=%d (rejected changes produced no new version)", s.Snapshot().Generation())
}

// TestEvolutionPersistsAcrossReopen 重命名/默认值语义在重开恢复后依然成立。
func TestEvolutionPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddColumn(ctx, "id", TypeInt64, ColumnAddOptions{Nullable: false, HasDefault: true, Default: iv(0)}); err != nil {
		t.Fatal(err)
	}
	sc := s.Snapshot().Schema()
	idCol, _ := sc.columnByName("id")
	tx := s.BeginWrite()
	if err := tx.Add(Row{idCol.ID: iv(42)}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddColumn(ctx, "region", TypeString, ColumnAddOptions{Nullable: false, HasDefault: true, Default: sv("cn")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RenameColumn(ctx, "id", "user_id"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	res, err := s2.Snapshot().Query(ctx, Query{})
	if err != nil {
		t.Fatalf("query after reopen: %v", Normalize(err))
	}
	logf(t, "EVOLUTION after reopen: columns=%v rows=%v", res.Columns, formatRows(res.Columns, res.Rows))
	if len(res.Columns) != 2 || res.Columns[0] != "user_id" || res.Columns[1] != "region" {
		t.Fatalf("schema after reopen wrong: %v", res.Columns)
	}
	if res.Rows[0][0].Int != 42 || res.Rows[0][1].Str != "cn" {
		t.Fatalf("historical semantics after reopen wrong: %v", res.Rows[0])
	}
}
