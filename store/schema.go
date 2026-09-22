package store

import "fmt"

// FieldType 列类型。
type FieldType string

const (
	TypeInt64   FieldType = "int64"
	TypeFloat64 FieldType = "float64"
	TypeString  FieldType = "string"
	TypeBool    FieldType = "bool"
)

// Field 表结构中的一个字段。ID 在表生命周期内唯一且不复用，
// 用于在结构演进后稳定地定位历史数据列。
type Field struct {
	ID   int
	Name string
	Type FieldType
}

// Schema 某一版本的表结构。
type Schema struct {
	Version int
	Fields  []Field // 按 ID 升序
	// Dropped 记录已删除字段的 ID 与名称，保证 ID 不复用。
	Dropped []Field
}

// FieldByID 按 ID 查找字段。
func (s *Schema) FieldByID(id int) *Field {
	for i := range s.Fields {
		if s.Fields[i].ID == id {
			return &s.Fields[i]
		}
	}
	return nil
}

// FieldByName 按名称查找字段。
func (s *Schema) FieldByName(name string) *Field {
	for i := range s.Fields {
		if s.Fields[i].Name == name {
			return &s.Fields[i]
		}
	}
	return nil
}

// nextFieldID 分配一个不复用的字段 ID。
func (s *Schema) nextFieldID() int {
	max := 0
	for _, f := range s.Fields {
		if f.ID > max {
			max = f.ID
		}
	}
	for _, f := range s.Dropped {
		if f.ID > max {
			max = f.ID
		}
	}
	return max + 1
}

// clone 深拷贝结构。
func (s *Schema) clone() *Schema {
	out := &Schema{Version: s.Version}
	out.Fields = append(out.Fields, s.Fields...)
	out.Dropped = append(out.Dropped, s.Dropped...)
	return out
}

// SchemaChange 一次结构变更操作。
type SchemaChange struct {
	Kind    ChangeKind
	Field   Field  // Add 时使用（ID 忽略，由存储分配）
	OldName string // Rename/Drop 时定位字段
	NewName string // Rename 时的新名
}

// ChangeKind 变更类型。
type ChangeKind int

const (
	ChangeAddField ChangeKind = iota
	ChangeDropField
	ChangeRenameField
)

func schemaErr(op, reason string) error {
	return &Error{Code: ErrCodeSchemaIncompatible, Reason: reason, Op: op,
		Err: fmt.Errorf("schema change rejected")}
}

// Apply 校验并应用变更，返回新版本结构；不兼容时返回
// ErrCodeSchemaIncompatible 错误。
//
// 兼容规则：
//   - Add：新字段分配新 ID；历史片段中该列缺失，读取时按 NULL 处理。
//   - Drop：字段 ID 进入 Dropped 不再复用；历史数据保留但不再可见。
//   - Rename：仅改名，ID 与类型不变，历史数据按 ID 命中，语义不变。
//   - 类型不可变更；名称不得与现存字段冲突；不得删除/重命名不存在的字段。
func (s *Schema) Apply(c SchemaChange) (*Schema, error) {
	next := s.clone()
	next.Version = s.Version + 1

	switch c.Kind {
	case ChangeAddField:
		if c.Field.Name == "" {
			return nil, schemaErr("add-field", "EMPTY_NAME")
		}
		if s.FieldByName(c.Field.Name) != nil {
			return nil, schemaErr("add-field", "NAME_EXISTS:"+c.Field.Name)
		}
		switch c.Field.Type {
		case TypeInt64, TypeFloat64, TypeString, TypeBool:
		default:
			return nil, schemaErr("add-field", "UNKNOWN_TYPE:"+string(c.Field.Type))
		}
		f := c.Field
		f.ID = s.nextFieldID()
		next.Fields = append(next.Fields, f)
		sortFields(next.Fields)
		return next, nil

	case ChangeDropField:
		f := s.FieldByName(c.OldName)
		if f == nil {
			return nil, schemaErr("drop-field", "NO_SUCH_FIELD:"+c.OldName)
		}
		kept := next.Fields[:0]
		for _, x := range next.Fields {
			if x.ID != f.ID {
				kept = append(kept, x)
			}
		}
		next.Fields = kept
		next.Dropped = append(next.Dropped, *f)
		return next, nil

	case ChangeRenameField:
		f := s.FieldByName(c.OldName)
		if f == nil {
			return nil, schemaErr("rename-field", "NO_SUCH_FIELD:"+c.OldName)
		}
		if c.NewName == "" {
			return nil, schemaErr("rename-field", "EMPTY_NAME")
		}
		if existing := s.FieldByName(c.NewName); existing != nil && existing.ID != f.ID {
			return nil, schemaErr("rename-field", "NAME_EXISTS:"+c.NewName)
		}
		for i := range next.Fields {
			if next.Fields[i].ID == f.ID {
				next.Fields[i].Name = c.NewName
			}
		}
		return next, nil
	}
	return nil, schemaErr("apply", "UNKNOWN_CHANGE_KIND")
}

func sortFields(fs []Field) {
	for i := 1; i < len(fs); i++ {
		for j := i; j > 0 && fs[j].ID < fs[j-1].ID; j-- {
			fs[j], fs[j-1] = fs[j-1], fs[j]
		}
	}
}
