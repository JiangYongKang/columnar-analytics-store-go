package colstore

// newSchema 创建初始空结构（版本 0）。
func newSchema() *Schema {
	return &Schema{Version: 0, Columns: nil}
}

// cloneSchema 深拷贝结构，保证旧快照持有的 Schema 不被后续变更污染。
func cloneSchema(s *Schema) *Schema {
	if s == nil {
		return nil
	}
	cp := &Schema{Version: s.Version, Columns: make([]*ColumnDef, len(s.Columns))}
	for i, c := range s.Columns {
		cc := *c
		if c.PreviousNames != nil {
			cc.PreviousNames = append([]string(nil), c.PreviousNames...)
		}
		cp.Columns[i] = &cc
	}
	return cp
}

// nameTaken 报告某个名字（含历史名字）是否被任意字段（含已删除墓碑）占用。
// 历史名字也保留，是为了让“重命名后再用旧名建新列”被明确判定，
// 避免旧名字歧义地解析到两段不同语义的数据。
func (s *Schema) nameTaken(name string) (*ColumnDef, bool) {
	for _, c := range s.Columns {
		if c.Name == name {
			return c, true
		}
		for _, p := range c.PreviousNames {
			if p == name {
				return c, true
			}
		}
	}
	return nil, false
}

// columnByName 按当前名字解析字段，已删除字段不可见。
func (s *Schema) columnByName(name string) (*ColumnDef, bool) {
	for _, c := range s.Columns {
		if !c.Dropped && c.Name == name {
			return c, true
		}
	}
	return nil, false
}

func (s *Schema) columnByID(id FieldID) (*ColumnDef, bool) {
	for _, c := range s.Columns {
		if c.ID == id {
			return c, true
		}
	}
	return nil, false
}

// liveColumns 返回当前可见字段，顺序即创建顺序，保证结果列顺序可复现。
func (s *Schema) liveColumns() []*ColumnDef {
	out := make([]*ColumnDef, 0, len(s.Columns))
	for _, c := range s.Columns {
		if !c.Dropped {
			out = append(out, c)
		}
	}
	return out
}

// ColumnAddOptions 描述新增字段的选项。
type ColumnAddOptions struct {
	Nullable   bool
	HasDefault bool
	Default    Value
}

// addColumn 在当前结构上派生新版本。语义：
//   - 新字段对历史行取默认值；未给默认值时历史行一律为 NULL，
//     因而非空（Nullable=false）字段必须提供非 NULL 默认值，否则拒绝。
//   - 名字（含任何字段的历史名字）冲突时明确拒绝，绝不静默替换。
func (s *Schema) addColumn(name string, typ ColumnType, opts ColumnAddOptions, nextID FieldID) (*Schema, error) {
	if name == "" {
		return nil, newSchemaChangeError(SchemaDuplicateColumn, name, "column name must not be empty")
	}
	if !typ.valid() {
		return nil, newSchemaChangeError(SchemaTypeMismatch, name, "invalid column type")
	}
	if _, taken := s.nameTaken(name); taken {
		return nil, newSchemaChangeError(SchemaDuplicateColumn, name, "name (or a historical name) is already in use")
	}
	def := &ColumnDef{
		ID:         nextID,
		Name:       name,
		Type:       typ,
		Nullable:   opts.Nullable,
		HasDefault: opts.HasDefault,
	}
	if opts.HasDefault {
		dv := opts.Default
		if dv.Null {
			def.HasDefault = false
		} else {
			if dv.Type != typ {
				return nil, newSchemaChangeError(SchemaTypeMismatch, name,
					"default value type does not match column type")
			}
			def.Default = dv
		}
	}
	if !def.Nullable && (!def.HasDefault || def.Default.Null) {
		return nil, newSchemaChangeError(SchemaNotNullWithoutValue, name,
			"NOT NULL column must be created with a non-NULL default so historical rows stay valid")
	}
	ns := cloneSchema(s)
	ns.Version = s.Version + 1
	ns.Columns = append(ns.Columns, def)
	return ns, nil
}

// dropColumn 软删除字段：置墓碑并升版本。
// 历史片段中的列式数据仍保留在磁盘上，只是对查询不可见；
// FieldID 不复用、不重排，从而不会发生静默错列。
func (s *Schema) dropColumn(name string) (*Schema, error) {
	c, ok := s.columnByName(name)
	if !ok {
		return nil, newSchemaChangeError(SchemaColumnNotFound, name, "column does not exist or was already dropped")
	}
	ns := cloneSchema(s)
	ns.Version = s.Version + 1
	for _, cc := range ns.Columns {
		if cc.ID == c.ID {
			cc.Dropped = true
			break
		}
	}
	return ns, nil
}

// renameColumn 改当前名字，旧名字进入 PreviousNames。
// FieldID 保持不变，因此历史片段无需任何数据迁移即可继续解析。
func (s *Schema) renameColumn(oldName, newName string) (*Schema, error) {
	c, ok := s.columnByName(oldName)
	if !ok {
		return nil, newSchemaChangeError(SchemaColumnNotFound, oldName, "column does not exist or was dropped")
	}
	if newName == "" {
		return nil, newSchemaChangeError(SchemaRenameTargetExists, newName, "new name must not be empty")
	}
	if oldName == newName {
		// 幂等：改名成自己不算演进，直接返回同版本拷贝。
		ns := cloneSchema(s)
		return ns, nil
	}
	if other, taken := s.nameTaken(newName); taken && other.ID != c.ID {
		return nil, newSchemaChangeError(SchemaRenameTargetExists, newName,
			"target name already belongs to another column")
	}
	ns := cloneSchema(s)
	ns.Version = s.Version + 1
	for _, cc := range ns.Columns {
		if cc.ID == c.ID {
			cc.PreviousNames = append(cc.PreviousNames, cc.Name)
			cc.Name = newName
			break
		}
	}
	return ns, nil
}

// resolveValueForWrite 决定一行数据写入某字段时的确定取值：
// 行内显式提供 -> 校验类型；未提供 -> 非空默认值；再否则 NULL；
// 非空字段得到 NULL 时拒绝输入。
func (s *Schema) resolveValueForWrite(c *ColumnDef, v Value, present bool) (Value, error) {
	if present {
		if !v.matchesType(c.Type) {
			return Value{}, newSchemaChangeError(SchemaTypeMismatch, c.Name,
				"row value type does not match column type")
		}
		if v.Null && !c.Nullable {
			return Value{}, newSchemaChangeError(SchemaNotNullWithoutValue, c.Name, "NULL into NOT NULL column")
		}
		return v, nil
	}
	if c.HasDefault {
		return c.Default, nil
	}
	if c.Nullable {
		return nullValue(), nil
	}
	return Value{}, newSchemaChangeError(SchemaNotNullWithoutValue, c.Name, "missing value for NOT NULL column without default")
}
