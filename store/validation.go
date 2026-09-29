package store

import "fmt"

// validateFragmentForSchema 校验片段可被结构完整解释：
//   - 列 ID 必须存在于当前结构或已删除集合（不允许未知 ID）；
//   - 可见列取值的 Go 类型必须与字段类型一致（NULL 总是允许）；
//   - 每列长度必须等于 NumRows。
//
// 任何不满足都在写入时以 SCHEMA_INCOMPATIBLE 明确失败，
// 杜绝“写入成功、查询时被静默漏掉或算错”。
func validateFragmentForSchema(f *Fragment, schema *Schema) error {
	const op = "write-fragment"
	for id, col := range f.Columns {
		fld := schema.FieldByID(id)
		if fld == nil {
			if !droppedHas(schema, id) {
				return &Error{Code: ErrCodeSchemaIncompatible,
					Reason: fmt.Sprintf("UNKNOWN_FIELD_ID:%d", id), Op: op,
					Err: fmt.Errorf("fragment column not in schema")}
			}
			if len(col) != f.NumRows {
				return &Error{Code: ErrCodeSchemaIncompatible,
					Reason: fmt.Sprintf("COLUMN_LEN_%d", id), Op: op,
					Err: fmt.Errorf("dropped column %d length %d != numRows %d",
						id, len(col), f.NumRows)}
			}
			// 已删除列：原样落盘保留历史数据，类型由写入者负责，不参与统计。
			continue
		}
		if len(col) != f.NumRows {
			return &Error{Code: ErrCodeSchemaIncompatible,
				Reason: fmt.Sprintf("COLUMN_LEN_%d", id), Op: op,
				Err: fmt.Errorf("column %q length %d != numRows %d",
					fld.Name, len(col), f.NumRows)}
		}
		for r, v := range col {
			if err := checkValueType(fld, v); err != nil {
				return &Error{Code: ErrCodeSchemaIncompatible,
					Reason: fmt.Sprintf("BAD_VALUE_TYPE_FIELD_%s_ROW_%d", fld.Name, r),
					Op:     op, Err: err}
			}
		}
	}
	return nil
}

func checkValueType(fld *Field, v Value) error {
	if v == nil {
		return nil
	}
	var want string
	var ok bool
	switch fld.Type {
	case TypeInt64:
		_, ok = v.(int64)
		want = "int64"
	case TypeFloat64:
		_, ok = v.(float64)
		want = "float64"
	case TypeString:
		_, ok = v.(string)
		want = "string"
	case TypeBool:
		_, ok = v.(bool)
		want = "bool"
	default:
		return fmt.Errorf("field %q has unknown type %q", fld.Name, fld.Type)
	}
	if !ok {
		return fmt.Errorf("field %q expects %s, got %T", fld.Name, want, v)
	}
	return nil
}
