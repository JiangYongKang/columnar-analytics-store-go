package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// ColumnZone 一列在某片段内的取值范围（zone map），用于查询裁剪。
//
// 语义：
//   - Type 为该列非 NULL 值的类型（结构校验保证同列同类型）；
//     全 NULL 时 Type 为 "null"。
//   - Min/Max 覆盖全部非 NULL 值；NULL 不参与范围，但计入 Nulls。
type ColumnZone struct {
	FieldID int
	Type    FieldType
	Min     Value
	Max     Value
	Nulls   int
}

// ZoneMap 片段级 zone map：字段 ID -> 该列范围。
type ZoneMap map[int]*ColumnZone

// QueryStats 查询执行统计（用于验证裁剪与预算行为）。
type QueryStats struct {
	FragmentsTotal   int
	FragmentsSkipped int
	FragmentsScanned int
	RowsMatched      int64
}

// zone 序列化的值类型标签。
const (
	zoneTagNull    = 0
	zoneTagInt64   = 1
	zoneTagFloat64 = 2
	zoneTagString  = 3
	zoneTagBool    = 4
)

// buildZones 扫描片段列数据构建 zone map。
// types 给出每列的权威类型（写入/合并时由结构校验得出）；为 nil 时按
// 首个非 NULL 值推断（仅兼容用途）。
func buildZones(f *Fragment, types map[int]FieldType) ZoneMap {
	z := ZoneMap{}
	for id, col := range f.Columns {
		cz := &ColumnZone{FieldID: id, Type: TypeFieldNull}
		if t, ok := types[id]; ok {
			cz.Type = t
		}
		for _, v := range col {
			if v == nil {
				cz.Nulls++
				continue
			}
			if cz.Type == TypeFieldNull {
				cz.Type = inferType(v)
			}
			if cz.Min == nil || valueLess(v, cz.Min) {
				cz.Min = v
			}
			if cz.Max == nil || valueLess(cz.Max, v) {
				cz.Max = v
			}
		}
		if cz.Type == TypeFieldNull {
			cz.Min, cz.Max = nil, nil
		}
		z[id] = cz
	}
	return z
}

// TypeFieldNull 表示整列均为 NULL 的 zone 类型标记。
const TypeFieldNull FieldType = "null"

func inferType(v Value) FieldType {
	switch v.(type) {
	case int64:
		return TypeInt64
	case float64:
		return TypeFloat64
	case string:
		return TypeString
	case bool:
		return TypeBool
	}
	return TypeFieldNull
}

// valueLess 有序比较；数值 int64/float64 互比，bool 视 false<true。
// 与查询的 compareValues 语义一致（类型不兼容返回 false）。
func valueLess(a, b Value) bool {
	switch x := a.(type) {
	case int64:
		switch y := b.(type) {
		case int64:
			return x < y
		case float64:
			return float64(x) < y
		}
	case float64:
		switch y := b.(type) {
		case int64:
			return x < float64(y)
		case float64:
			return x < y
		}
	case string:
		if y, ok := b.(string); ok {
			return x < y
		}
	case bool:
		if y, ok := b.(bool); ok {
			return !x && y
		}
	}
	return false
}

// pruneFragment 判断片段是否可被谓词整体跳过。
//
// 保证：只有“没有任何一行可能满足全部谓词”时才返回 true，
// NULL 行不匹配任何谓词；缺列视同全 NULL。结构演进导致的缺列、
// 全 NULL 列、改名（按 ID 命中）都由此正确处理。
func pruneFragment(z ZoneMap, preds []boundPred) bool {
	for _, p := range preds {
		cz, ok := z[p.fieldID]
		if !ok {
			// 片段根本没有该列：全部行为 NULL，不匹配。
			return true
		}
		if predicateUnsatisfiable(cz, p) {
			return true
		}
	}
	return false
}

// predicateUnsatisfiable 判断单个谓词对该列是否不可能命中。
func predicateUnsatisfiable(cz *ColumnZone, p boundPred) bool {
	if cz.Type == TypeFieldNull {
		// 整列 NULL：任何比较都不命中。
		return true
	}
	switch p.op {
	case OpEq:
		// 边界值落在 [Min, Max] 之外则不可能相等。
		if valueLess(p.val, cz.Min) || valueLess(cz.Max, p.val) {
			return true
		}
		// 类型不兼容（如字符串列与整数比较）时比较恒为 false。
		if !comparable(cz.Type, p.val) {
			return true
		}
		return false
	case OpNe:
		// 所有非 NULL 值都等于边界值时，没有“不等”的行；
		// NULL 行同样不匹配谓词。
		if !comparable(cz.Type, p.val) {
			return true
		}
		return !valueLess(cz.Min, p.val) && !valueLess(p.val, cz.Max)
	case OpLt:
		// 需要存在 v < bound：Min < bound 才可能。
		return !comparable(cz.Type, p.val) || !valueLess(cz.Min, p.val)
	case OpLe:
		// 需要存在 v <= bound：Min <= bound。
		return !comparable(cz.Type, p.val) || valueLess(p.val, cz.Min)
	case OpGt:
		// 需要存在 v > bound：Max > bound。
		return !comparable(cz.Type, p.val) || !valueLess(p.val, cz.Max)
	case OpGe:
		// 需要存在 v >= bound：Max >= bound。
		return !comparable(cz.Type, p.val) || valueLess(cz.Max, p.val)
	}
	return false
}

// comparable 判断边界值能否与该 zone 类型按查询语义比较，
// 与 compareValues 的类型规则一致（int64/float64 互通）。
func comparable(t FieldType, v Value) bool {
	switch t {
	case TypeInt64, TypeFloat64:
		switch v.(type) {
		case int64, float64:
			return true
		}
		return false
	case TypeString:
		_, ok := v.(string)
		return ok
	case TypeBool:
		_, ok := v.(bool)
		return ok
	}
	return false
}

// --- zone map 的二进制编解码（附在 CFR2 片段文件尾部）---

// 格式：uvarint 列数 | 每列: uvarint fieldID | byte tag | uvarint nulls |
//
//	[min value][max value]（null 列无 min/max）
func writeZoneMap(w io.Writer, z ZoneMap) error {
	ids := make([]int, 0, len(z))
	for id := range z {
		ids = append(ids, id)
	}
	sortInts(ids)
	buf := &bytes.Buffer{}
	writeUvarint(buf, uint64(len(ids)))
	for _, id := range ids {
		cz := z[id]
		writeUvarint(buf, uint64(id))
		buf.WriteByte(zoneTypeTag(cz.Type))
		writeUvarint(buf, uint64(cz.Nulls))
		if cz.Type != TypeFieldNull {
			if err := writeZoneValue(buf, cz.Min); err != nil {
				return err
			}
			if err := writeZoneValue(buf, cz.Max); err != nil {
				return err
			}
		}
	}
	_, err := w.Write(buf.Bytes())
	return err
}

func readZoneMap(r io.Reader) (ZoneMap, error) {
	br := byteReader{r: r}
	n, err := binary.ReadUvarint(&br)
	if err != nil {
		return nil, corruptErr("TRUNCATED_ZONE_HEADER")
	}
	z := ZoneMap{}
	for i := 0; i < int(n); i++ {
		id, err := binary.ReadUvarint(&br)
		if err != nil {
			return nil, corruptErr("TRUNCATED_ZONE_HEADER")
		}
		tag, err := br.ReadByte()
		if err != nil {
			return nil, corruptErr("TRUNCATED_ZONE_HEADER")
		}
		nulls, err := binary.ReadUvarint(&br)
		if err != nil {
			return nil, corruptErr("TRUNCATED_ZONE_HEADER")
		}
		cz := &ColumnZone{FieldID: int(id), Nulls: int(nulls), Type: tagZoneType(tag)}
		if cz.Type != TypeFieldNull {
			if cz.Min, err = readZoneValue(&br, cz.Type); err != nil {
				return nil, err
			}
			if cz.Max, err = readZoneValue(&br, cz.Type); err != nil {
				return nil, err
			}
		}
		z[int(id)] = cz
	}
	return z, nil
}

func zoneTypeTag(t FieldType) byte {
	switch t {
	case TypeInt64:
		return zoneTagInt64
	case TypeFloat64:
		return zoneTagFloat64
	case TypeString:
		return zoneTagString
	case TypeBool:
		return zoneTagBool
	}
	return zoneTagNull
}

func tagZoneType(tag byte) FieldType {
	switch tag {
	case zoneTagInt64:
		return TypeInt64
	case zoneTagFloat64:
		return TypeFloat64
	case zoneTagString:
		return TypeString
	case zoneTagBool:
		return TypeBool
	}
	return TypeFieldNull
}

func writeZoneValue(w *bytes.Buffer, v Value) error {
	switch x := v.(type) {
	case nil:
		w.WriteByte(zoneTagNull)
	case int64:
		w.WriteByte(zoneTagInt64)
		var b [8]byte
		putU64(b[:], uint64(x))
		w.Write(b[:])
	case float64:
		w.WriteByte(zoneTagFloat64)
		var b [8]byte
		putU64(b[:], mathFloat64bits(x))
		w.Write(b[:])
	case string:
		w.WriteByte(zoneTagString)
		writeUvarint(w, uint64(len(x)))
		w.WriteString(x)
	case bool:
		w.WriteByte(zoneTagBool)
		if x {
			w.WriteByte(1)
		} else {
			w.WriteByte(0)
		}
	default:
		return fmt.Errorf("zone: unsupported value type %T", v)
	}
	return nil
}

func readZoneValue(r *byteReader, typ FieldType) (Value, error) {
	tag, err := r.ReadByte()
	if err != nil {
		return nil, corruptErr("TRUNCATED_ZONE_VALUE")
	}
	if tag == zoneTagNull {
		return nil, nil
	}
	read8 := func() ([]byte, error) {
		b := make([]byte, 8)
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, corruptErr("TRUNCATED_ZONE_VALUE")
		}
		return b, nil
	}
	switch typ {
	case TypeInt64:
		if tag != zoneTagInt64 {
			return nil, corruptErr("ZONE_TYPE_TAG_MISMATCH")
		}
		b, err := read8()
		if err != nil {
			return nil, err
		}
		return int64(getU64(b)), nil
	case TypeFloat64:
		if tag != zoneTagFloat64 {
			return nil, corruptErr("ZONE_TYPE_TAG_MISMATCH")
		}
		b, err := read8()
		if err != nil {
			return nil, err
		}
		return mathFloat64frombits(getU64(b)), nil
	case TypeString:
		if tag != zoneTagString {
			return nil, corruptErr("ZONE_TYPE_TAG_MISMATCH")
		}
		n, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, corruptErr("TRUNCATED_ZONE_VALUE")
		}
		sb := make([]byte, n)
		if _, err := io.ReadFull(r, sb); err != nil {
			return nil, corruptErr("TRUNCATED_ZONE_VALUE")
		}
		return string(sb), nil
	case TypeBool:
		if tag != zoneTagBool {
			return nil, corruptErr("ZONE_TYPE_TAG_MISMATCH")
		}
		b, err := r.ReadByte()
		if err != nil {
			return nil, corruptErr("TRUNCATED_ZONE_VALUE")
		}
		return b == 1, nil
	}
	return nil, corruptErr("ZONE_UNKNOWN_TYPE")
}

// byteReader 让 io.Reader 同时满足 io.ByteReader（ReadUvarint 需要）。
type byteReader struct {
	r io.Reader
}

func (b *byteReader) ReadByte() (byte, error) {
	var buf [1]byte
	_, err := io.ReadFull(b.r, buf[:])
	return buf[0], err
}

func (b *byteReader) Read(p []byte) (int, error) { return b.r.Read(p) }

func putU64(b []byte, v uint64) { binary.BigEndian.PutUint64(b, v) }
func getU64(b []byte) uint64    { return binary.BigEndian.Uint64(b) }

func sortInts(xs []int) {
	for i := 1; i < len(xs); i++ {
		for j := i; j > 0 && xs[j] < xs[j-1]; j-- {
			xs[j], xs[j-1] = xs[j-1], xs[j]
		}
	}
}
