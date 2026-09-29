package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

// Value 一个单元格取值；nil 表示 NULL（历史数据中不存在该列时）。
type Value = any

// Row 一行数据：字段 ID -> 值。
type Row map[int]Value

// Fragment 一个不可变数据片段：按列存储一批行。
//
// Zones/ColTypes 由编码路径填充（写入或合并时生成）；解码路径也会
// 回填，供查询裁剪使用，普通调用方无需设置。
type Fragment struct {
	Key      string // 片段键（决定冲突检测的重叠判定）
	SchemaV  int    // 写入时的结构版本
	NumRows  int
	Columns  map[int][]Value
	Zones    ZoneMap           // 每列范围（CFR2 有；CFR1 解码后为 nil）
	ColTypes map[int]FieldType // 每列权威类型（编码时使用）
}

// FragmentMeta 提交日志中记录的片段元信息。
type FragmentMeta struct {
	Key     string
	SchemaV int
	NumRows int
	File    string // 磁盘文件名
	Size    int64  // 文件字节数（用于体积阈值与预算）
}

var fragmentMagic = []byte("CFR1")
var fragmentMagicV2 = []byte("CFR2")

// encodeFragment 将片段写入 w。
//
// CFR2 格式：
//
//	magic("CFR2") | uvarint schemaV | uvarint numRows | uvarint numCols
//	| [uvarint fieldID | type | crc32(4) | uvarint plen | payload]*
//	| zone section
//	| u64be zoneStart（距文件开头的偏移；zone section 为空时为 0）
//
// 每列 payload 独立 CRC32 校验，损坏可定位到列。zone section 内容见
// writeZoneMap；查询裁剪只读文件尾部，不必解码整段数据。
func encodeFragment(w io.Writer, f *Fragment) error {
	ids := make([]int, 0, len(f.Columns))
	for id := range f.Columns {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	hdr := &bytes.Buffer{}
	hdr.Write(fragmentMagicV2)
	writeUvarint(hdr, uint64(f.SchemaV))
	writeUvarint(hdr, uint64(f.NumRows))
	writeUvarint(hdr, uint64(len(ids)))

	body := &bytes.Buffer{}
	for _, id := range ids {
		col := f.Columns[id]
		if len(col) != f.NumRows {
			return fmt.Errorf("column %d length %d != numRows %d", id, len(col), f.NumRows)
		}
		payload := &bytes.Buffer{}
		colType := string(TypeFieldNull)
		if t, ok := f.ColTypes[id]; ok {
			colType = string(t)
		} else {
			// 未提供权威类型（兼容路径）：按首个非 NULL 值推断。
			for _, v := range col {
				if v != nil {
					colType = string(inferType(v))
					break
				}
			}
		}
		if err := encodeColumn(payload, col, FieldType(colType)); err != nil {
			return err
		}
		writeUvarint(body, uint64(id))
		writeUvarint(body, uint64(len(colType)))
		body.WriteString(colType)
		crc := crc32.ChecksumIEEE(payload.Bytes())
		var crcBuf [4]byte
		binary.BigEndian.PutUint32(crcBuf[:], crc)
		body.Write(crcBuf[:])
		writeUvarint(body, uint64(payload.Len()))
		body.Write(payload.Bytes())
	}

	if _, err := w.Write(hdr.Bytes()); err != nil {
		return err
	}
	if _, err := w.Write(body.Bytes()); err != nil {
		return err
	}

	zones := f.Zones
	if zones == nil {
		zones = buildZones(f, f.ColTypes)
	}
	zoneStart := uint64(hdr.Len() + body.Len())
	zoneBytes, err := encodeZoneSection(zones)
	if err != nil {
		return err
	}
	if _, err := w.Write(zoneBytes); err != nil {
		return err
	}
	var zoneStartBuf [8]byte
	putU64(zoneStartBuf[:], zoneStart)
	_, err = w.Write(zoneStartBuf[:])
	return err
}

// encodeZoneSection 序列化带 CRC 的 zone section：
// "ZONE" | zone payload | crc32(payload)(4)。
func encodeZoneSection(z ZoneMap) ([]byte, error) {
	payload := &bytes.Buffer{}
	if err := writeZoneMap(payload, z); err != nil {
		return nil, err
	}
	out := &bytes.Buffer{}
	out.Write([]byte("ZONE"))
	out.Write(payload.Bytes())
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc32.ChecksumIEEE(payload.Bytes()))
	out.Write(crcBuf[:])
	return out.Bytes(), nil
}

// parseZoneSection 校验并解析 zone section（sec 不含尾部 u64 偏移）。
func parseZoneSection(sec []byte) (ZoneMap, error) {
	if len(sec) < 8 || string(sec[:4]) != "ZONE" {
		return nil, corruptErr("BAD_ZONE_MARKER")
	}
	payload := sec[4 : len(sec)-4]
	want := binary.BigEndian.Uint32(sec[len(sec)-4:])
	if crc32.ChecksumIEEE(payload) != want {
		return nil, corruptErr("CRC_MISMATCH_ZONE")
	}
	return readZoneMap(bytes.NewReader(payload))
}

// decodeFragment 从 r 读取片段并校验完整性；
// 校验失败返回 ErrCodeCorruptFragment。兼容 CFR1（无 zone）与 CFR2。
func decodeFragment(r io.Reader) (*Fragment, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, &Error{Code: ErrCodeInterrupted, Op: "decode-fragment", Err: err}
	}
	f, colEnd, v2, err := decodeFragmentHeader(data)
	if err != nil {
		return nil, err
	}
	if v2 {
		if err := decodeFragmentZones(f, data, colEnd); err != nil {
			return nil, err
		}
	}
	return f, nil
}

// decodeFragmentHeader 解析 magic/头部/全部列，返回列区结束偏移。
func decodeFragmentHeader(data []byte) (f *Fragment, colEnd int, v2 bool, err error) {
	buf := bytes.NewReader(data)
	magic := make([]byte, 4)
	if _, err := io.ReadFull(buf, magic); err != nil {
		return nil, 0, false, corruptErr("BAD_MAGIC")
	}
	switch string(magic) {
	case string(fragmentMagic):
		v2 = false
	case string(fragmentMagicV2):
		v2 = true
	default:
		return nil, 0, false, corruptErr("BAD_MAGIC")
	}
	f = &Fragment{Columns: map[int][]Value{}, ColTypes: map[int]FieldType{}}
	f.SchemaV = int(readUvarint(buf))
	f.NumRows = int(readUvarint(buf))
	numCols := int(readUvarint(buf))

	for c := 0; c < numCols; c++ {
		id := int(readUvarint(buf))
		tlen := int(readUvarint(buf))
		tbuf := make([]byte, tlen)
		if _, err := io.ReadFull(buf, tbuf); err != nil {
			return nil, 0, v2, corruptErr("TRUNCATED_COLUMN_HEADER")
		}
		var crcBuf [4]byte
		if _, err := io.ReadFull(buf, crcBuf[:]); err != nil {
			return nil, 0, v2, corruptErr("TRUNCATED_CRC")
		}
		plen := int(readUvarint(buf))
		payload := make([]byte, plen)
		if _, err := io.ReadFull(buf, payload); err != nil {
			return nil, 0, v2, corruptErr("TRUNCATED_PAYLOAD")
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(crcBuf[:]) {
			return nil, 0, v2, corruptErr(fmt.Sprintf("CRC_MISMATCH_COLUMN_%d", id))
		}
		col, err := decodeColumn(payload, FieldType(tbuf), f.NumRows)
		if err != nil {
			return nil, 0, v2, err
		}
		f.Columns[id] = col
		f.ColTypes[id] = FieldType(tbuf)
	}
	pos, _ := buf.Seek(0, io.SeekCurrent)
	return f, int(pos), v2, nil
}

// decodeFragmentZones 解析 CFR2 尾部 zone section。
func decodeFragmentZones(f *Fragment, data []byte, colEnd int) error {
	if len(data) < colEnd+16 { // "ZONE"(4) + crc(4) + zoneStart u64(8)
		return corruptErr("TRUNCATED_ZONE_FOOTER")
	}
	zoneStart := int(getU64(data[len(data)-8:]))
	if zoneStart < colEnd || zoneStart+16 > len(data) {
		return corruptErr("BAD_ZONE_OFFSET")
	}
	z, err := parseZoneSection(data[zoneStart : len(data)-8])
	if err != nil {
		return err
	}
	f.Zones = z
	return nil
}

// fragmentHeaderInfo 只解析片段头部，返回列 ID、每列类型与列区结束偏移，
// 不读取列 payload 与 zone（供合并时先取列集合并做类型校验）。
func fragmentHeaderInfo(data []byte) (colTypes map[int]FieldType, colEnd int, v2 bool, err error) {
	buf := bytes.NewReader(data)
	magic := make([]byte, 4)
	if _, err := io.ReadFull(buf, magic); err != nil {
		return nil, 0, false, corruptErr("BAD_MAGIC")
	}
	switch string(magic) {
	case string(fragmentMagic):
		v2 = false
	case string(fragmentMagicV2):
		v2 = true
	default:
		return nil, 0, false, corruptErr("BAD_MAGIC")
	}
	_ = readUvarint(buf) // schemaV
	numRows := readUvarint(buf)
	numCols := readUvarint(buf)
	_ = numRows
	colTypes = map[int]FieldType{}
	for c := uint64(0); c < numCols; c++ {
		id := readUvarint(buf)
		tlen := readUvarint(buf)
		tbuf := make([]byte, tlen)
		if _, err := io.ReadFull(buf, tbuf); err != nil {
			return nil, 0, v2, corruptErr("TRUNCATED_COLUMN_HEADER")
		}
		if _, err := buf.Seek(4, io.SeekCurrent); err != nil { // crc
			return nil, 0, v2, corruptErr("TRUNCATED_CRC")
		}
		plen := readUvarint(buf)
		if _, err := buf.Seek(int64(plen), io.SeekCurrent); err != nil {
			return nil, 0, v2, corruptErr("TRUNCATED_PAYLOAD")
		}
		colTypes[int(id)] = FieldType(tbuf)
	}
	pos, _ := buf.Seek(0, io.SeekCurrent)
	return colTypes, int(pos), v2, nil
}

// loadZoneMapFile 只读取片段尾部 zone。
// CFR1（旧格式，无 zone）返回 (nil,nil)，查询回退为完整扫描；
// CFR2 的 zone section 必须存在且 CRC 校验通过，否则按损坏处理。
func loadZoneMapFile(path string) (ZoneMap, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, &Error{Code: ErrCodeInterrupted, Op: "read-zone", Err: err}
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, &Error{Code: ErrCodeInterrupted, Op: "read-zone", Err: err}
	}
	size := st.Size()
	magic := make([]byte, 4)
	if _, err := f.ReadAt(magic, 0); err != nil {
		return nil, &Error{Code: ErrCodeInterrupted, Op: "read-zone", Err: err}
	}
	if string(magic) == string(fragmentMagic) {
		return nil, nil // CFR1：无 zone，完整扫描
	}
	if string(magic) != string(fragmentMagicV2) {
		return nil, corruptErr("BAD_MAGIC")
	}
	if size < 16 {
		return nil, corruptErr("TRUNCATED_ZONE_FOOTER")
	}
	footer := make([]byte, 8)
	if _, err := f.ReadAt(footer, size-8); err != nil {
		return nil, &Error{Code: ErrCodeInterrupted, Op: "read-zone", Err: err}
	}
	zoneStart := int64(getU64(footer))
	if zoneStart+16 > size {
		return nil, corruptErr("BAD_ZONE_OFFSET")
	}
	sec := make([]byte, size-8-zoneStart)
	if _, err := f.ReadAt(sec, zoneStart); err != nil {
		return nil, &Error{Code: ErrCodeInterrupted, Op: "read-zone", Err: err}
	}
	return parseZoneSection(sec)
}

func corruptErr(reason string) error {
	return &Error{Code: ErrCodeCorruptFragment, Reason: reason, Op: "decode-fragment",
		Err: fmt.Errorf("fragment integrity check failed")}
}

func encodeColumn(w *bytes.Buffer, col []Value, typ FieldType) error {
	for _, v := range col {
		if v == nil {
			w.WriteByte(0)
			continue
		}
		if !valueHasType(v, typ) {
			return &Error{Code: ErrCodeSchemaIncompatible, Op: "encode-column",
				Reason: fmt.Sprintf("COLUMN_TYPE_MISMATCH:%s", typ),
				Err:    fmt.Errorf("value %v (%T) not encodable as %s", v, v, typ)}
		}
		w.WriteByte(1)
		switch x := v.(type) {
		case int64:
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], uint64(x))
			w.Write(b[:])
		case float64:
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], mathFloat64bits(x))
			w.Write(b[:])
		case string:
			writeUvarint(w, uint64(len(x)))
			w.WriteString(x)
		case bool:
			if x {
				w.WriteByte(1)
			} else {
				w.WriteByte(0)
			}
		default:
			return &Error{Code: ErrCodeSchemaIncompatible, Op: "encode-column",
				Reason: "UNSUPPORTED_VALUE_TYPE",
				Err:    fmt.Errorf("value type %T not supported", v)}
		}
	}
	return nil
}

// valueHasType 值是否可按列类型编码。
func valueHasType(v Value, typ FieldType) bool {
	switch v.(type) {
	case int64:
		return typ == TypeInt64
	case float64:
		return typ == TypeFloat64
	case string:
		return typ == TypeString
	case bool:
		return typ == TypeBool
	}
	return false
}

func decodeColumn(payload []byte, typ FieldType, numRows int) ([]Value, error) {
	r := bytes.NewReader(payload)
	col := make([]Value, numRows)
	for i := 0; i < numRows; i++ {
		present, err := r.ReadByte()
		if err != nil {
			return nil, corruptErr("TRUNCATED_VALUES")
		}
		if present == 0 {
			col[i] = nil
			continue
		}
		switch typ {
		case TypeInt64:
			var b [8]byte
			if _, err := io.ReadFull(r, b[:]); err != nil {
				return nil, corruptErr("TRUNCATED_VALUES")
			}
			col[i] = int64(binary.BigEndian.Uint64(b[:]))
		case TypeFloat64:
			var b [8]byte
			if _, err := io.ReadFull(r, b[:]); err != nil {
				return nil, corruptErr("TRUNCATED_VALUES")
			}
			col[i] = mathFloat64frombits(binary.BigEndian.Uint64(b[:]))
		case TypeString:
			n := readUvarint(r)
			sb := make([]byte, n)
			if _, err := io.ReadFull(r, sb); err != nil {
				return nil, corruptErr("TRUNCATED_VALUES")
			}
			col[i] = string(sb)
		case TypeBool:
			b, err := r.ReadByte()
			if err != nil {
				return nil, corruptErr("TRUNCATED_VALUES")
			}
			col[i] = b == 1
		default:
			// "null" 列不应出现 present=1
			return nil, corruptErr("UNEXPECTED_VALUE_IN_NULL_COLUMN")
		}
	}
	return col, nil
}

func writeUvarint(w *bytes.Buffer, v uint64) {
	var b [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(b[:], v)
	w.Write(b[:n])
}

func readUvarint(r io.ByteReader) uint64 {
	v, err := binary.ReadUvarint(r)
	if err != nil {
		return 0
	}
	return v
}
