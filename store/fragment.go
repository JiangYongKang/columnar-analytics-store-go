package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
)

// Value 一个单元格取值；nil 表示 NULL（历史数据中不存在该列时）。
type Value = any

// Row 一行数据：字段 ID -> 值。
type Row map[int]Value

// Fragment 一个不可变数据片段：按列存储一批行。
type Fragment struct {
	Key     string // 片段键（决定冲突检测的重叠判定）
	SchemaV int    // 写入时的结构版本
	NumRows int
	// Columns 按字段 ID 存储列数据，长度均为 NumRows。
	Columns map[int][]Value
}

// FragmentMeta 提交日志中记录的片段元信息。
type FragmentMeta struct {
	Key     string
	SchemaV int
	NumRows int
	File    string // 磁盘文件名
	// Size 片段文件字节数，用于自动整理体积阈值与查询内存预算。
	Size int64 `json:"size"`
	// Stats 按字段 ID 记录的区段统计（仅当前结构可见列；
	// 片段缺列不会出现，结构删除列不记录）。
	Stats map[int]ColumnStat `json:"stats,omitempty"`
}

var fragmentMagic = []byte("CFR1")

// encodeFragment 将片段写入 w。
// 格式：magic | schemaV | numRows | numCols | [fieldID | type | crc32 | payload]*
// 每列 payload 独立 CRC32 校验，损坏可定位到列。
func encodeFragment(w io.Writer, f *Fragment) error {
	ids := make([]int, 0, len(f.Columns))
	for id := range f.Columns {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	hdr := &bytes.Buffer{}
	hdr.Write(fragmentMagic)
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
		colType := encodeColumn(payload, col)
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
	_, err := w.Write(body.Bytes())
	return err
}

// decodeFragment 从 r 读取片段并校验完整性；
// 校验失败返回 ErrCodeCorruptFragment。
func decodeFragment(r io.Reader) (*Fragment, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, &Error{Code: ErrCodeInterrupted, Op: "decode-fragment", Err: err}
	}
	buf := bytes.NewReader(data)

	magic := make([]byte, 4)
	if _, err := io.ReadFull(buf, magic); err != nil || string(magic) != string(fragmentMagic) {
		return nil, corruptErr("BAD_MAGIC")
	}
	f := &Fragment{Columns: map[int][]Value{}}
	f.SchemaV = int(readUvarint(buf))
	f.NumRows = int(readUvarint(buf))
	numCols := int(readUvarint(buf))

	for c := 0; c < numCols; c++ {
		id := int(readUvarint(buf))
		tlen := int(readUvarint(buf))
		tbuf := make([]byte, tlen)
		if _, err := io.ReadFull(buf, tbuf); err != nil {
			return nil, corruptErr("TRUNCATED_COLUMN_HEADER")
		}
		var crcBuf [4]byte
		if _, err := io.ReadFull(buf, crcBuf[:]); err != nil {
			return nil, corruptErr("TRUNCATED_CRC")
		}
		plen := int(readUvarint(buf))
		payload := make([]byte, plen)
		if _, err := io.ReadFull(buf, payload); err != nil {
			return nil, corruptErr("TRUNCATED_PAYLOAD")
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(crcBuf[:]) {
			return nil, corruptErr(fmt.Sprintf("CRC_MISMATCH_COLUMN_%d", id))
		}
		col, err := decodeColumn(payload, FieldType(tbuf), f.NumRows)
		if err != nil {
			return nil, err
		}
		f.Columns[id] = col
	}
	return f, nil
}

func corruptErr(reason string) error {
	return &Error{Code: ErrCodeCorruptFragment, Reason: reason, Op: "decode-fragment",
		Err: fmt.Errorf("fragment integrity check failed")}
}

func encodeColumn(w *bytes.Buffer, col []Value) string {
	// 以首个非 NULL 值推断列类型；全 NULL 记为 "null"。
	typ := "null"
	for _, v := range col {
		if v != nil {
			switch v.(type) {
			case int64:
				typ = string(TypeInt64)
			case float64:
				typ = string(TypeFloat64)
			case string:
				typ = string(TypeString)
			case bool:
				typ = string(TypeBool)
			}
			break
		}
	}
	for _, v := range col {
		if v == nil {
			w.WriteByte(0)
			continue
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
		}
	}
	return typ
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

func readUvarint(r *bytes.Reader) uint64 {
	v, err := binary.ReadUvarint(r)
	if err != nil {
		return 0
	}
	return v
}
