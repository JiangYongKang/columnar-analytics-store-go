package colstore

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

const (
	manifestPrefix = "manifest-"
	manifestSuffix = ".bin"
	segmentPrefix  = "segment-"
	segmentSuffix  = ".bin"
	manifestMagic  = "CASMAN01"
	manifestHeader = 8 + 1 + 4         // magic + version + generation(uint32)
	segEntrySize   = 8 + 4 + 4         // segmentID(uint64) + schemaVersion(uint32) + rowCount(uint32)
	colEntrySize   = 8 + 1 + 1 + 1 + 1 // fieldID(uint64) + type + nullable + hasDefault + dropped
)

// manifest 是一代已提交状态的完整持久化描述，写文件保证原子。
type manifest struct {
	Generation    int
	SchemaVersion int
	NextFieldID   FieldID
	NextSegmentID int64
	Columns       []*ColumnDef
	Segments      []segmentInfo
}

func manifestPath(dir string, generation int) string {
	return filepath.Join(dir, fmt.Sprintf("%s%010d%s", manifestPrefix, generation, manifestSuffix))
}

func segmentFilePath(dir string, segID int64) string {
	return filepath.Join(dir, fmt.Sprintf("%s%010d%s", segmentPrefix, segID, segmentSuffix))
}

func tmpPath(p string) string { return p + ".tmp" }

// writeAtomic 先写 .tmp、fsync，再 rename，最后 fsync 目录，保证崩溃后不会出现半个正式文件。
func writeAtomic(path string, data []byte) error {
	tp := tmpPath(path)
	f, err := os.OpenFile(tp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tp)
		return err
	}
	if err := os.Rename(tp, path); err != nil {
		os.Remove(tp)
		return err
	}
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

func putStr(buf []byte, s string) []byte {
	var lb [4]byte
	binary.LittleEndian.PutUint32(lb[:], uint32(len(s)))
	buf = append(buf, lb[:]...)
	buf = append(buf, s...)
	return buf
}

func readStr(fields []byte, off int) (string, int, error) {
	if off+4 > len(fields) {
		return "", 0, errBadManifest
	}
	l := int(binary.LittleEndian.Uint32(fields[off : off+4]))
	off += 4
	if off+l > len(fields) {
		return "", 0, errBadManifest
	}
	return string(fields[off : off+l]), off + l, nil
}

var errBadManifest = corruptError(0, CorruptBadHeader, "malformed manifest", nil)

// encodeManifest 产出确定性的二进制清单（无 map，顺序即列创建/片段提交顺序）。
func encodeManifest(m *manifest) []byte {
	buf := make([]byte, 0, manifestHeader)
	buf = append(buf, manifestMagic...)
	buf = append(buf, segmentFormatVersion)
	var mb [8]byte
	binary.LittleEndian.PutUint32(mb[:], uint32(m.Generation))
	buf = append(buf, mb[:4]...)
	binary.LittleEndian.PutUint32(mb[:], uint32(m.SchemaVersion))
	buf = append(buf, mb[:4]...)
	binary.LittleEndian.PutUint64(mb[:], uint64(m.NextFieldID))
	buf = append(buf, mb[:]...)
	binary.LittleEndian.PutUint64(mb[:], uint64(m.NextSegmentID))
	buf = append(buf, mb[:]...)
	binary.LittleEndian.PutUint32(mb[:], uint32(len(m.Columns)))
	buf = append(buf, mb[:4]...)
	binary.LittleEndian.PutUint32(mb[:], uint32(len(m.Segments)))
	buf = append(buf, mb[:4]...)

	for _, c := range m.Columns {
		rec := make([]byte, colEntrySize)
		binary.LittleEndian.PutUint64(rec[0:8], uint64(c.ID))
		rec[8] = byte(c.Type)
		rec[9] = boolByte(c.Nullable)
		rec[10] = boolByte(c.HasDefault)
		rec[11] = boolByte(c.Dropped)
		buf = append(buf, rec...)
		buf = putStr(buf, c.Name)
		binary.LittleEndian.PutUint32(mb[:], uint32(len(c.PreviousNames)))
		buf = append(buf, mb[:4]...)
		for _, p := range c.PreviousNames {
			buf = putStr(buf, p)
		}
		if c.HasDefault {
			buf = append(buf, encodeValue(c.Default)...)
		}
	}
	for _, seg := range m.Segments {
		rec := make([]byte, segEntrySize)
		binary.LittleEndian.PutUint64(rec[0:8], uint64(seg.ID))
		binary.LittleEndian.PutUint32(rec[8:12], uint32(seg.SchemaVersion))
		binary.LittleEndian.PutUint32(rec[12:16], uint32(seg.RowCount))
		buf = append(buf, rec...)
	}
	return buf
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// encodeValue 编码一个非空标量（默认值专用）。
func encodeValue(v Value) []byte {
	out := []byte{byte(v.Type)}
	switch v.Type {
	case TypeInt64:
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(v.Int))
		out = append(out, b[:]...)
	case TypeFloat64:
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], floatToBits(v.Float))
		out = append(out, b[:]...)
	case TypeBool:
		out = append(out, boolByte(v.Bool))
	case TypeString:
		out = putStr(out, v.Str)
	}
	return out
}

func decodeValue(t ColumnType, data []byte, off int) (Value, int, error) {
	v := Value{Type: t}
	switch t {
	case TypeInt64:
		if off+8 > len(data) {
			return Value{}, 0, errBadManifest
		}
		v.Int = int64(binary.LittleEndian.Uint64(data[off : off+8]))
		off += 8
	case TypeFloat64:
		if off+8 > len(data) {
			return Value{}, 0, errBadManifest
		}
		v.Float = bitsToFloat(binary.LittleEndian.Uint64(data[off : off+8]))
		off += 8
	case TypeBool:
		if off+1 > len(data) {
			return Value{}, 0, errBadManifest
		}
		v.Bool = data[off] != 0
		off++
	case TypeString:
		s, n, err := readStr(data, off)
		if err != nil {
			return Value{}, 0, err
		}
		v.Str = s
		off = n
	default:
		return Value{}, 0, errBadManifest
	}
	return v, off, nil
}

func decodeManifest(data []byte) (*manifest, error) {
	if len(data) < manifestHeader || string(data[0:8]) != manifestMagic {
		return nil, errBadManifest
	}
	if data[8] != segmentFormatVersion {
		return nil, corruptError(0, CorruptBadVersion, "unsupported manifest version", nil)
	}
	off := 9 // generation(uint32) 紧跟 magic(8)+version(1)
	m := &manifest{Generation: int(binary.LittleEndian.Uint32(data[off : off+4]))}
	off = manifestHeader
	need := func(n int) error {
		if off+n > len(data) {
			return errBadManifest
		}
		return nil
	}
	if err := need(20); err != nil {
		return nil, err
	}
	m.SchemaVersion = int(binary.LittleEndian.Uint32(data[off : off+4]))
	off += 4
	m.NextFieldID = FieldID(binary.LittleEndian.Uint64(data[off : off+8]))
	off += 8
	m.NextSegmentID = int64(binary.LittleEndian.Uint64(data[off : off+8]))
	off += 8
	colCount := int(binary.LittleEndian.Uint32(data[off : off+4]))
	off += 4
	segCount := int(binary.LittleEndian.Uint32(data[off : off+4]))
	off += 4

	for i := 0; i < colCount; i++ {
		if err := need(colEntrySize); err != nil {
			return nil, err
		}
		c := &ColumnDef{
			ID:         FieldID(binary.LittleEndian.Uint64(data[off : off+8])),
			Type:       ColumnType(data[off+8]),
			Nullable:   data[off+9] != 0,
			HasDefault: data[off+10] != 0,
			Dropped:    data[off+11] != 0,
		}
		off += colEntrySize
		name, n, err := readStr(data, off)
		if err != nil {
			return nil, err
		}
		c.Name = name
		off = n
		if err := need(4); err != nil {
			return nil, err
		}
		pn := int(binary.LittleEndian.Uint32(data[off : off+4]))
		off += 4
		for j := 0; j < pn; j++ {
			p, nn, err := readStr(data, off)
			if err != nil {
				return nil, err
			}
			c.PreviousNames = append(c.PreviousNames, p)
			off = nn
		}
		if c.HasDefault {
			if err := need(1); err != nil {
				return nil, err
			}
			t := ColumnType(data[off])
			off++
			dv, nn, err := decodeValue(t, data, off)
			if err != nil {
				return nil, err
			}
			c.Default = dv
			off = nn
		}
		m.Columns = append(m.Columns, c)
	}
	for i := 0; i < segCount; i++ {
		if err := need(segEntrySize); err != nil {
			return nil, err
		}
		seg := segmentInfo{
			ID:            int64(binary.LittleEndian.Uint64(data[off : off+8])),
			SchemaVersion: int(binary.LittleEndian.Uint32(data[off+8 : off+12])),
			RowCount:      int(binary.LittleEndian.Uint32(data[off+12 : off+16])),
		}
		off += segEntrySize
		m.Segments = append(m.Segments, seg)
	}
	return m, nil
}

// loadLatestManifest 扫描目录，按代际取最大清单；损坏的旧清单跳过，
// 最新清单损坏则明确报错（绝不静默丢数据后继续）。
func loadLatestManifest(dir string) (*manifest, int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, err
	}
	var names []string
	maxSegID := int64(0)
	for _, e := range entries {
		n := e.Name()
		if len(n) >= len(manifestPrefix)+len(manifestSuffix) &&
			n[:len(manifestPrefix)] == manifestPrefix &&
			n[len(n)-len(manifestSuffix):] == manifestSuffix {
			names = append(names, n)
		}
		// 孤儿片段也参与 nextSegmentID 推导，避免崩溃后 ID 复用。
		var id int64
		if _, err := fmt.Sscanf(n, segmentPrefix+"%010d"+segmentSuffix, &id); err == nil {
			if id >= maxSegID {
				maxSegID = id + 1
			}
		}
	}
	sort.Strings(names) // 零填充命名保证字典序即代际序
	var latest *manifest
	for i := len(names) - 1; i >= 0; i-- {
		data, err := os.ReadFile(filepath.Join(dir, names[i]))
		if err != nil {
			continue
		}
		m, err := decodeManifest(data)
		if err != nil {
			if i == len(names)-1 {
				return nil, 0, corruptError(0, CorruptBadHeader,
					"latest manifest "+names[i]+" is unreadable", err)
			}
			continue
		}
		latest = m
		break
	}
	if latest != nil && latest.NextSegmentID < maxSegID {
		latest.NextSegmentID = maxSegID
	}
	if latest == nil {
		return nil, int(maxSegID), nil
	}
	return latest, 0, nil
}

func stateFromManifest(m *manifest) *engineState {
	st := &engineState{
		generation:    m.Generation,
		schemaVersion: m.SchemaVersion,
		nextFieldID:   m.NextFieldID,
		nextSegmentID: m.NextSegmentID,
		schema:        &Schema{Version: m.SchemaVersion, Columns: m.Columns},
		segments:      append([]segmentInfo(nil), m.Segments...),
	}
	return st
}

func manifestFromState(st *engineState) *manifest {
	return &manifest{
		Generation:    st.generation,
		SchemaVersion: st.schemaVersion,
		NextFieldID:   st.nextFieldID,
		NextSegmentID: st.nextSegmentID,
		Columns:       st.schema.Columns,
		Segments:      st.segments,
	}
}
