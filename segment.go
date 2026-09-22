package colstore

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

const (
	segmentMagic         = "CASSEG01"
	segmentFormatVersion = uint8(1)
	segmentHeaderSize    = 8 + 1 + 8 + 4 + 4 + 4 // magic + version + segID + schemaVer + rowCount + chunkCount
	chunkHeaderSize      = 8 + 1 + 4 + 4 + 4 + 4 + 4
)

// chunk 是一个字段在一个片段中的列式数据。null 槽在类型切片中仍占位置，
// 保证行号即下标，读取语义确定。
type chunk struct {
	id     FieldID
	typ    ColumnType
	n      int
	nulls  []uint64
	ints   []int64
	floats []float64
	strs   []string
	bools  []byte
}

func (c *chunk) isNull(row int) bool {
	if c.nulls == nil {
		return false
	}
	return c.nulls[row/64]&(1<<uint(row%64)) != 0
}

func nullBitmapFor(n int) []byte {
	return make([]byte, ((n+63)/64)*8)
}

func setNullBit(bm []byte, row int) {
	bm[row/8] |= 1 << uint(row%8)
}

// encodeChunkData 按类型产出确定的小端字节表示。
func encodeChunkData(c *chunk) []byte {
	switch c.typ {
	case TypeInt64:
		buf := make([]byte, 8*c.n)
		for i, v := range c.ints {
			binary.LittleEndian.PutUint64(buf[i*8:], uint64(v))
		}
		return buf
	case TypeFloat64:
		buf := make([]byte, 8*c.n)
		for i, v := range c.floats {
			binary.LittleEndian.PutUint64(buf[i*8:], floatToBits(v))
		}
		return buf
	case TypeBool:
		return append([]byte(nil), c.bools...)
	case TypeString:
		size := 0
		for _, s := range c.strs {
			size += 4 + len(s)
		}
		buf := make([]byte, 0, size)
		var lb [4]byte
		for i := 0; i < c.n; i++ {
			if c.isNull(i) {
				binary.LittleEndian.PutUint32(lb[:], 0)
				buf = append(buf, lb[:]...)
				continue
			}
			binary.LittleEndian.PutUint32(lb[:], uint32(len(c.strs[i])))
			buf = append(buf, lb[:]...)
			buf = append(buf, c.strs[i]...)
		}
		return buf
	default:
		panic(fmt.Sprintf("colstore: cannot encode chunk of type %d", c.typ))
	}
}

// buildNullBitmap 依据各类型槽产出空值位图。
func buildNullBitmap(c *chunk) []byte {
	bm := nullBitmapFor(c.n)
	for i := 0; i < c.n; i++ {
		isNull := false
		switch c.typ {
		case TypeInt64, TypeFloat64, TypeBool, TypeString:
			// 统一由外部 nulls 字段决定；构造 chunk 时已填充。
		}
		if c.nulls != nil && c.nulls[i/64]&(1<<uint(i%64)) != 0 {
			isNull = true
		}
		if isNull {
			setNullBit(bm, i)
		}
	}
	return bm
}

// encodeSegment 把一组列式块编码为不可变片段文件（直接写给定路径）。
// 块按 FieldID 升序写入且不得重复；调用方负责先写临时文件再原子改名。
func encodeSegment(path string, segID int64, schemaVersion int, rowCount int, chunks []*chunk) error {
	// 确定性顺序校验：严格升序，拒绝重复 ID。
	var prev FieldID
	for i, c := range chunks {
		if i > 0 && c.id <= prev {
			return fmt.Errorf("colstore: chunks must be strictly ordered by field id")
		}
		if c.n != rowCount {
			return fmt.Errorf("colstore: chunk %d row count mismatch", c.id)
		}
		prev = c.id
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := newCountingWriter(f)
	hdr := make([]byte, segmentHeaderSize)
	copy(hdr[0:8], segmentMagic)
	hdr[8] = segmentFormatVersion
	binary.LittleEndian.PutUint64(hdr[9:17], uint64(segID))
	binary.LittleEndian.PutUint32(hdr[17:21], uint32(schemaVersion))
	binary.LittleEndian.PutUint32(hdr[21:25], uint32(rowCount))
	binary.LittleEndian.PutUint32(hdr[25:29], uint32(len(chunks)))
	if err := writeFull(w, hdr); err != nil {
		return err
	}

	var lb [4]byte
	for _, c := range chunks {
		nullBM := buildNullBitmap(c)
		data := encodeChunkData(c)
		ch := make([]byte, chunkHeaderSize)
		binary.LittleEndian.PutUint64(ch[0:8], uint64(c.id))
		ch[8] = byte(c.typ)
		binary.LittleEndian.PutUint32(ch[9:13], uint32(c.n))
		binary.LittleEndian.PutUint32(ch[13:17], uint32(len(nullBM)))
		binary.LittleEndian.PutUint32(ch[17:21], uint32(len(data)))
		binary.LittleEndian.PutUint32(ch[21:25], crc32.ChecksumIEEE(nullBM))
		binary.LittleEndian.PutUint32(ch[25:29], crc32.ChecksumIEEE(data))
		if err := writeFull(w, ch); err != nil {
			return err
		}
		if err := writeFull(w, nullBM); err != nil {
			return err
		}
		if err := writeFull(w, data); err != nil {
			return err
		}
		_ = lb
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

// decodeChunks 顺序读取片段，只解码 want 中的字段块；不需要的块仅跳过。
// bytesRead 计量的是头部扫描与实际解码载荷之和，使查询预算与命中范围相关，
// 而不是与整表规模线性绑定。任何结构/校验异常归一为片段损坏错误。
func decodeChunks(ctx context.Context, path string, segID int64, want map[FieldID]bool, budget *scanBudget) (map[FieldID]*chunk, int64, error) {
	const op = "decode"
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, classified(CatIO, "open", op, segID, "cannot open segment", err)
	}
	defer f.Close()
	var r io.Reader = f

	hdr := make([]byte, segmentHeaderSize)
	if err := readFull(ctx, r, hdr); err != nil {
		return nil, 0, mapReadError(segID, err)
	}
	if err := budget.charge(int64(segmentHeaderSize)); err != nil {
		return nil, 0, err
	}
	if string(hdr[0:8]) != segmentMagic {
		return nil, 0, corruptError(segID, CorruptBadMagic, "segment magic mismatch", nil)
	}
	if hdr[8] != segmentFormatVersion {
		return nil, 0, corruptError(segID, CorruptBadVersion,
			fmt.Sprintf("format version %d unsupported", hdr[8]), nil)
	}
	fileSegID := int64(binary.LittleEndian.Uint64(hdr[9:17]))
	if fileSegID != segID {
		return nil, 0, corruptError(segID, CorruptBadHeader,
			fmt.Sprintf("segment id in file is %d, expected %d", fileSegID, segID), nil)
	}
	schemaVer := int(binary.LittleEndian.Uint32(hdr[17:21]))
	_ = schemaVer // 片段结构版本随解码结果一并交给查询层做解析。
	rowCount := int(binary.LittleEndian.Uint32(hdr[21:25]))
	chunkCount := int(binary.LittleEndian.Uint32(hdr[25:29]))
	if rowCount < 0 || chunkCount < 0 {
		return nil, 0, corruptError(segID, CorruptBadHeader, "negative row/chunk count", nil)
	}

	out := make(map[FieldID]*chunk)
	var bytesRead int64 = segmentHeaderSize
	var prevID FieldID
	for i := 0; i < chunkCount; i++ {
		if err := ctx.Err(); err != nil {
			return nil, bytesRead, classified(CatReadInterrupted, "context_canceled", op, segID, "canceled between chunks", err)
		}
		ch := make([]byte, chunkHeaderSize)
		if err := readFull(ctx, r, ch); err != nil {
			return nil, bytesRead, mapReadError(segID, err)
		}
		bytesRead += chunkHeaderSize
		if err := budget.charge(chunkHeaderSize); err != nil {
			return nil, bytesRead, err
		}

		id := FieldID(binary.LittleEndian.Uint64(ch[0:8]))
		typ := ColumnType(ch[8])
		cnt := int(binary.LittleEndian.Uint32(ch[9:13]))
		nullLen := int(binary.LittleEndian.Uint32(ch[13:17]))
		dataLen := int(binary.LittleEndian.Uint32(ch[17:21]))
		nullCRC := binary.LittleEndian.Uint32(ch[21:25])
		dataCRC := binary.LittleEndian.Uint32(ch[25:29])

		if i > 0 && id <= prevID {
			return nil, bytesRead, corruptError(segID, CorruptChunkOrder,
				fmt.Sprintf("chunk id %d out of strict ascending order", id), nil)
		}
		if _, dup := out[id]; dup {
			return nil, bytesRead, corruptError(segID, CorruptDuplicate,
				fmt.Sprintf("duplicate chunk id %d", id), nil)
		}
		prevID = id
		if cnt != rowCount {
			return nil, bytesRead, corruptError(segID, CorruptBadHeader,
				fmt.Sprintf("chunk %d count %d != segment row count %d", id, cnt, rowCount), nil)
		}
		if !typ.valid() {
			return nil, bytesRead, corruptError(segID, CorruptBadHeader,
				fmt.Sprintf("chunk %d has invalid type %d", id, typ), nil)
		}
		if nullLen != len(nullBitmapFor(cnt)) {
			return nil, bytesRead, corruptError(segID, CorruptBadHeader,
				fmt.Sprintf("chunk %d null bitmap length %d invalid for %d rows", id, nullLen, cnt), nil)
		}
		if dataLen < 0 {
			return nil, bytesRead, corruptError(segID, CorruptBadHeader, "negative data length", nil)
		}

		// 空值位图始终需要（用于判定 NULL），但只有被查询的字段才解码载荷。
		nullBM := make([]byte, nullLen)
		if err := readFull(ctx, r, nullBM); err != nil {
			return nil, bytesRead, mapReadError(segID, err)
		}
		bytesRead += int64(nullLen)
		if err := budget.charge(int64(nullLen)); err != nil {
			return nil, bytesRead, err
		}
		if crc32.ChecksumIEEE(nullBM) != nullCRC {
			return nil, bytesRead, corruptError(segID, CorruptCRC,
				fmt.Sprintf("chunk %d null bitmap crc mismatch", id), nil)
		}

		wantThis := want[id]
		if wantThis {
			payload := make([]byte, dataLen)
			if err := readFull(ctx, r, payload); err != nil {
				return nil, bytesRead, mapReadError(segID, err)
			}
			if crc32.ChecksumIEEE(payload) != dataCRC {
				return nil, bytesRead, corruptError(segID, CorruptCRC,
					fmt.Sprintf("chunk %d data crc mismatch", id), nil)
			}
			c, err := decodeChunkPayload(id, typ, cnt, nullBM, payload)
			if err != nil {
				return nil, bytesRead, corruptError(segID, CorruptBadHeader, err.Error(), err)
			}
			out[id] = c
			// 只有真正读入内存的目标字段载荷才计入读取量与预算，
			// 因而读取量（及占用）随命中范围而非整表规模增长。
			bytesRead += int64(dataLen)
			if err := budget.charge(int64(dataLen)); err != nil {
				return nil, bytesRead, err
			}
		} else {
			// 不需要的载荷通过 seek 直接越过：不读入、不拷贝、不计入预算。
			if rs, ok := r.(io.Seeker); ok {
				if _, err := rs.Seek(int64(dataLen), io.SeekCurrent); err != nil {
					return nil, bytesRead, mapReadError(segID, err)
				}
			} else {
				if _, err := io.CopyN(io.Discard, r, int64(dataLen)); err != nil {
					return nil, bytesRead, mapReadError(segID, err)
				}
			}
		}
	}
	return out, bytesRead, nil
}

func decodeChunkPayload(id FieldID, typ ColumnType, n int, nullBM []byte, data []byte) (*chunk, error) {
	c := &chunk{id: id, typ: typ, n: n}
	words := len(nullBM) / 8
	c.nulls = make([]uint64, words)
	for i := 0; i < words; i++ {
		c.nulls[i] = binary.LittleEndian.Uint64(nullBM[i*8:])
	}
	switch typ {
	case TypeInt64:
		if len(data) != 8*n {
			return nil, fmt.Errorf("int64 chunk %d data length %d, expected %d", id, len(data), 8*n)
		}
		c.ints = make([]int64, n)
		for i := 0; i < n; i++ {
			c.ints[i] = int64(binary.LittleEndian.Uint64(data[i*8:]))
		}
	case TypeFloat64:
		if len(data) != 8*n {
			return nil, fmt.Errorf("float64 chunk %d data length %d, expected %d", id, len(data), 8*n)
		}
		c.floats = make([]float64, n)
		for i := 0; i < n; i++ {
			c.floats[i] = bitsToFloat(binary.LittleEndian.Uint64(data[i*8:]))
		}
	case TypeBool:
		if len(data) != n {
			return nil, fmt.Errorf("bool chunk %d data length %d, expected %d", id, len(data), n)
		}
		c.bools = append([]byte(nil), data...)
	case TypeString:
		c.strs = make([]string, n)
		off := 0
		for i := 0; i < n; i++ {
			if c.isNull(i) {
				continue
			}
			if off+4 > len(data) {
				return nil, fmt.Errorf("string chunk %d truncated at length prefix row %d", id, i)
			}
			l := int(binary.LittleEndian.Uint32(data[off : off+4]))
			off += 4
			if off+l > len(data) {
				return nil, fmt.Errorf("string chunk %d truncated at payload row %d", id, i)
			}
			c.strs[i] = string(data[off : off+l])
			off += l
		}
	default:
		return nil, fmt.Errorf("chunk %d has unsupported type %d", id, typ)
	}
	return c, nil
}

// mapReadError 把读取层异常归一化：意外 EOF 一律是文件截断型损坏，
// 明确的 context 取消属于读取中断。
func mapReadError(segID int64, err error) error {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return corruptError(segID, CorruptTruncated, "segment ends prematurely", err)
	}
	if ce, ok := err.(*ClassifiedError); ok {
		return ce
	}
	return corruptError(segID, CorruptTruncated, "read failed", err)
}

func readFull(ctx context.Context, r io.Reader, buf []byte) error {
	if err := ctx.Err(); err != nil {
		return classified(CatReadInterrupted, "context_canceled", "decode", 0, "canceled before read", err)
	}
	_, err := io.ReadFull(r, buf)
	return err
}

type countingWriter struct {
	w io.Writer
	n int64
}

func newCountingWriter(w io.Writer) *countingWriter { return &countingWriter{w: w} }
func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n += int64(n)
	return n, err
}

func writeFull(w io.Writer, buf []byte) error {
	for len(buf) > 0 {
		n, err := w.Write(buf)
		if err != nil {
			return err
		}
		buf = buf[n:]
	}
	return nil
}
