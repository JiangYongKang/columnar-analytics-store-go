package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// CompactionOptions 手动整理的选项。
type CompactionOptions struct {
	// MemBudget 整理期间允许使用的最大内存字节，按“处理单个源片段时
	// 的驻留量”估算：计划阶段只驻留一个文件的头部，写入阶段按列处理，
	// 每次只驻留一个源的一列 payload；<=0 表示不限。
	MemBudget int64
}

// CompactionResult 一次整理的结果。
type CompactionResult struct {
	Compacted       bool
	BeforeVersion   uint64
	AfterVersion    uint64
	MergedFragments int   // 被合并的源片段数
	Rows            int   // 合并后行数
	Bytes           int64 // 合并后文件字节数
}

// fragmentMerger 把多个源片段流式合并成一个 CFR2 片段。
//
// 内存模型：计划阶段逐源解析头部（随即释放），写入阶段“按列、按源”
// 只解码一列 payload；任何时刻内存里只驻留一个源的一列。源片段估算
// 字节超过预算时返回 RESOURCE_EXHAUSTED；失败即删除临时文件，不留
// 半成品，可见状态不变。
type fragmentMerger struct {
	tableDir  string
	sources   []FragmentMeta
	schemaV   int
	budget    int64
	colIDs    []int
	colTypes  map[int]FieldType
	srcCols   []map[int]FieldType // 每个源片段的列集合（头部解析结果）
	totalRows int
}

func newFragmentMerger(tableDir string, sources []FragmentMeta, schemaV int, budget int64) *fragmentMerger {
	return &fragmentMerger{
		tableDir: tableDir, sources: sources, schemaV: schemaV,
		budget:   budget,
		colTypes: map[int]FieldType{},
	}
}

// plan 逐源解析头部：校验、汇总行数、求列集合并集与类型一致性。
func (m *fragmentMerger) plan(ctx context.Context) error {
	seen := map[int]bool{}
	for si, meta := range m.sources {
		if err := ctx.Err(); err != nil {
			return &Error{Code: ErrCodeInterrupted, Op: "compact-plan", Err: err}
		}
		if err := m.acquire(meta); err != nil {
			return err
		}
		path := filepath.Join(m.tableDir, meta.File)
		data, err := os.ReadFile(path)
		if err != nil {
			return &Error{Code: ErrCodeInterrupted, Op: "compact-plan", Err: err}
		}
		colTypes, _, _, err := fragmentHeaderInfo(data)
		if err != nil {
			return err
		}
		for id, t := range colTypes {
			if t != TypeFieldNull {
				if old, ok := m.colTypes[id]; ok && old != t {
					return &Error{Code: ErrCodeSchemaIncompatible, Op: "compact-plan",
						Reason: fmt.Sprintf("MERGE_TYPE_MISMATCH:%d", id),
						Err:    fmt.Errorf("column %d has conflicting types %s/%s", id, old, t)}
				}
				m.colTypes[id] = t
			}
			if !seen[id] {
				seen[id] = true
				m.colIDs = append(m.colIDs, id)
			}
		}
		m.srcCols = append(m.srcCols, colTypes)
		m.totalRows += meta.NumRows
		_ = si
	}
	sort.Ints(m.colIDs)
	return nil
}

// write 执行合并并原子落盘到 outPath（先写 outPath+".merge-tmp" 再 rename）。
func (m *fragmentMerger) write(ctx context.Context, outPath, key string) (*Fragment, int64, error) {
	tmp := outPath + ".merge-tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return nil, 0, err
	}
	cleanup := func() {
		f.Close()
		os.Remove(tmp)
	}

	hdr := &bytes.Buffer{}
	hdr.Write(fragmentMagicV2)
	writeUvarint(hdr, uint64(m.schemaV))
	writeUvarint(hdr, uint64(m.totalRows))
	writeUvarint(hdr, uint64(len(m.colIDs)))
	if _, err := f.Write(hdr.Bytes()); err != nil {
		cleanup()
		return nil, 0, err
	}

	zones := ZoneMap{}
	perColNull := make([]int, len(m.colIDs))
	for ci, id := range m.colIDs {
		if err := ctx.Err(); err != nil {
			cleanup()
			return nil, 0, &Error{Code: ErrCodeInterrupted, Op: "compact-write", Err: err}
		}
		typ := m.colTypes[id]
		if typ == "" {
			typ = TypeFieldNull
		}
		payload := &bytes.Buffer{}
		var zoneMin, zoneMax Value
		for si, meta := range m.sources {
			if err := ctx.Err(); err != nil {
				cleanup()
				return nil, 0, &Error{Code: ErrCodeInterrupted, Op: "compact-write", Err: err}
			}
			var col []Value
			if _, present := m.srcCols[si][id]; present {
				if err := m.acquire(meta); err != nil {
					cleanup()
					return nil, 0, err
				}
				col, err = m.loadColumn(meta, id, typ)
				if err != nil {
					cleanup()
					return nil, 0, err
				}
			} else {
				// 源片段缺列（结构演进前写入）=> 该源在此列全为 NULL。
				col = make([]Value, meta.NumRows)
			}
			if err := encodeColumn(payload, col, typ); err != nil {
				cleanup()
				return nil, 0, err
			}
			for _, v := range col {
				if v == nil {
					perColNull[ci]++
					continue
				}
				if zoneMin == nil || valueLess(v, zoneMin) {
					zoneMin = v
				}
				if zoneMax == nil || valueLess(zoneMax, v) {
					zoneMax = v
				}
			}
		}

		ch := &bytes.Buffer{}
		writeUvarint(ch, uint64(id))
		writeUvarint(ch, uint64(len(typ)))
		ch.WriteString(string(typ))
		var crcBuf [4]byte
		binary.BigEndian.PutUint32(crcBuf[:], crc32.ChecksumIEEE(payload.Bytes()))
		ch.Write(crcBuf[:])
		writeUvarint(ch, uint64(payload.Len()))
		if _, err := f.Write(ch.Bytes()); err != nil {
			cleanup()
			return nil, 0, err
		}
		if _, err := f.Write(payload.Bytes()); err != nil {
			cleanup()
			return nil, 0, err
		}

		zones[id] = &ColumnZone{FieldID: id, Type: typ,
			Min: zoneMin, Max: zoneMax, Nulls: perColNull[ci]}
	}

	zoneStart, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		cleanup()
		return nil, 0, err
	}
	zoneBytes, err := encodeZoneSection(zones)
	if err != nil {
		cleanup()
		return nil, 0, err
	}
	if _, err := f.Write(zoneBytes); err != nil {
		cleanup()
		return nil, 0, err
	}
	var offBuf [8]byte
	putU64(offBuf[:], uint64(zoneStart))
	if _, err := f.Write(offBuf[:]); err != nil {
		cleanup()
		return nil, 0, err
	}
	st, err := f.Stat()
	if err != nil {
		cleanup()
		return nil, 0, err
	}
	if err := f.Sync(); err != nil {
		cleanup()
		return nil, 0, err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return nil, 0, err
	}
	if err := os.Rename(tmp, outPath); err != nil {
		os.Remove(tmp)
		return nil, 0, err
	}

	return &Fragment{
		Key:      key,
		SchemaV:  m.schemaV,
		NumRows:  m.totalRows,
		Zones:    zones,
		ColTypes: m.colTypes,
	}, st.Size(), nil
}

func (m *fragmentMerger) acquire(meta FragmentMeta) error {
	if m.budget <= 0 {
		return nil
	}
	cost := meta.Size
	if cost < 1 {
		cost = 1
	}
	if cost > m.budget {
		return &Error{Code: ErrCodeResourceExhausted, Op: "compact-memory",
			Reason: fmt.Sprintf("BUDGET_%d_EXCEEDED", m.budget),
			Err:    fmt.Errorf("source fragment %s needs ~%d bytes", meta.Key, cost)}
	}
	return nil
}

// loadColumn 只读取并解码一个源片段的一列（CRC 校验后解码）。
func (m *fragmentMerger) loadColumn(meta FragmentMeta, id int, typ FieldType) ([]Value, error) {
	path := filepath.Join(m.tableDir, meta.File)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, &Error{Code: ErrCodeInterrupted, Op: "compact-read", Err: err}
	}
	buf := bytes.NewReader(data)
	magic := make([]byte, 4)
	if _, err := io.ReadFull(buf, magic); err != nil ||
		(string(magic) != string(fragmentMagic) && string(magic) != string(fragmentMagicV2)) {
		return nil, corruptErr("BAD_MAGIC")
	}
	readUvarint(buf) // schemaV
	numRows := int(readUvarint(buf))
	numCols := int(readUvarint(buf))
	for c := 0; c < numCols; c++ {
		cid := int(readUvarint(buf))
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
			return nil, corruptErr(fmt.Sprintf("CRC_MISMATCH_COLUMN_%d", cid))
		}
		if cid == id {
			return decodeColumn(payload, FieldType(tbuf), numRows)
		}
	}
	// 头部宣称有该列却没找到：数据损坏。
	return nil, corruptErr(fmt.Sprintf("MISSING_COLUMN_%d_IN_PAYLOAD", id))
}

// Compact 手动触发表整理；没有可合并片段（0/1 个）时返回
// Compacted=false 且不产生新版本。
//
// 语义保证：
//   - 整理前后相同查询结果完全一致（行集不变，按列原样拼接并重建 zone）；
//   - 与并发写入互不丢数据：整理基于开始时的片段计划，期间的新写入
//     保留在计划之外并在同一次提交中一并挂回；
//   - 任何失败（预算/中断/损坏）都不改变可见状态，临时文件被清理，
//     存储回到可继续读写的状态。
func (s *Store) Compact(ctx context.Context, table string, opts CompactionOptions) (*CompactionResult, error) {
	return s.compact(ctx, table, opts.MemBudget, "manual")
}
