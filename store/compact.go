package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// CompactOptions 手动整理选项。
type CompactOptions struct {
	// BudgetBytes 整理可使用的内存预算（字节）；0 表示不限。
	// 预算不足时返回 RESOURCE_EXHAUSTED，且不产生任何可见变更。
	BudgetBytes int64
}

// CompactResult 一次整理的结果描述。
type CompactResult struct {
	Version        uint64 // 整理提交版本（未发生整理时为当前版本）
	InputFragments int    // 被合并的片段数
	OutputFile     string // 合并后片段文件名（未发生整理时为空）
	Rows           int    // 合并的行数
	Retried        bool   // 整理期间结构发生变化而内部重试
}

// 整理测试钩子：便于让“整理进行中”与“新写入”确定性交错。
// beforeReadInputs: 读取输入片段之前（此时未持 store 锁）。
// beforeCommit:    合并完成、即将原子提交之前。
var (
	compactHookBeforeReadInputs func(table string)
	compactHookBeforeCommit     func(table string, rows int)
)

// Compact 手动整理表：把当前全部可见小片段合并为一个更大的片段。
//
// 原子性与中断安全：
//   - 合并输出先写到临时文件并原子改名，但在提交前对任何快照都不可见；
//   - 只有清单原子更新成功后，新片段才可见、旧片段才被回收；
//   - 合并期间若发生结构演进（或并发整理先提交），放弃本次输出并整体重试；
//   - 上下文取消 / 预算不足 / 片段损坏均在提交前返回，存储状态完全不变，
//     中断后重试不会产生重复行、丢失行或合并到一半的可见数据。
func (s *Store) Compact(ctx context.Context, table string, opts *CompactOptions) (*CompactResult, error) {
	if opts == nil {
		opts = &CompactOptions{}
	}
	// 同一时刻每张表只允许一个整理（手动/自动共享该标记）。
	s.mu.Lock()
	if _, err := s.tableLocked(table); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if s.compacting[table] {
		s.mu.Unlock()
		return nil, &Error{Code: ErrCodeConflict, Reason: "COMPACT_IN_PROGRESS",
			Op: "compact", Err: fmt.Errorf("another compaction is running for %q", table)}
	}
	s.compacting[table] = true
	s.mu.Unlock()
	defer s.finishCompact(table)
	return s.compactLoop(ctx, table, opts)
}

// compactLoop 执行整理（含结构变化时的整体重试）。调用方必须已经认领
// compacting 标记；本函数返回时由 Compact 的 defer 负责清理。
func (s *Store) compactLoop(ctx context.Context, table string, opts *CompactOptions) (*CompactResult, error) {
	res := &CompactResult{}
	const maxAttempts = 8
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, &Error{Code: ErrCodeInterrupted, Op: "compact", Err: err}
		}
		s.mu.Lock()
		ts, err := s.tableLocked(table)
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
		if ts.dropped {
			s.mu.Unlock()
			return nil, conflictErr("compact", ConflictTableDropped, table)
		}
		snapshotFrags := append([]FragmentMeta{}, lastFragments(ts)...)
		schemaV := ts.schema.Version
		headV := ts.commitV
		schema := ts.schema.clone()
		s.mu.Unlock()

		out, outFile, size, budgetErr, buildErr := s.buildCompactOutput(ctx, table, schema, snapshotFrags, opts.BudgetBytes)
		if buildErr != nil || budgetErr != nil {
			if outFile != "" {
				os.Remove(filepath.Join(s.dir, table, filepath.Base(outFile)))
			}
			if budgetErr != nil {
				return nil, budgetErr
			}
			return nil, buildErr
		}

		committed, commitErr := s.commitCompact(table, schemaV, headV, snapshotFrags, out, outFile, size)
		if commitErr == errCompactRetry {
			// 结构演进或并发写入先行提交：丢弃输出并重试。
			res.Retried = true
			os.Remove(outFile)
			continue
		}
		if commitErr != nil {
			os.Remove(outFile)
			return nil, commitErr
		}
		res.Version = committed
		res.InputFragments = len(snapshotFrags)
		if out != nil {
			res.OutputFile = filepath.Base(outFile)
			res.Rows = out.NumRows
		}
		return res, nil
	}
	return nil, &Error{Code: ErrCodeConflict, Reason: "COMPACT_RETRY_EXHAUSTED",
		Op: "compact", Err: fmt.Errorf("schema changed repeatedly during compaction")}
}

func (s *Store) finishCompact(table string) {
	s.mu.Lock()
	delete(s.compacting, table)
	s.mu.Unlock()
}

// buildCompactOutput 读取输入并构建合并片段；不触碰任何提交状态。
// 返回的 outFile 为磁盘绝对路径（已原子改名，尚未提交）。
func (s *Store) buildCompactOutput(ctx context.Context, table string, schema *Schema,
	inputs []FragmentMeta, budget int64) (out *Fragment, outFile string, size int64, budgetErr, buildErr error) {

	if hook := compactHookBeforeReadInputs; hook != nil {
		hook(table)
	}

	// 合并后的列只包含当前结构可见字段；历史缺列补 NULL，
	// 已删除列不再写入输出（历史原始文件在保留窗口内仍服务于旧版本）。
	ids := make([]int, 0, len(schema.Fields))
	for i := range schema.Fields {
		ids = append(ids, schema.Fields[i].ID)
	}
	cols := make(map[int][]Value, len(ids))
	for _, id := range ids {
		cols[id] = make([]Value, 0)
	}
	totalRows := 0
	var lim *MemLimiter
	if budget > 0 {
		lim = &MemLimiter{BudgetBytes: budget}
	}
	// 输出列容器的固定开销。
	if err := lim.Acquire(estimateColumnOverhead(int64(len(ids)))); err != nil {
		return nil, "", 0, err, nil
	}

	for _, meta := range inputs {
		if err := ctx.Err(); err != nil {
			return nil, "", 0, nil, &Error{Code: ErrCodeInterrupted, Op: "compact", Err: err}
		}
		frag, err := s.loadFragment(table, meta)
		if err != nil {
			return nil, "", 0, nil, err // CORRUPT_FRAGMENT / INTERRUPTED
		}
		// 输入必须可被整理时的结构解释（未知列说明文件与结构不匹配）。
		for id := range frag.Columns {
			if schema.FieldByID(id) == nil && !droppedHas(schema, id) {
				return nil, "", 0, nil, &Error{Code: ErrCodeCorruptFragment,
					Reason: fmt.Sprintf("UNKNOWN_FIELD_ID:%d", id), Op: "compact",
					Err: fmt.Errorf("compaction input %s has column not in schema", meta.File)}
			}
		}
		// 预算只覆盖实际搬运的单元格与输出行，不随整表历史规模膨胀。
		if err := lim.Acquire(estimateCellsBytes(frag, schema)); err != nil {
			return nil, "", 0, err, nil
		}
		for r := 0; r < frag.NumRows; r++ {
			for _, id := range ids {
				var v Value
				if col, ok := frag.Columns[id]; ok {
					v = col[r]
				}
				// v == nil（历史缺列或显式 NULL）在合并后仍是 NULL。
				cols[id] = append(cols[id], v)
			}
		}
		totalRows += frag.NumRows
	}

	// 不足两个输入时不需要整理：保持现状（不产生新提交）。
	if len(inputs) < 2 {
		return nil, "", 0, nil, nil
	}

	s.mu.Lock()
	seq := s.nextCompactSeqLocked()
	s.mu.Unlock()

	out = &Fragment{
		Key:     fmt.Sprintf("%sseq-%d", reservedKeyPrefix, seq),
		SchemaV: schema.Version,
		NumRows: totalRows,
		Columns: cols,
	}
	// 整理输出文件名不带提交版本（提交版本此刻尚不可知）；
	// 用固定前缀 + 全局序号即可保证全表唯一。
	fileName := fmt.Sprintf("frag-comp-%d.bin", seq)
	path := filepath.Join(s.dir, table, fileName)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, "", 0, nil, err
	}
	tmp := path + ".tmp"
	w, err := os.Create(tmp)
	if err != nil {
		return nil, "", 0, nil, err
	}
	if err := encodeFragment(w, out); err != nil {
		w.Close()
		os.Remove(tmp)
		return nil, "", 0, nil, err
	}
	if err := w.Close(); err != nil {
		return nil, "", 0, nil, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return nil, "", 0, nil, err
	}
	size = fileSize(path)

	if hook := compactHookBeforeCommit; hook != nil {
		hook(table, totalRows)
	}
	return out, path, size, nil, nil
}

// errCompactRetry 内部信号：提交点发现结构已变化，需要整体重试。
var errCompactRetry = fmt.Errorf("compact retry")

// commitCompact 在临界区内原子地发布整理结果。
// 仅当结构版本、头版本、输入片段集合均未变化时才提交。
func (s *Store) commitCompact(table string, schemaV int, headV uint64, inputMetas []FragmentMeta,
	out *Fragment, outPath string, outSize int64) (uint64, error) {

	s.mu.Lock()
	defer s.mu.Unlock()
	ts, err := s.tableLocked(table)
	if err != nil {
		return 0, err
	}
	if out == nil {
		// 无需整理（少于 2 个片段）：返回当前版本。
		return ts.commitV, nil
	}
	if ts.dropped {
		return 0, conflictErr("compact", ConflictTableDropped, table)
	}
	if ts.schema.Version != schemaV || ts.commitV != headV {
		return 0, errCompactRetry
	}
	cur := lastFragments(ts)
	if !sameFragmentSet(cur, inputMetas) {
		return 0, errCompactRetry
	}

	ts.commitV++
	meta := FragmentMeta{
		Key: out.Key, SchemaV: out.SchemaV, NumRows: out.NumRows,
		File: filepath.Base(outPath), Size: outSize,
		Stats: computeColumnStats(out, ts.schema),
	}
	// 合并片段位于可见列表最前，其余可见片段全部被其替代。
	frags := []FragmentMeta{meta}
	ts.commits = append(ts.commits, &commitRecord{
		version: ts.commitV, schema: ts.schema, fragments: frags, kind: commitKindCompact,
	})
	s.trimCommitsLocked(ts)
	if err := s.persistLocked(table, ts); err != nil {
		// 清单未更新：输出文件成为孤儿，启动时清理，状态不变。
		ts.commitV--
		ts.commits = ts.commits[:len(ts.commits)-1]
		return 0, err
	}
	// 清单已持久化：回收不再被任何保留提交引用的文件。
	if err := s.gcUnlocked(table, ts); err != nil {
		return 0, err
	}
	return ts.commitV, nil
}

// sameFragmentSet 判断两个片段元信息集合是否为同一组（按文件名比较，顺序敏感）。
func sameFragmentSet(a, b []FragmentMeta) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].File != b[i].File {
			return false
		}
	}
	return true
}

func estimateColumnOverhead(cols int64) int64 {
	const sliceAndMapOverhead = 256
	return cols*64 + sliceAndMapOverhead
}
