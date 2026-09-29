package store

import (
	"context"
	"fmt"
	"path/filepath"
)

// CompactionPolicy 自动整理触发策略（零值表示不自动整理）。
type CompactionPolicy struct {
	// MaxFragments 可见片段数达到该阈值时触发整理；<=0 不按数量触发。
	MaxFragments int
	// MaxTotalBytes 可见片段总字节达到该阈值时触发整理；<=0 不按体积触发。
	MaxTotalBytes int64
	// MemBudget 整理内存预算；<=0 表示不限。
	MemBudget int64
}

// TableOptions 表级长期选项（历史保留与自动整理）。
type TableOptions struct {
	// RetainVersions 保留最近 N 个已提交版本（含当前版本）；
	// 0 视为 1（只保留最新）。
	RetainVersions int
	AutoCompact    CompactionPolicy
}

// SetTableOptions 更新表级选项并立即按新保留窗口裁剪历史。
func (s *Store) SetTableOptions(name string, opts TableOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, err := s.tableLocked(name)
	if err != nil {
		return err
	}
	old := ts.options
	if opts.RetainVersions < 0 {
		opts.RetainVersions = 0
	}
	ts.options = opts
	if err := s.trimAndPersistLocked(name, ts); err != nil {
		ts.options = old // 回滚内存态
		return err
	}
	s.log("表选项更新: 表=%s 保留版本数=%d 自动整理={片段阈值=%d 字节阈值=%d 预算=%d}",
		name, effectiveRetain(opts), opts.AutoCompact.MaxFragments,
		opts.AutoCompact.MaxTotalBytes, opts.AutoCompact.MemBudget)
	return nil
}

// TableOptionsAt 读取表级选项。
func (s *Store) TableOptionsAt(name string) (TableOptions, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, err := s.tableLocked(name)
	if err != nil {
		return TableOptions{}, err
	}
	return ts.options, nil
}

func effectiveRetain(o TableOptions) int {
	if o.RetainVersions <= 0 {
		return 1
	}
	return o.RetainVersions
}

// compact 执行一次整理：锁外做重 IO，提交时短暂持锁。
//
// 两阶段：
//  1. 快照当前片段计划（按文件名字典序），锁外流式合并为
//     frag-0-merged-<tail>.bin（保留版本 0 => 不与写入文件名冲突）；
//  2. 提交时重新持锁校验：计划必须仍是当前可见片段集合的前缀。
//     期间的新写入原样挂回；计划失效（被并发整理改变）则放弃结果。
//
// 任何失败都不改变可见状态；新文件留在磁盘上，由下次启动回收。
func (s *Store) compact(ctx context.Context, table string, budget int64, trigger string) (*CompactionResult, error) {
	// 手动触发需要跨整段过程串行化；自动路径在调用前已持该锁。
	if trigger == "manual" {
		s.mu.Lock()
		ts, err := s.tableLocked(table)
		s.mu.Unlock()
		if err != nil {
			return nil, err
		}
		ts.compactMu.Lock()
		defer ts.compactMu.Unlock()
	}

	// 阶段 1：取计划。
	s.mu.Lock()
	ts, err := s.tableLocked(table)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	beforeV := ts.commitV
	schemaV := ts.schema.Version
	plan := append([]FragmentMeta{}, lastFragments(ts)...)
	tableDir := filepath.Join(s.dir, table)
	s.mu.Unlock()

	if len(plan) < 2 {
		s.log("整理跳过: 表=%s 触发=%s 可见片段数=%d (<2)", table, trigger, len(plan))
		return &CompactionResult{Compacted: false, BeforeVersion: beforeV,
			AfterVersion: beforeV}, nil
	}

	mg := newFragmentMerger(tableDir, plan, schemaV, budget)
	if err := mg.plan(ctx); err != nil {
		s.log("整理失败: 表=%s 触发=%s 阶段=计划 code=%s reason=%s err=%v",
			table, trigger, CodeOf(err), reasonOf(err), err)
		return nil, err
	}
	key := mergedKey(beforeV, plan[len(plan)-1], len(plan))
	outName := fmt.Sprintf("frag-0-%s.bin", sanitize(key))
	outPath := filepath.Join(tableDir, outName)

	out, size, err := mg.write(ctx, outPath, key)
	if err != nil {
		s.log("整理失败: 表=%s 触发=%s 阶段=合并 code=%s reason=%s err=%v",
			table, trigger, CodeOf(err), reasonOf(err), err)
		return nil, err
	}

	// 阶段 2：提交。
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, err = s.tableLocked(table)
	if err != nil {
		s.removeMerged(tableDir, outName)
		return nil, err
	}
	current := lastFragments(ts)
	kept, ok := planPrefix(current, plan)
	if !ok {
		// 计划已被并发整理替换（写入不会删除片段，只会追加）。
		s.removeMerged(tableDir, outName)
		s.log("整理放弃: 表=%s 触发=%s 计划已过期（期间发生过其他整理）", table, trigger)
		return nil, &Error{Code: ErrCodeConflict, Reason: "COMPACTION_PLAN_STALE",
			Op: "compact", Err: fmt.Errorf("compaction plan no longer matches visible fragments")}
	}

	ts.commitV++
	mergedMeta := FragmentMeta{Key: key, SchemaV: ts.schema.Version, NumRows: out.NumRows,
		File: outName, Size: size}
	frags := append([]FragmentMeta{mergedMeta}, kept...)
	replaced := make([]string, len(plan))
	for i, m := range plan {
		replaced[i] = m.Key
	}
	ts.commits = append(ts.commits, &commitRecord{
		version: ts.commitV, schema: ts.schema, fragments: frags, Replaces: replaced,
	})
	if err := s.trimAndPersistLocked(table, ts); err != nil {
		// manifest 未落盘：回滚内存态，merged 文件成为孤儿，启动回收。
		ts.commitV--
		ts.commits = ts.commits[:len(ts.commits)-1]
		s.removeMerged(tableDir, outName)
		return nil, err
	}
	s.log("整理提交: 表=%s 触发=%s 版本 %d->%d 合并 %d 个片段(键=%s) 行数=%d；计划外新片段=%d 个已挂回",
		table, trigger, beforeV, ts.commitV, len(plan), replacedSummary(replaced),
		out.NumRows, len(kept))
	return &CompactionResult{
		Compacted: true, BeforeVersion: beforeV, AfterVersion: ts.commitV,
		MergedFragments: len(plan), Rows: out.NumRows, Bytes: size,
	}, nil
}

// planPrefix 校验 plan 是否为 current 的前缀（即整理期间只有追加写入），
// 返回计划之外需保留的片段。
func planPrefix(current, plan []FragmentMeta) ([]FragmentMeta, bool) {
	if len(current) < len(plan) {
		return nil, false
	}
	for i := range plan {
		if current[i].Key != plan[i].Key || current[i].File != plan[i].File {
			return nil, false
		}
	}
	return append([]FragmentMeta{}, current[len(plan):]...), true
}

func (s *Store) removeMerged(tableDir, name string) {
	if err := removeFile(filepath.Join(tableDir, name)); err != nil {
		s.log("整理清理: 临时合并文件 %s 删除失败: %v（启动时回收）", name, err)
	}
}

func mergedKey(beforeV uint64, tail FragmentMeta, n int) string {
	return fmt.Sprintf("merged-v%d-%s-x%d", beforeV, tail.File, n)
}

func replacedSummary(keys []string) string {
	if len(keys) <= 3 {
		return fmt.Sprintf("%v", keys)
	}
	return fmt.Sprintf("%v...(+%d)", keys[:3], len(keys)-3)
}
