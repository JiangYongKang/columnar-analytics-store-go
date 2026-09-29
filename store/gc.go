package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// gcUnlocked 删除表目录下不再被任何保留提交引用的 frag-*.bin 文件。
// 必须在清单已持久化之后调用：即使删除前进程崩溃，重启时孤儿清理会
// 再次扫描；引用中的文件永远不会被删除。
func (s *Store) gcUnlocked(name string, ts *tableState) error {
	referenced := map[string]bool{}
	for _, c := range ts.commits {
		for _, m := range c.fragments {
			referenced[m.File] = true
		}
	}
	dir := filepath.Join(s.dir, name)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		fn := e.Name()
		if !strings.HasPrefix(fn, "frag-") || !strings.HasSuffix(fn, ".bin") {
			continue
		}
		if referenced[fn] {
			continue
		}
		if err := os.Remove(filepath.Join(dir, fn)); err != nil && !os.IsNotExist(err) {
			return &Error{Code: ErrCodeInterrupted, Op: "gc",
				Err: fmt.Errorf("remove unreferenced fragment %s: %w", fn, err)}
		}
	}
	return nil
}

// removeOrphanFilesLocked 启动恢复时清理：
//   - 任何 .tmp 文件（写入/提交中断的残留，从未进入清单）；
//   - 不被保留提交引用的 frag-*.bin（整理输出已落盘但提交未完成）。
//
// 由于清单中记录的文件集合即“可见集合”，清理这些文件不会影响
// 任何可回读版本，也不会产生重复/丢失行。
func (s *Store) removeOrphanFilesLocked(name string, ts *tableState) error {
	dir := filepath.Join(s.dir, name)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	referenced := map[string]bool{}
	for _, c := range ts.commits {
		for _, m := range c.fragments {
			referenced[m.File] = true
		}
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		fn := e.Name()
		if strings.HasSuffix(fn, ".tmp") {
			if err := os.Remove(filepath.Join(dir, fn)); err != nil && !os.IsNotExist(err) {
				return err
			}
			continue
		}
		if strings.HasPrefix(fn, "frag-") && strings.HasSuffix(fn, ".bin") && !referenced[fn] {
			if err := os.Remove(filepath.Join(dir, fn)); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

// maxCompactSeq 从所有保留提交引用的整理输出文件名 frag-<v>-<key>.bin
// 中恢复最大序号。整理文件的键形如 $comp-seq-<n>，sanitize 后 '$' 变 '_'，
// 因此磁盘名为 frag-<v>-_comp-seq-<n>.bin。
func maxCompactSeq(ts *tableState) uint64 {
	var maxV uint64
	seen := map[string]bool{}
	for _, c := range ts.commits {
		for _, m := range c.fragments {
			if seen[m.File] {
				continue
			}
			seen[m.File] = true
			if !strings.HasPrefix(m.Key, reservedKeyPrefix) {
				continue
			}
			if n, ok := parseCompactSeq(m.Key); ok && n > maxV {
				maxV = n
			}
		}
	}
	return maxV
}

func parseCompactSeq(key string) (uint64, bool) {
	const p = reservedKeyPrefix + "seq-"
	if !strings.HasPrefix(key, p) {
		return 0, false
	}
	n, err := strconv.ParseUint(key[len(p):], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// nextCompactSeqLocked 分配下一个整理序号（调用方持锁）。
func (s *Store) nextCompactSeqLocked() uint64 {
	s.seq++
	return s.seq
}
