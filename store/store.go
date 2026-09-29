package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

// Store 本地列式存储引擎，提供 MVCC 快照读写、历史版本读取、
// 片段整理（compaction）与按 zone 裁剪的查询。
//
// 并发与快照语义：
//   - 所有写经互斥锁串行化提交，每次提交产生单调递增的提交版本。
//   - Snapshot/SnapshotAt 捕获某一提交版本的不可变视图，之后的提交
//     对旧快照不可见（无脏读、无部分可见）。
//   - 整理在锁外完成重 IO，只在提交瞬间持锁；期间新写入不丢失，
//     提交时计划外的新片段会原样挂回。
//   - 写冲突按原因区分：过期结构版本 / 片段键重叠 / 表已删除。
type Store struct {
	mu     sync.Mutex
	dir    string
	tables map[string]*tableState
	wg     sync.WaitGroup // 自动整理 goroutine
	logf   atomic.Pointer[func(format string, args ...any)]
}

// tableState 单表的提交状态。
type tableState struct {
	schema  *Schema
	commitV uint64          // 单调递增的提交版本
	commits []*commitRecord // 按版本升序（已按保留窗口裁剪）
	dropped bool
	options TableOptions

	// 自动整理：compactCh 容量 1 起到信号合并作用；compactMu 保证
	// 同一表同时只有一个整理在运行。
	compactMu  sync.Mutex
	compactCh  chan struct{}
	stopCh     chan struct{}
	stopClosed bool
}

// commitRecord 一次提交的不可变记录。
type commitRecord struct {
	version   uint64
	schema    *Schema
	fragments []FragmentMeta // 截至该版本的全部可见片段
	// Replaces 非空时表示这是一次整理提交：被合并掉的源片段键。
	Replaces []string
}

// manifest 落盘的表清单（JSON），用于重启后恢复。
type manifest struct {
	Schema         *Schema           `json:"schema"`
	CommitV        uint64            `json:"commit_v"`
	Commits        []manifestCommit  `json:"commits"`
	Dropped        bool              `json:"dropped"`
	RetainVersions int               `json:"retain_versions,omitempty"`
	AutoCompact    *CompactionPolicy `json:"auto_compact,omitempty"`
}

type manifestCommit struct {
	Version   uint64         `json:"version"`
	Schema    *Schema        `json:"schema"`
	Fragments []FragmentMeta `json:"fragments"`
	Replaces  []string       `json:"replaces,omitempty"`
}

// Open 打开（或创建）位于 dir 的存储，并恢复已有表的提交日志。
// 重启时会清理未被任何保留版本引用的片段文件与中断留下的临时文件。
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, tables: map[string]*tableState{}}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		ts, err := loadTable(s, dir, e.Name())
		if err != nil {
			return nil, err
		}
		if ts != nil {
			s.tables[e.Name()] = ts
			startCompactor(s, e.Name())
		}
	}
	return s, nil
}

func loadTable(s *Store, dir, name string) (*tableState, error) {
	mf := filepath.Join(dir, name, "manifest.json")
	data, err := os.ReadFile(mf)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("load manifest %s: %w", mf, err)
	}
	ts := &tableState{schema: m.Schema, commitV: m.CommitV, dropped: m.Dropped,
		options: TableOptions{RetainVersions: m.RetainVersions}}
	if m.AutoCompact != nil {
		ts.options.AutoCompact = *m.AutoCompact
	}
	for _, mc := range m.Commits {
		ts.commits = append(ts.commits, &commitRecord{
			version: mc.Version, schema: mc.Schema,
			fragments: mc.Fragments, Replaces: mc.Replaces,
		})
	}
	if err := s.gcOrphanFilesLocked(name, ts); err != nil {
		return nil, err
	}
	return ts, nil
}

// gcOrphanFilesLocked 删除不被任何保留版本引用的 frag-*.bin，以及
// 整理/写入中断留下的 .tmp/.merge-tmp。只在启动加载时执行：运行期
// 延迟到下次启动删除，以免旧快照正在读取的文件被提前移除。
func (s *Store) gcOrphanFilesLocked(name string, ts *tableState) error {
	referenced := map[string]bool{}
	for _, c := range ts.commits {
		for _, f := range c.fragments {
			referenced[f.File] = true
		}
	}
	tdir := filepath.Join(s.dir, name)
	entries, err := os.ReadDir(tdir)
	if err != nil {
		return err
	}
	removed := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		fn := e.Name()
		if strings.HasSuffix(fn, ".tmp") || strings.HasSuffix(fn, ".merge-tmp") {
			if err := os.Remove(filepath.Join(tdir, fn)); err == nil {
				removed++
			}
			continue
		}
		if strings.HasPrefix(fn, "frag-") && strings.HasSuffix(fn, ".bin") && !referenced[fn] {
			if err := os.Remove(filepath.Join(tdir, fn)); err == nil {
				removed++
			}
		}
	}
	if removed > 0 {
		s.log("启动回收: 表=%s 删除孤儿/临时文件=%d", name, removed)
	}
	return nil
}

// Close 停止自动整理 goroutine（提交均已落盘，无其他待刷状态）。
func (s *Store) Close() error {
	s.mu.Lock()
	for name, ts := range s.tables {
		if !ts.stopClosed {
			close(ts.stopCh)
			ts.stopClosed = true
		}
		_ = name
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

// SetLogger 注入日志钩子（测试用于核对输入、版本关系、窗口判定与失败原因）。
func (s *Store) SetLogger(f func(format string, args ...any)) {
	if f == nil {
		s.logf.Store(nil)
		return
	}
	s.logf.Store(&f)
}

func (s *Store) log(format string, args ...any) {
	if p := s.logf.Load(); p != nil {
		(*p)(format, args...)
	}
}

// CreateTable 建表，初始结构为 schema（Version 忽略，由存储置 1）。
func (s *Store) CreateTable(name string, schema *Schema) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tables[name]; ok {
		return &Error{Code: ErrCodeConflict, Reason: "TABLE_EXISTS", Op: "create-table",
			Err: fmt.Errorf("table %q already exists", name)}
	}
	sc := schema.clone()
	sc.Version = 1
	maxID := 0
	for _, f := range sc.Fields {
		if f.ID > maxID {
			maxID = f.ID
		}
	}
	for i := range sc.Fields {
		if sc.Fields[i].ID == 0 {
			maxID++
			sc.Fields[i].ID = maxID
		}
	}
	ts := &tableState{
		schema:    sc,
		commitV:   1,
		compactCh: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
	}
	ts.commits = append(ts.commits, &commitRecord{version: 1, schema: sc})
	s.tables[name] = ts
	if err := s.persistLocked(name, ts); err != nil {
		delete(s.tables, name)
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	startCompactor(s, name)
	s.mu.Lock()
	return nil
}

// EvolveSchema 对表应用一次结构变更，返回新结构。
func (s *Store) EvolveSchema(name string, baseVersion int, c SchemaChange) (*Schema, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, err := s.tableLocked(name)
	if err != nil {
		return nil, err
	}
	if ts.dropped {
		return nil, conflictErr("evolve-schema", ConflictTableDropped, name)
	}
	if baseVersion != ts.schema.Version {
		return nil, conflictErr("evolve-schema", ConflictSchemaVersion,
			fmt.Sprintf("base=%d current=%d", baseVersion, ts.schema.Version))
	}
	next, err := ts.schema.Apply(c)
	if err != nil {
		return nil, err
	}
	ts.schema = next
	ts.commitV++
	ts.commits = append(ts.commits, &commitRecord{
		version: ts.commitV, schema: next, fragments: lastFragments(ts),
	})
	if err := s.trimAndPersistLocked(name, ts); err != nil {
		return nil, err
	}
	return next, nil
}

// validateFragmentForSchema 校验片段可被当前结构完整解释：
// 每列 ID 必须现存或已删除；现存列的每个非 NULL 值必须与字段类型一致。
// 返回每列权威类型（现存列取结构类型；已删除列按值推断并要求一致）。
func validateFragmentForSchema(ts *tableState, f *Fragment) (map[int]FieldType, error) {
	types := map[int]FieldType{}
	for id, col := range f.Columns {
		if len(col) != f.NumRows {
			return nil, &Error{Code: ErrCodeSchemaIncompatible,
				Reason: fmt.Sprintf("COLUMN_LENGTH_%d", id), Op: "write-fragment",
				Err: fmt.Errorf("column %d length %d != numRows %d", id, len(col), f.NumRows)}
		}
		field := ts.schema.FieldByID(id)
		switch {
		case field != nil:
			for ri, v := range col {
				if v != nil && !valueHasType(v, field.Type) {
					return nil, &Error{Code: ErrCodeSchemaIncompatible,
						Reason: fmt.Sprintf("VALUE_TYPE_MISMATCH:field=%s", field.Name),
						Op:     "write-fragment",
						Err:    fmt.Errorf("row %d value %v (%T) not %s", ri, v, v, field.Type)}
				}
			}
			types[id] = field.Type
		case droppedHas(ts.schema, id):
			// 已删除列：允许保留历史数据，类型按值推断且不得混杂。
			t := FieldType("")
			for _, v := range col {
				if v == nil {
					continue
				}
				it := inferType(v)
				if t == "" {
					t = it
				} else if t != it {
					return nil, &Error{Code: ErrCodeSchemaIncompatible,
						Reason: fmt.Sprintf("MIXED_TYPES_IN_COLUMN_%d", id),
						Op:     "write-fragment",
						Err:    fmt.Errorf("dropped column %d has mixed value types", id)}
				}
			}
			if t != "" {
				types[id] = t
			}
		default:
			return nil, &Error{Code: ErrCodeSchemaIncompatible,
				Reason: fmt.Sprintf("UNKNOWN_FIELD_ID:%d", id),
				Op:     "write-fragment", Err: fmt.Errorf("fragment column not in schema")}
		}
	}
	return types, nil
}

// WriteFragment 基于读取时的结构版本 baseVersion 提交一个片段。
func (s *Store) WriteFragment(name string, baseVersion int, f *Fragment) error {
	s.mu.Lock()
	ts, err := s.tableLocked(name)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if ts.dropped {
		s.mu.Unlock()
		return conflictErr("write-fragment", ConflictTableDropped, name)
	}
	if baseVersion != ts.schema.Version {
		s.mu.Unlock()
		return conflictErr("write-fragment", ConflictSchemaVersion,
			fmt.Sprintf("base=%d current=%d", baseVersion, ts.schema.Version))
	}
	for _, meta := range lastFragments(ts) {
		if meta.Key == f.Key {
			s.mu.Unlock()
			return conflictErr("write-fragment", ConflictFragmentOverlap,
				fmt.Sprintf("key=%q already committed", f.Key))
		}
	}
	colTypes, err := validateFragmentForSchema(ts, f)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	f.SchemaV = ts.schema.Version
	f.ColTypes = colTypes
	f.Zones = buildZones(f, colTypes)

	ts.commitV++
	fileName := fmt.Sprintf("frag-%d-%s.bin", ts.commitV, sanitize(f.Key))
	path := filepath.Join(s.dir, name, fileName)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		s.mu.Unlock()
		return err
	}
	tmp := path + ".tmp"
	w, err := os.Create(tmp)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if err := encodeFragment(w, f); err != nil {
		w.Close()
		os.Remove(tmp)
		s.mu.Unlock()
		return err
	}
	if err := w.Close(); err != nil {
		os.Remove(tmp)
		s.mu.Unlock()
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		s.mu.Unlock()
		return err
	}
	st, err := os.Stat(path)
	if err != nil {
		s.mu.Unlock()
		return err
	}

	meta := FragmentMeta{Key: f.Key, SchemaV: f.SchemaV, NumRows: f.NumRows,
		File: fileName, Size: st.Size()}
	frags := append(append([]FragmentMeta{}, lastFragments(ts)...), meta)
	ts.commits = append(ts.commits, &commitRecord{
		version: ts.commitV, schema: ts.schema, fragments: frags,
	})
	s.log("写入提交: 表=%s 版本=%d 片段=%s 行数=%d 字节=%d 可见片段数=%d",
		name, ts.commitV, fileName, f.NumRows, st.Size(), len(frags))
	if err := s.trimAndPersistLocked(name, ts); err != nil {
		s.mu.Unlock()
		return err
	}
	s.maybeAutoCompactLocked(name)
	s.mu.Unlock()
	return nil
}

// Snapshot 获取表最新提交版本的一致快照。
func (s *Store) Snapshot(name string) (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, err := s.tableLocked(name)
	if err != nil {
		return nil, err
	}
	head := ts.commits[len(ts.commits)-1]
	return s.snapshotOfLocked(name, head), nil
}

// SnapshotAt 读取指定提交版本的一致快照。
//
// 保留窗口内的旧版本可完整回读（包括整理之后：整理提交保留旧版本
// 视图，且其引用的源片段文件在进程生命周期内不会被删除）；
// 超出保留窗口或从不存在的版本返回 VERSION_UNAVAILABLE，
// 绝不悄悄返回最新数据或空结果。
func (s *Store) SnapshotAt(name string, version uint64) (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, err := s.tableLocked(name)
	if err != nil {
		return nil, err
	}
	for _, c := range ts.commits {
		if c.version == version {
			s.log("历史读取: 表=%s 请求版本=%d 命中保留窗口[%d..%d]",
				name, version, ts.commits[0].version, ts.commits[len(ts.commits)-1].version)
			return s.snapshotOfLocked(name, c), nil
		}
	}
	oldest := ts.commits[0].version
	head := ts.commits[len(ts.commits)-1].version
	reason := "NO_SUCH_VERSION"
	if version < oldest {
		reason = "OUT_OF_RETENTION"
	}
	s.log("历史读取: 表=%s 请求版本=%d 不可用 窗口=[%d..%d] 判定=%s",
		name, version, oldest, head, reason)
	return nil, &Error{Code: ErrCodeVersionUnavailable, Reason: reason, Op: "snapshot-at",
		Err: fmt.Errorf("version %d not available (retained window [%d..%d])",
			version, oldest, head)}
}

func (s *Store) snapshotOfLocked(table string, c *commitRecord) *Snapshot {
	return &Snapshot{
		schema:    c.schema.clone(),
		version:   c.version,
		fragments: append([]FragmentMeta{}, c.fragments...),
		store:     s,
		table:     table,
	}
}

// loadFragment 从磁盘读取完整片段（供查询执行使用）。
func (s *Store) loadFragment(table string, meta FragmentMeta) (*Fragment, error) {
	path := filepath.Join(s.dir, table, meta.File)
	f, err := os.Open(path)
	if err != nil {
		return nil, &Error{Code: ErrCodeInterrupted, Op: "read-fragment", Err: err}
	}
	defer f.Close()
	frag, err := decodeFragment(f)
	if err != nil {
		return nil, err
	}
	frag.Key = meta.Key
	frag.SchemaV = meta.SchemaV
	return frag, nil
}

// loadZone 只读片段尾部 zone map；CFR1 等无 zone 的片段返回 (nil,nil)，
// 查询因此回退为完整扫描，绝不误裁。
func (s *Store) loadZone(table string, meta FragmentMeta) (ZoneMap, error) {
	return loadZoneMapFile(filepath.Join(s.dir, table, meta.File))
}

func (s *Store) tableLocked(name string) (*tableState, error) {
	ts, ok := s.tables[name]
	if !ok {
		return nil, &Error{Code: ErrCodeNotFound, Op: "table", Err: fmt.Errorf("table %q not found", name)}
	}
	return ts, nil
}

func (s *Store) persistLocked(name string, ts *tableState) error {
	return s.writeManifestLocked(name, ts)
}

// trimAndPersistLocked 按保留窗口裁剪提交历史并落盘。
func (s *Store) trimAndPersistLocked(name string, ts *tableState) error {
	n := ts.options.RetainVersions
	if n <= 0 {
		n = 1
	}
	if len(ts.commits) > n {
		ts.commits = append([]*commitRecord{}, ts.commits[len(ts.commits)-n:]...)
	}
	return s.writeManifestLocked(name, ts)
}

func (s *Store) writeManifestLocked(name string, ts *tableState) error {
	m := manifest{Schema: ts.schema, CommitV: ts.commitV, Dropped: ts.dropped,
		RetainVersions: ts.options.RetainVersions}
	if ts.options.AutoCompact != (CompactionPolicy{}) {
		pol := ts.options.AutoCompact
		m.AutoCompact = &pol
	}
	for _, c := range ts.commits {
		m.Commits = append(m.Commits, manifestCommit{
			Version: c.version, Schema: c.schema, Fragments: c.fragments,
			Replaces: c.Replaces,
		})
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Join(s.dir, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := filepath.Join(dir, "manifest.json.tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, "manifest.json"))
}

func lastFragments(ts *tableState) []FragmentMeta {
	if len(ts.commits) == 0 {
		return nil
	}
	return ts.commits[len(ts.commits)-1].fragments
}

func droppedHas(s *Schema, id int) bool {
	for _, f := range s.Dropped {
		if f.ID == id {
			return true
		}
	}
	return false
}

func conflictErr(op string, reason ConflictReason, detail any) error {
	return &Error{Code: ErrCodeConflict, Reason: string(reason), Op: op,
		Err: fmt.Errorf("write conflict: %v", detail)}
}

func sanitize(key string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		}
		return '_'
	}, key)
}

// Snapshot 某一提交版本的一致视图。
type Snapshot struct {
	schema    *Schema
	version   uint64
	fragments []FragmentMeta
	store     *Store
	table     string
}

// Schema 返回快照可见的表结构。
func (sn *Snapshot) Schema() *Schema { return sn.schema }

// Version 返回快照对应的提交版本。
func (sn *Snapshot) Version() uint64 { return sn.version }

// Fragments 返回快照可见的片段元信息（副本）。
func (sn *Snapshot) Fragments() []FragmentMeta {
	return append([]FragmentMeta{}, sn.fragments...)
}

// --- 自动整理 ---

func startCompactor(s *Store, name string) {
	s.mu.Lock()
	ts, ok := s.tables[name]
	if !ok {
		s.mu.Unlock()
		return
	}
	if ts.stopCh == nil {
		ts.compactCh = make(chan struct{}, 1)
		ts.stopCh = make(chan struct{})
	}
	ch := ts.compactCh
	stop := ts.stopCh
	s.wg.Add(1)
	s.mu.Unlock()

	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-ch:
				// 自动触发：拿不到整理锁（手动整理在跑）就跳过，
				// 信号已被合并，下一轮写入仍会再次评估。
				s.mu.Lock()
				ts2, ok2 := s.tables[name]
				budget := int64(0)
				if ok2 {
					budget = ts2.options.AutoCompact.MemBudget
				}
				s.mu.Unlock()
				if !ok2 {
					return
				}
				if !ts2.compactMu.TryLock() {
					s.log("自动整理: 表=%s 已有整理运行，跳过", name)
					continue
				}
				res, err := s.compact(context.Background(), name, budget, "auto")
				ts2.compactMu.Unlock()
				switch {
				case err != nil:
					s.log("自动整理: 表=%s 失败 code=%s reason=%v err=%v",
						name, CodeOf(err), reasonOf(err), err)
				case res.Compacted:
					s.log("自动整理: 表=%s 版本 %d->%d 合并片段=%d 行数=%d",
						name, res.BeforeVersion, res.AfterVersion, res.MergedFragments, res.Rows)
				}
			}
		}
	}()
}

func reasonOf(err error) string {
	if e, ok := err.(*Error); ok {
		return e.Reason
	}
	return ""
}

// maybeAutoCompactLocked 在写入后按策略评估并异步触发整理（持锁调用）。
func (s *Store) maybeAutoCompactLocked(name string) {
	ts, ok := s.tables[name]
	if !ok {
		return
	}
	pol := ts.options.AutoCompact
	if pol.MaxFragments <= 0 && pol.MaxTotalBytes <= 0 {
		return
	}
	frags := lastFragments(ts)
	var total int64
	for _, f := range frags {
		total += f.Size
	}
	hit := false
	switch {
	case pol.MaxFragments > 0 && len(frags) >= pol.MaxFragments:
		hit = true
		s.log("自动整理评估: 表=%s 片段数=%d 达到阈值=%d", name, len(frags), pol.MaxFragments)
	case pol.MaxTotalBytes > 0 && total >= pol.MaxTotalBytes:
		hit = true
		s.log("自动整理评估: 表=%s 总字节=%d 达到阈值=%d", name, total, pol.MaxTotalBytes)
	}
	if hit {
		select {
		case ts.compactCh <- struct{}{}:
		default:
		}
	}
}
