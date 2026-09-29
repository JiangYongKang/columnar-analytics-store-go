package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Store 本地列式存储引擎，提供 MVCC 快照读写。
//
// 并发与快照语义：
//   - 所有写经互斥锁串行化提交，每次提交产生单调递增的提交版本。
//   - Snapshot 捕获某一提交版本的不可变视图，之后的提交对旧快照不可见，
//     因此读不会看到部分提交的中间状态（无脏读）。
//   - 写冲突按原因区分：过期结构版本 / 片段键重叠 / 表已删除。
type Store struct {
	mu         sync.Mutex
	dir        string
	tables     map[string]*tableState
	options    Options
	seq        uint64 // 整理输出文件名的单调序号（重启后按可见文件名恢复）
	compactWg  sync.WaitGroup
	compacting map[string]bool
}

// tableState 单表的提交状态。
type tableState struct {
	schema  *Schema
	commitV uint64          // 单调递增的提交版本
	commits []*commitRecord // 按版本升序
	dropped bool
	retain  int // 保留最近 N 个已提交版本；0 表示全部保留
}

// Options 返回存储打开时的选项。
func (s *Store) Options() Options { return s.options }

// commitRecord 一次提交的不可变记录。
type commitRecord struct {
	version   uint64
	schema    *Schema
	fragments []FragmentMeta // 截至该版本的全部可见片段
	kind      string         // "write" | "evolve" | "compact"
}

const (
	commitKindWrite   = "write"
	commitKindEvolve  = "evolve"
	commitKindCompact = "compact"
)

// manifest 落盘的表清单（JSON），用于重启后恢复。
type manifest struct {
	Schema  *Schema          `json:"schema"`
	CommitV uint64           `json:"commit_v"`
	Commits []manifestCommit `json:"commits"`
	Dropped bool             `json:"dropped"`
	Retain  int              `json:"retain,omitempty"`
	Seq     uint64           `json:"seq,omitempty"`
}

type manifestCommit struct {
	Version   uint64         `json:"version"`
	Schema    *Schema        `json:"schema"`
	Fragments []FragmentMeta `json:"fragments"`
	Kind      string         `json:"kind,omitempty"` // "write" | "evolve" | "compact"
}

// Open 打开（或创建）位于 dir 的存储，并恢复已有表的提交日志。
func Open(dir string) (*Store, error) {
	return OpenWithOptions(dir, Options{})
}

func openStore(dir string, opt Options) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, tables: map[string]*tableState{}, options: opt, compacting: map[string]bool{}}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		ts, err := loadTable(dir, e.Name())
		if err != nil {
			return nil, err
		}
		if ts != nil {
			s.tables[e.Name()] = ts
			if err := s.removeOrphanFilesLocked(e.Name(), ts); err != nil {
				return nil, err
			}
			if m := maxCompactSeq(ts); m > s.seq {
				s.seq = m
			}
		}
	}
	return s, nil
}

func loadTable(dir, name string) (*tableState, error) {
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
	ts := &tableState{schema: m.Schema, commitV: m.CommitV, dropped: m.Dropped, retain: m.Retain}
	for _, mc := range m.Commits {
		ts.commits = append(ts.commits, &commitRecord{
			version: mc.Version, schema: mc.Schema, fragments: mc.Fragments, kind: mc.Kind,
		})
	}
	return ts, nil
}

// loadFragment 从磁盘读取完整片段（供查询执行使用）。

// Close 等待在途后台整理结束后关闭存储。所有提交本就已原子落盘，
// 即使不等待直接退出也不会产生半可见状态；等待仅为优雅停机。
func (s *Store) Close() error {
	s.compactWg.Wait()
	return nil
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
	// 建表时若字段未分配 ID，按顺序分配。
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
	ts := &tableState{schema: sc, commitV: 1, retain: s.options.RetainVersions}
	ts.commits = append(ts.commits, &commitRecord{version: 1, schema: sc, kind: "create"})
	s.tables[name] = ts
	return s.persistLocked(name, ts)
}

// EvolveSchema 对表应用一次结构变更，返回新结构。
// 基于过期版本提交返回 ConflictSchemaVersion；变更不兼容返回
// ErrCodeSchemaIncompatible。
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
		version: ts.commitV, schema: next, fragments: lastFragments(ts), kind: commitKindEvolve,
	})
	s.trimCommitsLocked(ts)
	if err := s.persistLocked(name, ts); err != nil {
		return nil, err
	}
	if err := s.gcUnlocked(name, ts); err != nil {
		return nil, err
	}
	return next, nil
}

// WriteFragment 基于读取时的结构版本 baseVersion 提交一个片段。
// 冲突时返回 *Error（Code=ErrCodeConflict，Reason 区分原因）。
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
	if isReservedKey(f.Key) {
		s.mu.Unlock()
		return conflictErr("write-fragment", ConflictFragmentOverlap,
			fmt.Sprintf("key=%q uses reserved prefix %q", f.Key, reservedKeyPrefix))
	}
	for _, meta := range lastFragments(ts) {
		if meta.Key == f.Key {
			s.mu.Unlock()
			return conflictErr("write-fragment", ConflictFragmentOverlap,
				fmt.Sprintf("key=%q already committed", f.Key))
		}
	}
	// 校验片段可被当前结构完整解释：列 ID 必须存在于当前结构或已删除集合；
	// 可见列取值类型必须与结构一致；列长度必须等于行数。
	// 不满足时写入即失败，避免“写入成功、查询时静默漏掉或算错”。
	if err := validateFragmentForSchema(f, ts.schema); err != nil {
		s.mu.Unlock()
		return err
	}

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
		s.mu.Unlock()
		return err
	}
	if err := os.Rename(tmp, path); err != nil { // 原子替换，避免部分可见文件
		s.mu.Unlock()
		return err
	}
	size := fileSize(path)

	f.SchemaV = ts.schema.Version
	meta := FragmentMeta{
		Key: f.Key, SchemaV: f.SchemaV, NumRows: f.NumRows, File: fileName,
		Size: size, Stats: computeColumnStats(f, ts.schema),
	}
	frags := append(append([]FragmentMeta{}, lastFragments(ts)...), meta)
	ts.commits = append(ts.commits, &commitRecord{
		version: ts.commitV, schema: ts.schema, fragments: frags, kind: commitKindWrite,
	})
	s.trimCommitsLocked(ts)
	if err := s.persistLocked(name, ts); err != nil {
		s.mu.Unlock()
		return err
	}
	// 清单已更新：回收不再被保留窗口引用的旧片段文件。
	if err := s.gcUnlocked(name, ts); err != nil {
		s.mu.Unlock()
		return err
	}
	// 写入已原子落盘，先释放写锁再触发整理：同步自动整理也不会
	// 长时间阻塞写入路径，后台整理期间新的写入照常进行。
	s.mu.Unlock()
	s.maybeScheduleAutoCompact(name)
	return nil
}

// Snapshot 获取表的一致快照；之后并发提交不影响该快照视图。
func (s *Store) Snapshot(name string) (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, err := s.tableLocked(name)
	if err != nil {
		return nil, err
	}
	head := ts.commits[len(ts.commits)-1]
	return &Snapshot{
		schema:    head.schema.clone(),
		version:   head.version,
		fragments: append([]FragmentMeta{}, head.fragments...),
		store:     s,
		table:     name,
	}, nil
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

func (s *Store) tableLocked(name string) (*tableState, error) {
	ts, ok := s.tables[name]
	if !ok {
		return nil, &Error{Code: ErrCodeNotFound, Op: "table", Err: fmt.Errorf("table %q not found", name)}
	}
	return ts, nil
}

func (s *Store) persistLocked(name string, ts *tableState) error {
	m := manifest{Schema: ts.schema, CommitV: ts.commitV, Dropped: ts.dropped,
		Retain: ts.retain, Seq: s.seq}
	for _, c := range ts.commits {
		m.Commits = append(m.Commits, manifestCommit{
			Version: c.version, Schema: c.schema, Fragments: c.fragments, Kind: c.kind,
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
