package colstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

// Options 是打开存储的选项；预算默认不限，测试可显式收紧。
type Options struct {
	Dir            string
	MaxScanBytes   int64 // 查询允许读取的编码字节上限；0 表示不限
	MaxMatchedRows int64 // 行扫描允许命中行数上限；0 表示不限
	Hooks          *FaultHooks
}

// FaultHooks 提供确定性的故障注入点，供损坏/中断/资源受限测试使用。
type FaultHooks struct {
	// OnSegmentWritten 在片段文件 fsync 之后、清单发布之前触发；
	// 返回 error 时模拟“片段写完但提交前崩溃”，事务失败且不发布清单。
	OnSegmentWritten func(segID int64) error
	// OnCommit 可在发布前篡改片段文件模拟落盘损坏（测试中使用）。
	OnCommit func(segID int64)
	// OnManifestPublished 在新代际发布后触发。
	OnManifestPublished func(generation int)
}

// segmentInfo 是已提交片段的索引项（不含数据，数据在磁盘上）。
type segmentInfo struct {
	ID            int64
	SchemaVersion int
	RowCount      int
	FileName      string
}

// engineState 是某个已提交代际的不可变状态；
// 读操作直接持有其指针，因此永远读到某一完整的已提交状态，天然无脏读。
type engineState struct {
	generation    int
	schemaVersion int
	nextFieldID   FieldID
	nextSegmentID int64
	schema        *Schema
	segments      []segmentInfo
}

// Store 是本地列式分析存储。
type Store struct {
	opts       Options
	mu         sync.Mutex // 串行化提交临界区；快照读取完全不持锁
	state      atomic.Pointer[engineState]
	closed     atomic.Bool
	activeTxns atomic.Int64
}

// Open 打开（或初始化）目录下的存储，并重放最新清单恢复状态。
func Open(opts Options) (*Store, error) {
	if opts.Dir == "" {
		return nil, classified(CatInvalidInput, "empty_dir", "open", 0, "Dir must not be empty", nil)
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, classified(CatIO, "mkdir", "open", 0, err.Error(), err)
	}
	s := &Store{opts: opts}

	m, orphanBase, err := loadLatestManifest(opts.Dir)
	if err != nil {
		return nil, err
	}
	var st *engineState
	if m == nil {
		st = &engineState{
			generation:    0,
			schemaVersion: 0,
			nextFieldID:   1,
			nextSegmentID: int64(orphanBase),
			schema:        newSchema(),
			segments:      nil,
		}
	} else {
		st = stateFromManifest(m)
		// 恢复时校验清单中每个片段文件都真实存在，缺失即明确报损坏，
		// 避免把“清单有记录、文件缺失”拖到查询期变成不可解释的 open 错误。
		for _, seg := range st.segments {
			p := segmentFilePath(opts.Dir, seg.ID)
			if _, err := os.Stat(p); err != nil {
				return nil, corruptError(seg.ID, CorruptTruncated,
					"segment referenced by latest manifest is missing on disk", err)
			}
		}
		// 清理任何“不在清单里”的孤儿片段（写段落后崩溃的残留）。
		if err := s.reapOrphans(st); err != nil {
			return nil, err
		}
	}
	s.state.Store(st)
	return s, nil
}

// reapOrphans 删除不属于最新清单的片段文件，保证崩溃恢复后状态可解释。
func (s *Store) reapOrphans(st *engineState) error {
	live := make(map[int64]bool, len(st.segments))
	for _, seg := range st.segments {
		live[seg.ID] = true
	}
	entries, err := os.ReadDir(s.opts.Dir)
	if err != nil {
		return classified(CatIO, "readdir", "open", 0, err.Error(), err)
	}
	for _, e := range entries {
		n := e.Name()
		var id int64
		if _, err := fmt.Sscanf(n, segmentPrefix+"%010d"+segmentSuffix, &id); err == nil {
			if !live[id] {
				_ = os.Remove(filepath.Join(s.opts.Dir, n))
			}
		}
	}
	return nil
}

func (s *Store) current() *engineState { return s.state.Load() }

func (s *Store) Close() error {
	s.closed.Store(true)
	return nil
}

// Snapshot 获取当前已提交状态的一致性快照；无需加锁。
func (s *Store) Snapshot() *Snapshot {
	st := s.current()
	return &Snapshot{st: st, opts: s.opts, store: s}
}

// Snapshot 是某一代际的只读视图，其生命周期内看到的数据固定不变。
type Snapshot struct {
	st    *engineState
	opts  Options
	store *Store
}

func (snap *Snapshot) Generation() int    { return snap.st.generation }
func (snap *Snapshot) SchemaVersion() int { return snap.st.schemaVersion }
func (snap *Snapshot) SegmentCount() int  { return len(snap.st.segments) }

// Schema 返回快照结构的深拷贝，避免调用方修改内部状态。
func (snap *Snapshot) Schema() Schema {
	return *cloneSchema(snap.st.schema)
}

// WriteTxn 是一次写事务；它基于开启时的代际与结构副本做校验，提交时检测并发推进。
type WriteTxn struct {
	store             *Store
	baseGeneration    int
	baseSchemaVersion int
	baseSchema        *Schema // 开启时刻的结构深拷贝，保证校验不被并发 DDL 影响
	rows              []Row
	finished          bool
}

// BeginWrite 基于当前已提交状态开启写事务。
func (s *Store) BeginWrite() *WriteTxn {
	st := s.current()
	s.activeTxns.Add(1)
	return &WriteTxn{
		store:             s,
		baseGeneration:    st.generation,
		baseSchemaVersion: st.schemaVersion,
		baseSchema:        cloneSchema(st.schema),
	}
}

// Add 向事务追加行，并按基准结构做合法性校验。
// 校验规则：所有字段必须在基准结构中存在且未删除；类型必须匹配；
// 未提供的字段取默认值或 NULL（在提交时物化）。
func (tx *WriteTxn) Add(rows ...Row) error {
	if tx.finished {
		return classified(CatInvalidInput, "txn_finished", "add", 0, "transaction already committed or rolled back", nil)
	}
	schema := tx.baseSchema
	for ri, row := range rows {
		for id, v := range row {
			c, ok := schema.columnByID(id)
			if !ok {
				return classified(CatInvalidInput, "unknown_field_id", "add", 0,
					fmt.Sprintf("row %d references unknown/dropped field id %d", ri, id), ErrInvalidInput)
			}
			if c.Dropped {
				return classified(CatInvalidInput, "dropped_column", "add", 0,
					fmt.Sprintf("row %d writes dropped column %q", ri, c.Name), ErrInvalidInput)
			}
			if !v.matchesType(c.Type) {
				return classified(CatInvalidInput, "type_mismatch", "add", 0,
					fmt.Sprintf("row %d column %q: value type mismatch", ri, c.Name), ErrInvalidInput)
			}
			if v.Null && !c.Nullable {
				return classified(CatInvalidInput, "null_not_allowed", "add", 0,
					fmt.Sprintf("row %d column %q is NOT NULL", ri, c.Name), ErrInvalidInput)
			}
		}
	}
	tx.rows = append(tx.rows, rows...)
	return nil
}

// stateAt 与 schemaAtGeneration 已移除：事务持开启时结构副本，提交做代际比较。

// Commit 落盘片段并原子发布新状态。
// 冲突判定是确定性的：
//   - 基准代际 != 当前代际且结构版本也推进 -> schema_modified；
//   - 仅代际推进（并发数据写入）-> concurrent_commit。
func (tx *WriteTxn) Commit(ctx context.Context) (int, error) {
	defer tx.store.activeTxns.Add(-1)
	if tx.finished {
		return 0, classified(CatInvalidInput, "txn_finished", "commit", 0, "transaction already finished", nil)
	}
	tx.finished = true
	if err := ctx.Err(); err != nil {
		return 0, classified(CatReadInterrupted, "context_canceled", "commit", 0, "canceled before commit", err)
	}
	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()

	cur := tx.store.current()
	if cur.generation != tx.baseGeneration {
		reason := ConflictConcurrentCommit
		if cur.schemaVersion != tx.baseSchemaVersion {
			reason = ConflictSchemaModified
		}
		return cur.generation, newConflictError(reason,
			tx.baseGeneration, cur.generation, tx.baseSchemaVersion, cur.schemaVersion)
	}
	if len(tx.rows) == 0 {
		// 空提交不产生代际推进。
		return cur.generation, nil
	}

	next := cloneState(cur)
	segID := next.nextSegmentID
	next.nextSegmentID++
	next.generation++

	chunks, err := buildChunks(next.schema, tx.rows)
	if err != nil {
		return cur.generation, err
	}
	path := segmentFilePath(tx.store.opts.Dir, segID)
	if err := encodeSegment(path, segID, next.schemaVersion, len(tx.rows), chunks); err != nil {
		return cur.generation, classified(CatIO, "encode", "commit", segID, err.Error(), err)
	}
	if h := tx.store.opts.Hooks; h != nil {
		if h.OnSegmentWritten != nil {
			if err := h.OnSegmentWritten(segID); err != nil {
				_ = os.Remove(path)
				return cur.generation, classified(CatIO, "segment_write_hook", "commit", segID, err.Error(), err)
			}
		}
		if h.OnCommit != nil {
			h.OnCommit(segID)
		}
	}

	next.segments = append(next.segments, segmentInfo{
		ID:            segID,
		SchemaVersion: next.schemaVersion,
		RowCount:      len(tx.rows),
		FileName:      filepath.Base(path),
	})
	if err := tx.store.publish(next); err != nil {
		return cur.generation, err
	}
	return next.generation, nil
}

// Rollback 放弃事务；未提交不会落任何清单。
func (tx *WriteTxn) Rollback() {
	if tx.finished {
		return
	}
	tx.finished = true
	tx.store.activeTxns.Add(-1)
	tx.rows = nil
}

func cloneState(st *engineState) *engineState {
	return &engineState{
		generation:    st.generation,
		schemaVersion: st.schemaVersion,
		nextFieldID:   st.nextFieldID,
		nextSegmentID: st.nextSegmentID,
		schema:        cloneSchema(st.schema),
		segments:      append([]segmentInfo(nil), st.segments...),
	}
}

// publish 原子写清单并切换内存状态；顺序保证读者只会看到“旧”或“新”。
func (s *Store) publish(next *engineState) error {
	data := encodeManifest(manifestFromState(next))
	path := manifestPath(s.opts.Dir, next.generation)
	if err := writeAtomic(path, data); err != nil {
		return classified(CatIO, "write_manifest", "commit", 0, err.Error(), err)
	}
	s.state.Store(next)
	s.pruneManifests()
	if h := s.opts.Hooks; h != nil && h.OnManifestPublished != nil {
		h.OnManifestPublished(next.generation)
	}
	return nil
}

// pruneManifests 仅保留最近 3 代清单，删除多余旧文件；最新清单永不删除。
func (s *Store) pruneManifests() {
	entries, err := os.ReadDir(s.opts.Dir)
	if err != nil {
		return
	}
	var names []string
	for _, e := range entries {
		n := e.Name()
		if len(n) >= len(manifestPrefix)+len(manifestSuffix) &&
			n[:len(manifestPrefix)] == manifestPrefix &&
			n[len(n)-len(manifestSuffix):] == manifestSuffix {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	if len(names) > 3 {
		for _, n := range names[:len(names)-3] {
			_ = os.Remove(filepath.Join(s.opts.Dir, n))
		}
	}
}

// buildChunks 按当前结构把行物化为列式块；
// 缺失字段使用默认值（结构演进后新增列对旧片段的历史行语义由读取侧保证）。
func buildChunks(schema *Schema, rows []Row) ([]*chunk, error) {
	live := schema.liveColumns()
	chunks := make([]*chunk, 0, len(live))
	n := len(rows)
	for _, c := range live {
		ck := &chunk{id: c.ID, typ: c.Type, n: n, nulls: make([]uint64, (n+63)/64)}
		switch c.Type {
		case TypeInt64:
			ck.ints = make([]int64, n)
		case TypeFloat64:
			ck.floats = make([]float64, n)
		case TypeBool:
			ck.bools = make([]byte, n)
		case TypeString:
			ck.strs = make([]string, n)
		}
		for i, row := range rows {
			v, present := row[c.ID]
			rv, err := schema.resolveValueForWrite(c, v, present)
			if err != nil {
				return nil, classified(CatInvalidInput, errReason(err), "commit", 0,
					fmt.Sprintf("column %q row %d: %s", c.Name, i, err.Error()), err)
			}
			if rv.Null {
				ck.nulls[i/64] |= 1 << uint(i%64)
				continue
			}
			switch c.Type {
			case TypeInt64:
				ck.ints[i] = rv.Int
			case TypeFloat64:
				ck.floats[i] = rv.Float
			case TypeBool:
				if rv.Bool {
					ck.bools[i] = 1
				}
			case TypeString:
				ck.strs[i] = rv.Str
			}
		}
		chunks = append(chunks, ck)
	}
	return chunks, nil
}

// errReason 尝试从结构变更错误中取细粒度原因，便于输入错误可区分。
func errReason(err error) string {
	var sce *SchemaChangeError
	if errors.As(err, &sce) {
		return string(sce.Reason)
	}
	return "invalid_row"
}

// AddColumn 是结构演进：新增字段（规则见 Schema.addColumn）。
func (s *Store) AddColumn(ctx context.Context, name string, typ ColumnType, opts ColumnAddOptions) (int, error) {
	return s.alterSchema(ctx, func(schema *Schema) (*Schema, FieldID, error) {
		ns, err := schema.addColumn(name, typ, opts, schema.maxIDPlusOne())
		if err != nil {
			return nil, 0, err
		}
		return ns, ns.Columns[len(ns.Columns)-1].ID, nil
	})
}

// DropColumn 软删除字段。
func (s *Store) DropColumn(ctx context.Context, name string) (int, error) {
	return s.alterSchema(ctx, func(schema *Schema) (*Schema, FieldID, error) {
		ns, err := schema.dropColumn(name)
		return ns, 0, err
	})
}

// RenameColumn 重命名字段，FieldID 不变。
func (s *Store) RenameColumn(ctx context.Context, oldName, newName string) (int, error) {
	return s.alterSchema(ctx, func(schema *Schema) (*Schema, FieldID, error) {
		ns, err := schema.renameColumn(oldName, newName)
		return ns, 0, err
	})
}

// alterSchema 串行化结构变更：取最新结构派生新版本并发布新代际。
func (s *Store) alterSchema(ctx context.Context, fn func(*Schema) (*Schema, FieldID, error)) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, classified(CatReadInterrupted, "context_canceled", "alter_schema", 0, "canceled", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return 0, classified(CatIO, "closed", "alter_schema", 0, "store closed", nil)
	}
	cur := s.current()
	ns, newID, err := fn(cur.schema)
	if err != nil {
		return cur.generation, err
	}
	next := cloneState(cur)
	next.schema = ns
	next.schemaVersion = ns.Version
	next.generation++
	if newID >= next.nextFieldID {
		next.nextFieldID = newID + 1
	}
	if err := s.publish(next); err != nil {
		return cur.generation, err
	}
	return next.generation, nil
}

func (s *Schema) maxIDPlusOne() FieldID {
	var max FieldID
	for _, c := range s.Columns {
		if c.ID > max {
			max = c.ID
		}
	}
	return max + 1
}
