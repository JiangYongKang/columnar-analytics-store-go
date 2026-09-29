package store

import "fmt"

// SnapshotAt 获取指定提交版本的一致快照，用于历史回读。
//   - 版本超出保留窗口（或从未存在）：返回 VERSION_UNAVAILABLE / VERSION_TRIMMED；
//   - 版本晚于当前最新版本：返回 VERSION_UNAVAILABLE / VERSION_NOT_COMMITTED；
//   - 绝不会悄悄回退为最新快照或空结果。
func (s *Store) SnapshotAt(name string, version uint64) (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, err := s.tableLocked(name)
	if err != nil {
		return nil, err
	}
	head := ts.commits[len(ts.commits)-1]
	if version > head.version {
		return nil, &Error{Code: ErrCodeVersionUnavailable, Reason: ReasonVersionNotCommitted,
			Op: "snapshot-at", Err: fmt.Errorf("version %d > head %d", version, head.version)}
	}
	// 找到 <= version 的最大已保留提交（提交版本严格递增）。
	var found *commitRecord
	for i := len(ts.commits) - 1; i >= 0; i-- {
		if ts.commits[i].version <= version {
			found = ts.commits[i]
			break
		}
	}
	if found == nil {
		// 所有保留提交都晚于该版本：该版本已被裁剪出窗口。
		oldest := ts.commits[0].version
		return nil, &Error{Code: ErrCodeVersionUnavailable, Reason: ReasonVersionTrimmed,
			Op:  "snapshot-at",
			Err: fmt.Errorf("version %d older than oldest retained %d", version, oldest)}
	}
	if found.version != version {
		// 请求版本恰好落在两次保留提交之间是不可能的（版本即提交号），
		// 但裁剪后中间版本必然不可用。
		return nil, &Error{Code: ErrCodeVersionUnavailable, Reason: ReasonVersionTrimmed,
			Op: "snapshot-at",
			Err: fmt.Errorf("version %d not retained (oldest=%d head=%d)",
				version, ts.commits[0].version, head.version)}
	}
	return &Snapshot{
		schema:    found.schema.clone(),
		version:   found.version,
		fragments: append([]FragmentMeta{}, found.fragments...),
		store:     s,
		table:     name,
	}, nil
}

// SetRetention 配置表保留的历史已提交版本数；0 表示全部保留。
// 立即按新窗口裁剪提交日志与不再被引用的片段文件。
func (s *Store) SetRetention(table string, retain int) error {
	if retain < 0 {
		return &Error{Code: ErrCodeSchemaIncompatible, Reason: "INVALID_RETENTION",
			Op: "set-retention", Err: fmt.Errorf("retain=%d must be >= 0", retain)}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, err := s.tableLocked(table)
	if err != nil {
		return err
	}
	ts.retain = retain
	s.trimCommitsLocked(ts)
	if err := s.persistLocked(table, ts); err != nil {
		return err
	}
	return s.gcUnlocked(table, ts)
}

// trimCommitsLocked 按保留窗口裁剪提交记录（始终保留最新提交）。
// 裁剪只影响历史回读范围；最新快照的可见片段集合永远不变。
func (s *Store) trimCommitsLocked(ts *tableState) {
	if ts.retain <= 0 || len(ts.commits) <= ts.retain {
		return
	}
	cut := len(ts.commits) - ts.retain
	ts.commits = append([]*commitRecord{}, ts.commits[cut:]...)
}

// Retention 返回表当前保留窗口（0 表示全部保留）。
func (s *Store) Retention(table string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, err := s.tableLocked(table)
	if err != nil {
		return 0, err
	}
	return ts.retain, nil
}

// OldestVersion 返回当前保留窗口内最旧的可回读提交版本。
func (s *Store) OldestVersion(table string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, err := s.tableLocked(table)
	if err != nil {
		return 0, err
	}
	return ts.commits[0].version, nil
}
