package store

import "context"

// claimAutoCompact 判断阈值并在锁内认领整理标记。
// 返回 true 表示调用线程“赢得”本次整理并负责执行 compactLoop、清理标记；
// 返回 false 表示未达阈值或已有整理在跑（安静跳过，不算错误）。
func (s *Store) claimAutoCompact(name string) bool {
	cfg := s.options.AutoCompact
	if cfg.MinFragments <= 0 && cfg.MinBytes <= 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, ok := s.tables[name]
	if !ok || ts.dropped || s.compacting[name] {
		return false
	}
	frags := lastFragments(ts)
	n := len(frags)
	var bytes int64
	for _, m := range frags {
		bytes += m.Size
	}
	if !((cfg.MinFragments > 0 && n >= cfg.MinFragments) ||
		(cfg.MinBytes > 0 && bytes >= cfg.MinBytes)) {
		return false
	}
	s.compacting[name] = true
	return true
}

// runAutoCompact 执行一次自动整理；任何错误（含不可重试的冲突）都经
// 回调上报，绝不影响触发它的写入。
func (s *Store) runAutoCompact(name string) {
	defer s.finishCompact(name)
	if _, err := s.compactLoop(context.Background(), name, &CompactOptions{}); err != nil {
		s.reportAutoCompactError(name, err)
	}
}

// maybeScheduleAutoCompact 在一次写入提交后按阈值判断是否触发整理：
//   - Synchronous：在写操作的调用栈内联执行（确定性，主要用于测试）；
//   - 默认：后台 goroutine 执行，写入立即返回，不被整理阻塞。
//
// 认领在锁内完成，因此并发写最多触发一个整理；整理期间的新写入
// 不会丢，也不会被整理卡住，下一次写入仍可再触发。
func (s *Store) maybeScheduleAutoCompact(name string) {
	if !s.claimAutoCompact(name) {
		return
	}
	if s.options.AutoCompact.Synchronous {
		s.runAutoCompact(name)
		return
	}
	s.compactWg.Add(1)
	go func() {
		defer s.compactWg.Done()
		s.runAutoCompact(name)
	}()
}

func (s *Store) reportAutoCompactError(name string, err error) {
	if s.options.OnAutoCompactError != nil {
		s.options.OnAutoCompactError(name, err)
	}
}

// WaitAutoCompact 等待所有已调度的后台自动整理结束。
// ctx 取消时返回 INTERRUPTED（后台整理本身不会被取消）。
func (s *Store) WaitAutoCompact(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.compactWg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return &Error{Code: ErrCodeInterrupted, Op: "wait-auto-compact", Err: ctx.Err()}
	}
}
