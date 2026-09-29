package store

// AutoCompactConfig 自动整理触发阈值；零值表示不自动触发。
type AutoCompactConfig struct {
	// MinFragments 可见片段数达到该值时触发整理。
	MinFragments int
	// MinBytes 可见片段总字节数达到该值时触发整理。
	MinBytes int64
	// Synchronous 为 true 时在触发写入的调用返回前内联执行整理（测试用）；
	// 默认 false 时后台执行，不阻塞写入路径。
	Synchronous bool
}

// Options 打开存储时的选项。
type Options struct {
	// RetainVersions 保留最近 N 个已提交版本用于历史回读；
	// 0 表示全部保留。
	RetainVersions int
	// AutoCompact 自动整理配置。
	AutoCompact AutoCompactConfig
	// OnAutoCompactError 后台自动整理失败时的回调（可为 nil）。
	OnAutoCompactError func(table string, err error)
}

// OpenWithOptions 以指定选项打开（或创建）存储。
func OpenWithOptions(dir string, opt Options) (*Store, error) {
	// 骨架：暂不处理选项，与 Open 行为一致。
	return openStore(dir, opt)
}
