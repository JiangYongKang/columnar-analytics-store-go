# columnar-analytics-store-go

本地列式分析存储与查询执行引擎。用本地数据文件模拟表数据的写入与查询，
使并发读写、结构演进、数据整理、历史版本回读以及查询失败时的结果
都可预期且可解释。

## 架构

```
store/
  errors.go       错误分类（ErrCode）、写冲突原因（ConflictReason）、
                  历史版本不可用原因（VERSION_TRIMMED / VERSION_NOT_COMMITTED）
  schema.go       表结构与兼容演进规则
  fragment.go     不可变列式片段的编码/解码（每列 CRC32 校验）
  validation.go   写入片段的结构可解释性校验（列 ID/类型/长度）
  stats.go        片段区段统计（zone map）、按列类型的精确 JSON 落盘/读回、
                  整理内存估算
  prune.go        查询区段裁剪与片段字节预算计费
  store.go        MVCC 提交日志、快照、冲突检测、磁盘持久化、垃圾回收
  history.go      历史版本回读（SnapshotAt）与保留窗口
  compact.go      手动整理：小片段合并、原子提交、中断/重试安全、预算
  autocompact.go  按片段数/体积阈值自动整理（内联或后台）
  query.go        过滤 + 聚合的流式查询执行、行级与片段级内存配额
  options.go      打开选项（历史保留、自动整理阈值、失败回调）
  gc.go           不可达片段清理、崩溃残留回收、整理序号恢复
```

数据布局：`<dir>/<table>/manifest.json`（提交日志，临时文件 + rename 原子更新）+
`<dir>/<table>/frag-<version>-<key>.bin`（写入片段）与
`frag-comp-<seq>.bin`（整理输出）。片段文件一律先写 `.tmp` 再原子改名；
任何未被 manifest 引用的文件在重启时都会被识别为残留并清理。

## 数据整理（compaction）

把多个小片段合并成一个更大的存储单元，减少查询需要打开的文件数。

- **手动触发**：`Store.Compact(ctx, table, &CompactOptions{BudgetBytes})`
  返回 `CompactResult`（整理提交版本、输入片段数、输出文件、行数、是否重试）。
- **自动触发**：`OpenWithOptions(dir, Options{AutoCompact: AutoCompactConfig{
  MinFragments, MinBytes, Synchronous}})`。每次写入提交后检查：
  可见片段数 `>= MinFragments`，或可见片段总字节数 `>= MinBytes` 即触发。
  - `Synchronous=true`：写入返回前内联完成整理（便于确定性测试）；
  - 默认后台 goroutine 执行，写入立即返回；`WaitAutoCompact(ctx)` 等待收尾。
  - 并发写最多触发一个整理（锁内认领），整理进行中再次达阈值会安静跳过，
    失败经 `Options.OnAutoCompactError` 回调，不影响触发它的写入。

### 一致性与并发保证

- **查询结果不变**：整理只是把行重新排列进一个新片段，相同过滤/聚合的
  结果整理前后**完全一致**；整理提交是一次普通 MVCC 提交。
- **新写入不丢、不长时间阻塞**：写入先原子落盘并释放写锁，再触发整理；
  整理期间提交的写入会让本次整理在提交点检测到状态变化，
  **丢弃已生成的输出并整体重试**（`CompactResult.Retried=true`），
  不会重复行、丢失行，也不会出现“合并到一半”的可见数据。
- **中断/崩溃安全**：整理输出在清单更新前对任何快照不可见。
  `ctx` 取消返回 `INTERRUPTED`、预算不足返回 `RESOURCE_EXHAUSTED`、
  片段损坏返回 `CORRUPT_FRAGMENT`——均发生在提交前，存储状态完全不变，
  可立即重试。崩溃留下的 `.tmp` / 未引用输出在重启时被清理。
- 整理输出键使用保留前缀 `$comp-`，用户写入此前缀会得到
  `CONFLICT / FRAGMENT_OVERLAP`。

## 历史版本回读与保留窗口

- `Store.SnapshotAt(table, version)` 返回指定提交版本的不可变快照；
  之后通过同一个 `Execute` 查询，得到的是**该版本当时**的结果。
- `Options.RetainVersions`（建表时生效）或 `Store.SetRetention(table, n)`
  配置**保留最近 N 个已提交版本**；`0`（默认）表示全部保留。
  `OldestVersion` 返回当前窗口内最旧可回读版本。
- **整理不破坏旧版本**：整理产生新片段，但旧片段文件只要仍被窗口内的
  某个版本引用就不会被物理删除；因此保留范围内的旧版本整理后仍可完整回读，
  且严格看不到该版本之后（含整理之后）写入的数据。
- **超窗请求明确失败**：返回 `VERSION_UNAVAILABLE`，
  `Reason=VERSION_TRIMMED`（曾存在但已被裁剪）或
  `VERSION_NOT_COMMITTED`（请求版本晚于最新提交）；
  绝不会悄悄返回最新数据或空结果。

## 区段裁剪（zone-map pruning）

每个片段在提交时按当前结构为各可见列记录 `Min/Max/HasNull` 区段统计，
随 manifest 持久化。查询加载片段前先判断是否可能命中：

- 区段与谓词不相交（如谓词 `id >= 50` 而片段 `Max=30`）→ **跳过整个片段**，
  不打开文件、不计内存；跳过与完整扫描结果**完全一致**。
- `!=`（Ne）不做跳过；边界与列类型不可比较时按“逐行比较必不匹配”处理。
- **结构演进语义**：统计按稳定字段 ID 存取，因此字段改名后裁剪照常正确；
  片段缺列（字段新增前的历史数据）没有统计，按全 NULL 处理，任何比较
  谓词都不命中；列存在但全为 NULL 同样跳过；字段删除后按名查询返回
  `SCHEMA_INCOMPATIBLE`；整理合并产生的片段重新计算统计。

### 重启后的一致性保证

区段裁剪只是优化手段，**任何情况下都不改变查询结果**，重启前后亦然：

- **统计精确落盘**：`ColumnStat` 按列类型自定义 JSON 解码——`int64`
  统计用 `json.Number` 精确解析，不经过 `float64` 舍入。因此大于 2^53
  的整数（十几位、千万亿量级，含 `math.MaxInt64/MinInt64` 边界）写入后
  重启，区段边界与重启前**逐位相同**，跳过判定结论不变。
- **精确混合比较**：行级过滤与区段裁剪共用同一套比较；`int64` 与
  `float64` 的混合比较不做 `float64(x)` 舍入（截断 + 小数部分修正），
  保证大整数参与的比较在行级与片段级得出一致结论。
- **拿不准就不跳过**：只有确实不可能命中的片段才被跳过；缺列/全 NULL/
  类型不可比较等场景按“逐行评估必然不匹配”处理，与逐行执行语义严格对齐。
- **异常不静默**：预算不足返回 `RESOURCE_EXHAUSTED`、片段损坏返回
  `CORRUPT_FRAGMENT`，均明确失败，绝不返回少算或空的结果。

需要重点关注的取值与结构场景（均有重启前后对照测试）：

- 大正数 / 大负数（|v| > 2^53）、`MaxInt64`/`MinInt64`、2^53±1 边界；
- 字段新增之前写入的历史数据（片段缺列，按全 NULL）；
- 显式 NULL 与全 NULL 列；
- 字段改名后按新名过滤旧数据、字段删除后回读剩余列的历史数据。

### 写入必须可被结构解释

写入片段在落盘前校验：列 ID 必须属于当前结构或已删除集合、
可见列取值类型必须与字段类型一致、每列长度必须等于行数。
不满足直接返回 `SCHEMA_INCOMPATIBLE`，从源头杜绝“写入成功、
查询时被静默漏掉或算错”。

## 资源预算

- **查询**：
  - 行级预算 `MemLimiter`（既有）：只有命中行按 `perRowBytes` 计费；
  - 片段字节预算 `Execute(ctx, sn, q, lim, QueryOptions{FragmentBytesBudget})`：
    按**实际扫描**的片段文件字节累计，被裁剪跳过的片段不计费。
    因此表里历史再多，只命中少量数据时也不按整表规模占用资源。
- **整理**：`CompactOptions.BudgetBytes` 覆盖合并输出容器与实际搬运的
  单元格，只统计参与合并的数据；不足时提交前返回 `RESOURCE_EXHAUSTED`。
- 任何预算/中断失败都返回可区分错误，**不返回不完整结果**。

## 数据兼容规则（结构演进）

字段以**稳定 ID** 标识，ID 在表生命周期内唯一且不复用；数据列按 ID 存取：

| 变更 | 语义 | 历史数据 |
|------|------|----------|
| 新增字段 | 分配新 ID | 历史片段该列缺失，读取为 NULL：不匹配任何谓词、不参与数值聚合、区段裁剪跳过 |
| 删除字段 | ID 进入 Dropped 集合，不再复用 | 数据保留在磁盘但不再可见；按名查询返回 `SCHEMA_INCOMPATIBLE` |
| 重命名字段 | 仅改名，ID 与类型不变 | 历史数据按 ID 命中，新名即可读到旧数据，裁剪统计同样按 ID 命中 |

明确拒绝（返回 `SCHEMA_INCOMPATIBLE`）：重名新增、未知类型、
删除/重命名不存在的字段、未知变更类型、写入未知列 ID/值类型不符/列长不符。
字段类型不可变更。

## 并发与快照语义

- 所有写经互斥锁串行化提交，每次提交产生单调递增的提交版本，
  并原子更新 manifest；片段文件同样原子落盘。
- `Snapshot` / `SnapshotAt` 捕获某一提交版本的不可变视图：之后的并发
  提交（含整理）对旧快照不可见，无脏读、无部分可见。
- 写冲突按原因区分：`STALE_SCHEMA_VERSION` / `FRAGMENT_OVERLAP` /
  `TABLE_DROPPED`；整理并发冲突为 `COMPACT_IN_PROGRESS`（手动整理）。

## 错误分类

所有失败归一化为 `*store.Error`，`CodeOf(err)` 提取分类码：

| Code | 触发场景 |
|------|----------|
| `CONFLICT` | 写冲突；手动整理时已有整理在跑（Reason 细分） |
| `SCHEMA_INCOMPATIBLE` | 不兼容结构变更；查询不存在/已删除字段；写入列不可被结构解释 |
| `CORRUPT_FRAGMENT` | 片段 CRC 校验失败、文件截断、魔数错误 |
| `RESOURCE_EXHAUSTED` | 查询/整理内存或字节预算超限 |
| `INTERRUPTED` | 上下文取消或读取中断 |
| `NOT_FOUND` | 表不存在 |
| `VERSION_UNAVAILABLE` | 历史版本超出保留窗口（`VERSION_TRIMMED`）或尚未提交（`VERSION_NOT_COMMITTED`） |

## 本地验证方法

```bash
# 全部测试（含 race 检测）
go test ./... -race -count=1 -v

# 只看“写入后重启再查询”的裁剪一致性用例（日志含输入数据、
# 每个片段的跳过判定与依据、裁剪结果与完整扫描的对比）
go test ./store/ -run 'TestRestartPrune' -v
```

新增/既有测试覆盖：

- `compact_test.go`：整理前后过滤聚合完全一致；整理与写入交错自动重试、
  不重不漏；整理期间结构演进后缺列按 NULL；取消整理无残留可重试；
  整理预算不足无可见变更；模拟崩溃后残留清理、可继续读写；保留键冲突；
  写入类型/列长不符被拒；6 路并发写 × 反复整理的压力校验（race）。
- `history_test.go`：保留窗口内旧版本回读且看不到整理后写入；
  窗口边界（窗口内可读、窗口外 `VERSION_TRIMMED`、未提交版本
  `VERSION_NOT_COMMITTED`）；运行期缩窗立即裁剪并回收文件；
  重启后历史与裁剪结果可复现。
- `prune_test.go`：区段不相交片段被跳过且与完整扫描一致；新增/改名/删除/
  缺列/NULL/整理前后裁剪一致性；裁剪节省片段字节预算、预算不足明确失败；
  按片段数（内联）与体积（后台 `WaitAutoCompact`）自动触发。
- `restart_prune_test.go`：**写入后关闭再重开同一目录**（模拟进程重启），
  校验重启前后裁剪结果与完整扫描完全一致：>2^53 大正数/大负数、
  `MaxInt64`/`MinInt64` 与 2^53±1 边界、缺列（字段新增前的历史数据）、
  显式 NULL、字段改名/删除后的历史数据回读，聚合（count/sum/min/max）
  一并校验；已删除字段上的谓词在重启后仍明确返回 `SCHEMA_INCOMPATIBLE`。
- `store_test.go` / `schema_test.go` / `query_test.go`：快照隔离、
  并发写冲突、并发读写一致性、过期版本拒绝、结构演进语义、
  片段损坏/内存预算/中断错误分类（均确认不回归）。

`go test -v` 日志会打印输入数据、整理前后的版本关系、保留窗口判定、
裁剪跳过原因以及失败归一化结果，例如：

```
整理: 合并 3 个片段 -> frag-comp-1.bin, 提交版本=5, 行数=9
交错: 整理输出已构建(rows=3)但未提交，此时写入 p2
窗口判定: head=6 retain=3 -> 最旧可回读版本=4
裁剪判定: 片段 low 区段[1..3] 与谓词 id=4 op=5 v=50 不相交 => 跳过
错误归一化: 取消整理 -> code="INTERRUPTED"
```
