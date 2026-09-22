# columnar-analytics-store-go

本地列式分析存储与查询执行的最小完整实现。数据落在本地目录的数据文件中，
模拟一张表的**并发写入、一致性快照查询、结构兼容演进**，并对**片段损坏、
读取中断、资源受限**给出可区分、可复现的错误。

无第三方依赖，仅用标准库；模块路径见 `go.mod`。

## 目录中的文件

| 文件 | 职责 |
| --- | --- |
| `types.go` | 逻辑类型、`Value`、`Schema`、`Query`、确定序比较与过滤语义 |
| `schema.go` | 新增 / 删除 / 重命名字段的兼容演进规则 |
| `segment.go` `bits.go` | 列式片段的二进制编码 / 解码、CRC、按字段裁剪读取 |
| `manifest.go` | 已提交状态清单（schema + 片段索引）的原子持久化与崩溃恢复 |
| `store.go` | 存储引擎、快照、写事务、冲突判定、DDL、提交发布 |
| `query.go` | 过滤 / 投影 / 聚合执行、扫描预算与错误区分 |
| `errors.go` | 错误大类、细粒度原因与 `Normalize` 归一化 |

## 数据模型与文件布局

- `segment-NNN.bin`：不可变列式片段。一次提交写入一个片段，块按 **FieldID
  升序**排列，每个字段块含独立 CRC32。片段一旦写入不再修改。
- `manifest-NNN.bin`：某一**已提交代际（generation）**的完整状态：结构版本、
  全部列定义（含已删除列墓碑）、字段 ID 与片段 ID 分配水位、片段索引。
  通过“写临时文件 → fsync → rename → fsync 目录”原子发布。
- 提交顺序：先写片段并 fsync，再原子发布清单，最后用一次原子指针切换更新内存
  状态。清单只保留最近 3 代。

## 并发与快照语义

- 每个已提交状态是不可变的 `engineState`；`Snapshot()` 只是取当前指针，**读不
  加锁、不阻塞写、写也不阻塞读**。
- 快照在其生命周期内固定：拿到快照后发生的提交对该快照**完全不可见**，因此
  读操作只会看到某个完整的已提交状态，不会脏读或部分可见。
- 写事务在 `BeginWrite()` 时记录基准 `generation` 与基准结构副本；提交在互斥
  临界区内做确定性的冲突判定：
  - 基准代际 ≠ 当前代际、且结构版本也推进 → `conflict/schema_modified`
    （写事务进行中发生了 DDL）；
  - 仅代际推进 → `conflict/concurrent_commit`（并发数据写入抢先提交）。
- 被拒绝的事务**不产生任何片段、不改变已提交状态**；并发写只有一个能提交。
- DDL（增删改字段）同样串行化并推进一个代际，所以 DDL 与 DML、DDL 与 DDL
  之间都可被确定性地仲裁。

## 数据兼容（结构演进）规则

字段以稳定的 **FieldID** 标识，片段按 FieldID 存列；重命名不改 FieldID。

- **新增字段**
  - 带非空默认值：历史行读取时取该默认值（在查询侧按 FieldID 缺失回填，
    不重写历史片段）；
  - 不带默认值：必须可空，历史行读取为 `NULL`；
  - 声明 `NOT NULL` 却不提供非空默认值 → 拒绝（`not_null_without_value`），
    以保证历史行仍然合法；
  - 默认值类型与字段类型不一致 → 拒绝（`type_mismatch`）；
  - 名字与任何现存字段的当前名或**历史名**冲突 → 拒绝（`duplicate_column`）。
- **删除字段**：软删除（置墓碑、升版本）。历史片段文件原样保留，只是对查询
  不可见；FieldID 不复用、不重排，杜绝静默错列。重复删除 → `column_not_found`。
- **重命名字段**：FieldID 不变，旧名进入 `PreviousNames`，历史与新数据都以新
  名正确读出。新名被其它字段占用 → 拒绝（`rename_target_exists`）；旧名保留为
  历史名，防止歧义地解析到不同语义的数据。
- 历史行取值因此是**确定且可复现**的：旧片段缺列 ⇒ 该字段默认值或 `NULL`；
  过滤、聚合对历史行也应用同一取值规则。

## 查询执行

- 仅解码 `Filters` / `Projections` / `Aggregates` 涉及的字段块；不需要的块
  通过 `seek` 跳过，不读入内存。因此读取量与内存占用随**实际命中的字段范围**
  变化，而不是随整表列规模无界增长。
- 过滤：等值与比较算子；`NULL` 与任何值比较都不满足（不满足 WHERE）。
- 聚合：`count(*)`、`count/sum/min/max/avg(col)`，列聚合跳过 `NULL`；
  `sum/avg` 要求数值列。
- 任何片段在解码前都会与快照结构交叉校验（FieldID 可解析、类型一致），不一致
  按片段损坏处理，绝不静默错列。
- 任何失败都返回错误且**结果为 nil**，不会给出不完整结果。

## 错误分类

调用方应使用 `Normalize(err)` 得到稳定的 `{Category, Reason}`，并用
`errors.Is` 配合哨兵错误判定。错误文案可能变化，分类常量保持稳定。

| Category | 哨兵 | 典型 Reason |
| --- | --- | --- |
| `conflict` | `ErrConflict` | `concurrent_commit`、`schema_modified` |
| `incompatible_schema` | `ErrIncompatibleSchema` | `duplicate_column`、`column_not_found`、`type_mismatch`、`rename_target_exists`、`not_null_without_value` |
| `corrupt_segment` | `ErrCorruptSegment` | `bad_magic`、`bad_version`、`truncated`、`crc_mismatch`、`bad_header`、`chunk_order`、`duplicate_chunk` |
| `read_interrupted` | `ErrReadInterrupted` | `context_canceled`（context 取消 / 超时） |
| `resource_limit` | `ErrResourceLimit` | `scan_bytes`、`matched_rows` |
| `invalid_query` | `ErrInvalidQuery` | `unknown_column`、`bad_operator`、`type_mismatch`、`bad_aggregate`、`agg_requires_column` 等 |
| `invalid_input` | `ErrInvalidInput` | `unknown_field_id`、`dropped_column`、`type_mismatch`、`null_not_allowed` |
| `io_error` | — | `unknown` / 其它底层 I/O 故障 |

三类“读不出来”的失败被刻意区分：**文件坏了**（corrupt_segment）、
**被调用方打断**（read_interrupted）、**超出预算**（resource_limit）。
`context.Canceled` / `context.DeadlineExceeded` 一律归一为 read_interrupted。

## 资源预算

`Options{MaxScanBytes, MaxMatchedRows}` 在查询开始时固定：

- `MaxScanBytes`：实际读入的编码字节（片段头 + 空值位图 + 命中字段载荷）超过
  预算即 `resource_limit/scan_bytes`，在分配大块内存**之前**中止；
- `MaxMatchedRows`：命中行数超过预算即 `resource_limit/matched_rows`。
- `0` 表示不限。结果里的 `BytesRead / RowsScanned / SegmentsScanned` 可用于
  解释一次查询实际触及的范围。

## 崩溃恢复

- 片段已落盘但清单未发布（模拟“提交前崩溃”）：该片段不在任何清单中，重开时
  作为**孤儿片段被回收**，状态回到提交前。
- 清单原子发布，磁盘上不会出现半个正式清单；重开时取代际最大的清单恢复，
  并校验清单引用的每个片段文件都存在（缺失即按损坏报错，不静默丢数据）。
- 故障注入点见 `Options.Hooks`（`OnSegmentWritten` / `OnCommit` /
  `OnManifestPublished`），供确定性测试使用。

## 本地验证

```bash
go build ./...
go vet ./...
go test ./... -race -count=3 -v      # 全量、竞态、重复 3 次
```

测试覆盖与对应场景：

- `concurrency_test.go`：8 路并发写恰好一个成功、其余按原因拒绝；快照隔离；
  读写并发只观察到 0/100 行两种完整状态；DDL 撞 DML 判为 `schema_modified`；
  多轮重复结论一致。
- `evolution_test.go`：新增带默认值 / 可空列后历史行取值、重命名后新旧数据
  可读、软删除不可见、各类不兼容变更被拒且不推进版本、重开恢复后语义不变。
- `failure_test.go`：magic/截断/CRC 三类损坏可区分；context 取消与超时归一为
  读取中断；字节预算与命中行预算两类资源受限；列裁剪使读取量随命中范围下降；
  “片段写完、清单未发布”崩溃后孤儿被回收。
- `errors_test.go`：归一化确定性、非法查询分类、空表查询。

所有单测都通过 `t.Logf` 打印**输入、冲突判定与错误归一化结果**（搜索日志中的
`INPUT` / `CONFLICT` / `EVOLUTION` / `FAULT` / `BUDGET` / `NORMALIZE`
前缀），用 `-v` 即可查看；重复运行结论保持一致。
