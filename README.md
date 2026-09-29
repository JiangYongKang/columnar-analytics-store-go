# columnar-analytics-store-go

本地列式分析存储与查询执行引擎。用本地数据文件模拟表数据的写入与查询，
使并发读写、结构演进、历史版本读取、片段整理（compaction）、按范围裁剪
以及查询失败时的结果都可预期且可解释。

## 架构

```
store/
  errors.go    错误分类（ErrCode）与写冲突原因（ConflictReason）
  schema.go    表结构与兼容演进规则
  fragment.go  不可变列式片段编解码（列 CRC32 + zone map CRC32；CFR2 格式，兼容旧 CFR1）
  zone.go      片段级 zone map（min/max/nulls）构建、序列化与裁剪判定
  store.go     MVCC 提交日志、快照/历史版本、保留窗口、磁盘持久化与启动回收
  merge.go     流式片段合并（按列逐源处理，内存受预算限制）
  options.go   表选项（历史保留、自动整理策略）与整理两阶段提交
  query.go     过滤 + 聚合的流式查询执行、zone 裁剪与内存配额
  fsutil.go    文件辅助
```

数据布局（每张表一个目录）：

- `manifest.json`：提交日志（临时文件 + rename 原子更新）；
- `frag-<version>-<key>.bin`：普通写入的列式片段；
- `frag-0-merged-v<基准版本>-...bin`：整理产生的合并片段（版本 0 前缀，
  不与写入文件名冲突）。

片段文件均先写 `*.tmp` / `*.merge-tmp` 再原子改名。

## 片段文件格式（CFR2）

```
magic("CFR2") | schemaV | numRows | numCols
| [fieldID | type | crc32 | payloadLen | payload]*   // 每列独立 CRC32
| "ZONE" | zonePayload | crc32(zonePayload)           // 片段级 zone map
| u64be(zoneStart)                                     // zone 区起始偏移
```

zone map 为每列记录 `min/max/nulls` 与列类型；查询先只读文件尾部做
裁剪，zone 区损坏返回 `CORRUPT_FRAGMENT`（`CRC_MISMATCH_ZONE`），
绝不会依据损坏的 zone 错误裁剪。旧的 CFR1 文件（无 zone）继续可读，
查询自动回退为完整扫描，绝不误裁。

## 数据整理（compaction）

把多个小片段合并成一个更大的不可变片段，两种触发方式：

```go
// 手动触发（可用 CompactionOptions{MemBudget: ...} 限制内存）
res, err := store.Compact(ctx, "orders", CompactionOptions{})

// 自动触发：按片段数量或总字节阈值
store.SetTableOptions("orders", TableOptions{
    RetainVersions: 5,
    AutoCompact: CompactionPolicy{
        MaxFragments:  50,        // 可见片段达到 50 个时触发
        MaxTotalBytes: 64 << 20,  // 或总字节达到 64MiB 时触发
        MemBudget:     16 << 20,
    },
})
```

保证：

- **结果不变**：整理只是按列原样拼接行并重建覆盖全部源范围的 zone，
  整理前后相同过滤/聚合查询结果完全一致；
- **并发写入不丢、不阻塞**：重 IO 在锁外完成，只在提交瞬间持锁；
  提交时校验合并计划仍是当前可见片段集合的“前缀”，整理期间的新写入
  原样挂回，不会重复或丢失；
- **失败可恢复**：预算不足（`RESOURCE_EXHAUSTED`）、上下文取消
  （`INTERRUPTED`）、源片段损坏（`CORRUPT_FRAGMENT`）都不会改变可见
  状态；临时文件被清理，未被引用的合并结果在下次启动时回收；
- **合并器按列逐源流式处理**：任意时刻只驻留一个源片段的一列，
  内存占用按“单个源片段”估算而不是待合并数据总规模；只有 0/1 个
  可见片段时手动整理为空操作（`Compacted=false`，不产生新版本）；
- 手动整理对同一表串行（整理中再次调用会等待），自动整理与手动整理
  互不冲突。

## 历史版本与保留窗口

- `Snapshot(name)` 取最新快照；`SnapshotAt(name, version)` 读指定的
  已提交版本；
- `TableOptions.RetainVersions` 配置保留最近 N 个已提交版本
  （0 视为 1）。每次提交后按窗口裁剪；
- **窗口内的旧版本在整理之后仍能完整回读**：整理提交保留旧版本视图
  （仍引用合并前的源片段），且旧版本看不到整理之后才写入的数据。
  源文件在进程生命周期内不会被提前删除，进程重启后窗口内版本同样可读；
- **窗口外请求明确失败**：返回 `VERSION_UNAVAILABLE`
  （`OUT_OF_RETENTION` 或 `NO_SUCH_VERSION`），绝不悄悄返回最新数据
  或空结果。缩小保留窗口会立即让旧版本不可用。

## zone 裁剪查询

`Execute` 在结果与完整扫描完全一致的前提下，先读片段尾部 zone map，
跳过“不可能有任何行满足全部谓词”的片段；`ExecuteStats` 额外返回
跳过/扫描统计。裁剪安全规则：

- 谓词引用的列在片段中**缺列**（新增字段前的历史数据）=> 该片段全部
  行为 NULL，不匹配任何谓词，整体跳过；
- 列**全为 NULL** => 同样不匹配任何比较，整体跳过；
- min/max 与谓词边界不可能相交（如 `v > 100` 而 `max=50`）才跳过；
  边界落在 `[min,max]` 内时保守扫描，宁可多扫绝不漏行（min/max
  无法证明某个具体值不存在）；
- **重命名字段**按稳定字段 ID 命中 zone，历史数据照常裁剪；
- 类型不兼容的谓词（如对字符串列用整数比较）不会误裁；
- **没有谓词的查询不裁剪**；旧格式片段不裁剪。

写入与整理进来的数据都必须能被表结构完整解释：列 ID 必须现存或处于
已删除集合、每个非 NULL 值必须与字段类型一致、列长度必须等于行数，
否则写入阶段直接返回 `SCHEMA_INCOMPATIBLE`（不会“写时成功、查时漏算”）。

### 资源与预算

- 查询逐片段、逐行流式处理，仅命中行计入内存配额
  （`MemLimiter`，每行按 `perRowBytes` 估算）；裁剪使大量不可能命中的
  片段连列数据都不加载，因此“历史再多，只处理少量数据”时资源不随
  整表规模增长；
- 整理按“单个源片段”受 `MemBudget` 约束；
- 预算不足明确返回 `RESOURCE_EXHAUSTED`，绝不返回不完整结果。

## 数据兼容规则（结构演进）

字段以**稳定 ID** 标识，ID 在表生命周期内唯一且不复用；数据列按 ID
存取，因此演进后的取值语义确定且可复现：

| 变更 | 语义 | 历史数据 |
|------|------|----------|
| 新增字段 | 分配新 ID | 历史片段该列缺失，读取为 NULL：不匹配任何谓词、不参与数值聚合，可被安全裁剪 |
| 删除字段 | ID 进入 Dropped 集合，不再复用 | 数据保留在磁盘（整理也保留），但不再可见；按名查询返回 `SCHEMA_INCOMPATIBLE` |
| 重命名字段 | 仅改名，ID 与类型不变 | 历史数据与 zone 均按 ID 命中，新名即可读到/裁剪旧数据 |

明确拒绝（返回 `SCHEMA_INCOMPATIBLE`）：重名新增、未知类型、删除/
重命名不存在的字段、未知变更类型、值类型与字段不符、列长不符、
未知列 ID。字段类型不可变更。

## 并发与快照语义

- 所有写经互斥锁串行化提交，每次提交产生单调递增的提交版本，
  manifest 与片段文件均原子落盘；
- 快照捕获某一提交版本的不可变视图：之后的并发提交对旧快照不可见
  （无脏读、无部分可见）；
- 写冲突按原因区分（`Error.Reason`）：
  - `STALE_SCHEMA_VERSION`：基于过期结构版本提交写入或演进；
  - `FRAGMENT_OVERLAP`：片段键与已提交片段重复；
  - `TABLE_DROPPED`：目标表已删除。

## 错误分类

所有失败归一化为 `*store.Error`，`CodeOf(err)` 提取分类码：

| Code | 触发场景 |
|------|----------|
| `CONFLICT` | 写冲突（Reason 细分上述三种；整理计划过期为 `COMPACTION_PLAN_STALE`） |
| `SCHEMA_INCOMPATIBLE` | 不兼容结构变更；查询不存在/已删除字段；写入数据无法被结构解释；合并时列类型冲突 |
| `CORRUPT_FRAGMENT` | 片段列或 zone 的 CRC 校验失败、文件截断、魔数错误 |
| `RESOURCE_EXHAUSTED` | 查询或整理的内存预算超限 |
| `INTERRUPTED` | 上下文取消或读取中断 |
| `NOT_FOUND` | 表不存在 |
| `VERSION_UNAVAILABLE` | 历史版本超出保留窗口或从未存在 |
| `COMPACTION_BUSY` | 整理冲突（保留分类码） |

## 本地验证方法

```bash
# 全部测试（含 race 检测）
go test ./... -race -count=1 -v
```

测试覆盖：

- `store_test.go`：快照隔离、8 并发写同一键（恰好 1 成功、其余
  `FRAGMENT_OVERLAP`，重复 5 轮结论一致）、并发读写期间每次读都落在
  某个已提交版本、过期版本写入被拒；
- `schema_test.go`：新增/删除/重命名后历史数据语义、不兼容变更拒绝、
  重启后演进历史可复现；
- `query_test.go`：过滤+聚合正确性、片段损坏 `CORRUPT_FRAGMENT`、
  内存预算不足 `RESOURCE_EXHAUSTED`、上下文取消 `INTERRUPTED`、
  三种失败模式错误码可区分；
- `compact_test.go`：手动整理结果一致、整理与 8 路并发写入交错
  （无重复/丢失行）、整理被取消后可继续读写并重试、整理预算受限失败
  不改可见状态、重启后整理结果可复现且孤儿文件被回收、按片段数/字节
  阈值自动整理、删除字段后整理语义不变；
- `history_test.go`：整理前后版本关系、窗口内历史版本（含重启后）
  完整回读且看不到整理后新数据、窗口边界与缩小窗口返回
  `VERSION_UNAVAILABLE`、多轮整理后窗口内旧版本仍可读；
- `prune_test.go`：多种谓词下裁剪结果与逐行参照完全一致且确实跳过
  片段、无谓词不裁剪、新增列缺列/全 NULL/重命名历史数据的裁剪正确性、
  整理前后裁剪结果一致、zone 损坏返回 `CORRUPT_FRAGMENT`、裁剪下预算
  只随命中行数增长、写入无法被结构解释时直接拒绝。

`SetLogger(func(format string, args ...any))` 可注入日志钩子；
`go test -v` 会打印输入数据、写入/整理前后的版本关系、保留窗口判定、
裁剪统计与失败原因，例如：

```
写入提交: 表=t 版本=5 片段=frag-5-p4.bin 行数=1 字节=... 可见片段数=4
自动整理评估: 表=t 片段数=4 达到阈值=4
整理提交: 表=t 触发=auto 版本 5->6 合并 4 个片段(键=[p0 p1 p2]...(+1)) 行数=...；计划外新片段=0 个已挂回
历史读取: 表=t 请求版本=5 命中保留窗口[5..7]
历史读取: 表=t 请求版本=2 不可用 窗口=[5..7] 判定=OUT_OF_RETENTION
新增列裁剪: 跳过=1 扫描=1 count=2
zone 损坏: code="CORRUPT_FRAGMENT" reason=CRC_MISMATCH_ZONE
```
