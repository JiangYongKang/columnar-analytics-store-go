# columnar-analytics-store-go

本地列式分析存储与查询执行引擎。用本地数据文件模拟表数据的写入与查询，
使并发读写、结构演进以及查询失败时的结果都可预期且可解释。

## 架构

```
store/
  errors.go    错误分类（ErrCode）与写冲突原因（ConflictReason）
  schema.go    表结构与兼容演进规则
  fragment.go  不可变列式数据片段的编码/解码（每列 CRC32 校验）
  store.go     MVCC 提交日志、快照、冲突检测、磁盘持久化
  query.go     过滤 + 聚合的流式查询执行与内存配额
```

数据布局：`<dir>/<table>/manifest.json`（提交日志）+
`<dir>/<table>/frag-<version>-<key>.bin`（列式片段，先写临时文件再原子改名）。

## 数据兼容规则（结构演进）

字段以**稳定 ID** 标识，ID 在表生命周期内唯一且不复用；数据列按 ID 存取，
因此演进后的取值语义确定且可复现：

| 变更 | 语义 | 历史数据 |
|------|------|----------|
| 新增字段 | 分配新 ID | 历史片段该列缺失，读取为 NULL：不匹配任何谓词、不参与数值聚合 |
| 删除字段 | ID 进入 Dropped 集合，不再复用 | 数据保留在磁盘但不再可见；按名查询返回 `SCHEMA_INCOMPATIBLE` |
| 重命名字段 | 仅改名，ID 与类型不变 | 历史数据按 ID 命中，新名即可读到旧数据 |

明确拒绝（返回 `SCHEMA_INCOMPATIBLE`，不静默错列/丢字段）：
重名新增、未知类型、删除/重命名不存在的字段、未知变更类型。
字段类型不可变更（无此变更操作）。

## 并发与快照语义

- 所有写经互斥锁串行化提交，每次提交产生单调递增的提交版本，
  并原子更新 manifest（临时文件 + rename），片段文件同样原子落盘。
- `Snapshot` 捕获某一提交版本的不可变视图：之后的并发提交对旧快照不可见，
  读不会看到部分提交的中间状态（无脏读、无部分可见）。
- 相互冲突的写被拒绝，并按原因区分（`Error.Reason`）：
  - `STALE_SCHEMA_VERSION`：基于过期结构版本提交写入或演进；
  - `FRAGMENT_OVERLAP`：片段键与已提交片段重复；
  - `TABLE_DROPPED`：目标表已删除。

## 错误分类

所有失败归一化为 `*store.Error`，`CodeOf(err)` 提取分类码：

| Code | 触发场景 |
|------|----------|
| `CONFLICT` | 写冲突（Reason 细分上述三种） |
| `SCHEMA_INCOMPATIBLE` | 不兼容结构变更；查询不存在/已删除字段 |
| `CORRUPT_FRAGMENT` | 片段 CRC 校验失败、文件截断、魔数错误 |
| `RESOURCE_EXHAUSTED` | 查询内存配额超限 |
| `INTERRUPTED` | 上下文取消或读取中断 |
| `NOT_FOUND` | 表不存在 |

查询执行逐片段、逐行流式处理，仅命中行计入内存配额
（`MemLimiter`，每行按 `perRowBytes` 估算），因此内存占用与命中
数据范围相关，不随整表规模无界增长；任何失败都返回上述可区分错误，
不会静默给出不完整结果。

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
- `query_test.go`：过滤+聚合正确性、片段损坏返回 `CORRUPT_FRAGMENT`、
  内存预算不足返回 `RESOURCE_EXHAUSTED` 且预算随命中行数变化、
  上下文取消返回 `INTERRUPTED`、三种失败模式错误码可区分。

单测日志（`go test -v`）会打印输入、冲突判定与错误归一化结果，例如：

```
输入: 8 个并发写同一键 -> 成功=1 键重叠冲突=7 其他=0
冲突判定: 基于旧版本写入 -> code="CONFLICT" (STALE_SCHEMA_VERSION)
错误归一化: 损坏片段 -> code="CORRUPT_FRAGMENT" (CRC_MISMATCH_COLUMN_1)
```
