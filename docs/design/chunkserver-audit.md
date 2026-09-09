当前 chunkserver 实现审查（2026-09-09）

审查基线为 `3e481443fc170233e0312ed5f3603cbe6fc75024`，重点是已实现的 engine 与 node 的数据保留、失败重试和恢复语义。以下发现针对实际实现，不把设计文档中尚未实现的网络、迁移、checkpoint 或调度能力视为已有保证。本次不是完整的并发内存模型或掉电一致性证明。

**本次修改**

- 移除 `ReclaimPolicy`、Cache 淘汰分支、`FLAG_ACCESSED`、访问位更新及专用条件删除方法；`reclaim(queue)` 只执行保留有效 chunk 的 GC，`pick_victim()` 按有效字节数、年龄选择 sealed segment。
- 有效 chunk 校验失败时，GC 返回 `Corrupt`，保留索引和原 segment，不再删除记录。移除表示这种隐式删除的 `ReclaimReport::corrupt`。
- sealed segment 在已知数据结束位置之前遇到无效 batch header 时，GC 返回 `Corrupt`，不再将其当作扫描结束并释放 segment。
- 更新接口调用、测试、README 和设计文档。以上修改不改变磁盘格式，但改变 reclaim 的 Rust API。

GC 的两种损坏场景分别有回归测试覆盖报错、保留原数据位置、修复字节后的成功重试。另有未访问对象经 GC 和重启仍然可读的测试，以及已有随机模型、并发读写和回收测试。

**仍需修复的实现问题**

1. **P1：恢复将不可读 segment 当成空闲，并可能恢复旧版本。**

   `engine.rs::open` 对 segment header 的读取失败、校验失败或身份不符统一计数后跳过；`SegmentTable::new` 的初始状态却是 Free，`Writer::new` 会把这些 segment 加入空闲队列。复现：写入并 seal，破坏其 header，重新 open 成功；chunk 消失，后续新写入复用该 segment。另一个复现中，旧版本在较早的 segment，新版本在损坏 header 的 segment，恢复后旧 LSN 重新成为当前版本。

   建议：正常 open 对未知 segment 状态返回错误；显式 salvage 模式必须隔离该空间，不能直接复用。单纯隔离空间仍不足以防止旧版本恢复，因为不可读区可能包含更新或 tombstone；需要阻止不完整恢复结果作为正常服务视图发布。

   位置：[恢复 header](../../core/moat-engine/src/engine.rs)、[segment 初始状态](../../core/moat-engine/src/segments.rs)、[writer 空闲队列](../../core/moat-engine/src/writer.rs)。

2. **P1：sealed segment 的 footer 回退扫描会截断已确认的数据。**

   footer 校验失败后，`open` 使用与 active segment 相同的 `scan_segment`；任何 record 校验失败都会结束扫描，随后在该位置写入新 footer 并 seal。复现：同一 segment 中两个分别完成写入的 chunk，破坏 footer 和第一个 value，open 成功但完整的第二个 chunk 消失；再次 open 不再报告坏 footer 或重新扫描。

   建议：区分 active 尾部恢复与 sealed 完整性检查。对于后者，至少要求扫描完整覆盖 header 记录的数据范围；不能将中途损坏认定为未确认尾部。先返回错误并保留证据，再设计显式 salvage。active segment 的介质损坏与未完成尾写也不能无条件等同。

   位置：[open / scan_segment / seal_segment_blocking](../../core/moat-engine/src/engine.rs)。

3. **P1：失败的删除重试返回 Missing，重启后对象重新出现。**

   `Writer::delete` 在 tombstone 写入完成之前修改共享索引，使读者也立即看见 Missing；失败路径调用 `untrack` 清理状态，但不恢复旧索引。复现：写入并 seal，提交 delete，对其写入注入 EIO，等待结果失败；恢复设备后重试 delete 返回 Missing；重启后原 chunk 仍然存在。

   建议：分离 writer 的 pending 操作视图和读者的已完成视图，在 tombstone 完成时发布删除；失败时保留可重试状态。同 key 多次 put/delete 的顺序、GC 对 pending tombstone 的依赖需要一起处理，不能只补一次索引回滚。

   位置：[delete / fail_batch / untrack / apply_record](../../core/moat-engine/src/writer.rs)。

4. **P1：重复 PUT 的 Exists 混淆 pending 和已完成状态。**

   `exists` 会查询 `unapplied`，所以第一个 PUT 仍在 pending batch 时，第二个同 ID PUT 已返回不带 ticket 的 `Exists`。复现中，此时共享索引尚无该 chunk；让第一个写入失败之后，最终也没有 chunk。调用者不能把这个 `Exists` 当作可确认的幂等成功。

   建议：仅对已完成的现存 chunk 返回 Exists；pending 重复写应返回可等待的依赖 ticket、共享最终结果或明确的 pending 状态。它与失败删除属于同一组操作状态机问题，适合一起修复。

   位置：[put / put_large / exists](../../core/moat-engine/src/writer.rs)。

5. **P1：重复磁盘 UUID 被接受，导致多盘 placement 退化。**

   `FormatOptions::default` 使用全零 UUID，format 原样写入；`Node::open` 和 `Placement::new` 未验证唯一性。相同 UUID 产生相同 seed，相同容量下所有 key 都选择列表中的同一个磁盘；改变磁盘列表顺序还会改变对应的物理盘。复现使用两个默认 UUID、相同容量的设备，1000 个 key 全部落在同一盘。

   建议：Node 构建 placement 前拒绝重复 UUID；format 要求显式唯一身份，或由明确的格式化入口生成并持久化身份。禁止在每次 open 时重新生成身份，否则重启会改变映射。

   位置：[FormatOptions](../../core/moat-engine/src/options.rs)、[format](../../core/moat-engine/src/engine.rs)、[Node::open](../../core/moat-server/src/node.rs)、[Placement](../../core/moat-server/src/placement.rs)。

以上五项用六个独立的 MemDevice 复现用例验证了当前错误行为；它们尚未在本次修改中修复。优先处理恢复的两个问题，再统一处理 pending mutation 和重复请求，最后收紧多盘身份检查。

**需要明确的已有约束**

`verify_reads = false` 是现有显式性能选择，当前网络层尚未提供端到端校验，因此不能同时宣称默认读取绝不会返回损坏数据。`sync_on_flush` 只在显式 barrier 上安排同步，PUT 的单次 completion 不等于无 PLP 设备上的掉电持久化；GC 搬迁与 Free header 的持久化顺序也需要在非 PLP 支持中单独证明。这些需要确定支持契约，本次未擅自改变配置默认值，也未进行硬件掉电验证。

单盘独占 writer、opaque ChunkId、chunk 范围读取、小对象 batch、物理 GC 和明确请求的覆盖写都属于 chunkserver 的合理职责，没有因 foyer 的需求而移除。
