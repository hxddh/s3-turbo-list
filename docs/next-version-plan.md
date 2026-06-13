# 下一版本规划（v0.9.0 评估）

本文是对当前代码（v0.8.0）与文档的一次系统 review，给出下一版本的取舍建议。
三条主线：**显著提升性能、简化产品、文档清晰**。每项都标注了优先级（P0/P1/P2）、
预估工作量与风险，便于按优先级裁剪。

---

## 0. 现状判断（先把"性能天花板"说清楚）

任何性能规划都要先承认一个已被实测验证的事实（见
`docs/validation-results/oss-list-hints-benchmark-20260611.md` 与
`docs/tuning.md` 的"单桶请求速率上限"一节）：

- **单桶 list 的吞吐最终由 provider 的每桶 `ListObjectsV2` 请求速率决定**，
  不是客户端。OSS 实测 `-c 8` 与 `-c 64` 都停在 ~50K objects/sec。
- 本地 Parquet 输出热路径已达 ~1.5M objects/sec（`list-end-to-end-hot-path-benchmark-20260611.md`），
  在真实 provider 上**不是瓶颈**。

结论：再去优化"客户端编码/分配热路径"的边际收益已经很低。下一版本的性能预算应
投向**尚未触及天花板的场景**：

1. **diff**（当前扁平侧串行，是最大的真实差距）；
2. **把请求预算用在刀刃上**（高并发下的超时/限流会浪费请求，反而降低有效吞吐——
   OSS 测试 `-c 32` 出现 19 次 timeout 且吞吐下降）；
3. **首次运行的启动延迟**（startup discovery 目前是串行 BFS）。

---

## 1. 显著提升性能

### P0 — diff 扁平侧的并行化（已知限制 #1）
**问题**：list 模式有运行时长尾分裂，diff 没有——因为有序 merge-join 需要静态段集合。
所以"单个扁平命名空间的 diff 侧"会退化成串行单链，是当前最大的性能洼地。

**方案**：在 diff 启动阶段、merge 开始之前，对每一侧做一次**静态预分裂**：
- 有结构的侧已由 startup discovery 切分；
- 对**扁平**（无 `/` 结构）的侧，复用 `tasks_s3::probe_flat_cut` 的游标派生切点逻辑，
  在启动时连续探测，生成 N 个静态边界，再交给已有的 `diff_list_side_task`
  并行执行。段集合在运行期保持静态，merge-join 的有序消费与"乱序即响亮失败"保证完全不变。

**收益**：把 diff 扁平侧从 1 段拉到 N 段，预计与 list 的 flat-split 收益同量级
（OSS 上扁平桶 2.3x→打满并发）。
**工作量**：中。复用现成探测逻辑，主要工作在 diff 启动路径与边界对齐断言。
**风险**：低-中。需保证预分裂边界与 `senders` 数量严格对齐（已有 `assert_eq!` 兜底）。

### P1 — 自适应并发 / 限流退避（把请求预算用在刀刃上）
**问题**：固定 `--concurrency` 在限流型 provider 上要么吃不满、要么压垮：高并发触发
503 `SlowDown` 与 5s 操作超时，重试浪费的是同一份稀缺的"每桶请求预算"。

**方案**：基于 `HttpStatusCodeTracker`（已存在）做一个轻量 AIMD 控制器——
正常时缓增在途段数，遇到 503/超时时乘性回退。等价于"自动找到那条
'再加并发也不涨'的拐点"，省去手调 `-c`。
**收益**：在限流型 provider 上提升**有效**吞吐（减少重试浪费），并直接服务"简化"——
用户不再需要手调并发。
**工作量**：中。需要一个反馈回路与谨慎的实测。
**风险**：中。需避免控制器抖动；务必有实测数据支撑，并保留手动 `-c` 覆盖。

### P2 — 并行化 startup discovery
**问题**：startup discovery 是串行 BFS（每层一页探测，最多 3 层），首跑增加 1-2s。
**方案**：同层的 delimiter 探测并发发出。
**收益**：缩短首跑启动延迟；对大量小桶的批处理场景有意义。
**工作量**：小-中。**风险**：低。

> 说明：不建议在本版本追求"单桶 list 更快"的标题数字——它已贴近 provider 天花板，
> 投入产出比低。性能叙事应如实聚焦 diff 与"有效吞吐"。

---

## 2. 简化产品

核心定位是"一个二进制，把 list 和 diff 做到极快"。但当前**对外可见子命令约 16 个**，
其中很大一部分是辅助/帮助类，稀释了核心。

### P0 — 清理"Phase 5"遗留死代码
启动脚手架注释 `Phase 5` 散落各处（仅 `core.rs` 就 9 处），并有真正的死代码：
- `MatchResult::Dup` / `MatchResult::Ignore`：定义了但全仓库无任何构造/匹配（legacy diff 机器的残留，v0.5.0 已删除那套机器）。
- `ObjectFilter::compile`：无调用者的空 stub（注释自己写明"Deferred to Phase 2"）。
- 多处 `#[allow(dead_code)] // Phase 5: ...` 占位。

**动作**：删除未使用变体/ stub，清掉"Phase 5"注释或换成真实说明。
**收益**：减小理解负担，删除会误导的"将来会用"注释。**工作量**：小。**风险**：低（纯删除 + 编译验证）。

### P0 — 修正过时的命名（auto-hints 的影子）
v0.8.0 删了 `auto-hints`/`discover-prefixes` 子命令，但命名残留：
- 模块 `src/auto_hints.rs` 现在装的是**自动 startup discovery**，名字误导——建议重命名为
  `discovery`/`startup_discovery`。
- 全局 flag `--no-auto-hints` 帮助文案仍是
  "Disable auto-hints (forces manual hints or single-threaded)"——术语已过时。
  它现在实际含义是"关闭自动分区（startup discovery + 运行时分裂），退化为单段"。
  建议重命名为 `--no-auto-partition`（或 `--single-segment`），保留旧名为隐藏别名一个版本。

**收益**：名实相符，降低误解。**工作量**：小。**风险**：低（flag 改名做向后兼容别名）。

### P1 — 收敛帮助/引导类子命令
当前并存：`recipes`、`cheatsheet`、`quickstart`、`init-config`、`config-inspect`、`doctor`
——六个"本地帮助"子命令。建议合并为更少的入口（例如把
`recipes`/`cheatsheet`/`quickstart` 统一到单个 `examples`/`help` 下，引导内容尽量回流 README），
保留 `doctor`（预检）与 `init-config`（生成配置）这两个有独立价值的。
**收益**：`--help` 顶层列表显著变短，核心 `list`/`diff` 更突出。
**工作量**：中。**风险**：低-中（行为兼容，可保留旧名一个版本）。

### P2 — 评估显式 hints 工具链的去留
`--hints-file` + `hints-validate` + `hints-merge` 是为"重复盘点"保留的。但自从
startup discovery 会把发现的边界**自动缓存到约定路径**、且运行时分裂已自动处理长尾，
显式 hints 的使用面已很窄。这与项目自身"逐步移除 auto-hints"的轨迹一致。
**建议**：本版本先**度量真实使用/收集反馈**，下一版本再决定是否弃用，不要本版本就删。
**工作量**：小（仅评估）。**风险**：删除属破坏性，需谨慎，故本版本不动。

---

## 3. 文档清晰

### P0 — 修正过时的命令示例
`docs/validation-results/oss-list-hints-benchmark-20260611.md` 的"推荐起步命令"仍在用
**已删除的 `auto-hints` 子命令**，且无删除说明，会误导照抄的用户。
**动作**：加一行历史说明，或改写为当前的"零参数自动分区"用法。
（`bos-s3-compatible-*` 与 `endpoint-validation-plan-*` 也含 auto-hints 字样，但属带日期的历史归档，
加统一的"历史文档"抬头即可，不必逐字改。）
**工作量**：小。**风险**：低。

### P1 — 消除 README 与 tuning.md 的重复，避免漂移
"边界来源优先级"列表在 README 的"Performance and hints"与 `docs/tuning.md` 各有一份，
内容会各自漂移。建议**单一事实源**：tuning.md 保留权威完整版，README 只留一段摘要 + 链接。
**工作量**：小-中。**风险**：低。

### P1 — 配合上面 flag 改名，刷新文档术语
`--no-auto-hints` 改名后，README/tuning/agent-usage 里的相关措辞同步更新；
把"automatic startup discovery vs 运行时分裂 vs（小众的）显式 hints"用**一张精确的优先级表 + 一句话**讲清，
不要在多处重复叙述。
**工作量**：小。**风险**：低。

### P2 — 归档/整理 validation-results
`docs/validation-results/` 有 19 个文件，不少是 v0.2.x 时代的重叠基准，版本号已远落后当前 0.8.0，
形成噪音。建议加一个 `validation-results/README.md` 索引，并标注哪些是"当前有效基准"、
哪些是"历史归档"。
**工作量**：小。**风险**：低（不删数据，只加索引）。

---

## 建议的 v0.9.0 范围（提案）

聚焦少数高价值项，避免又一次"加旋钮"：

| # | 项目 | 主线 | 优先级 |
|---|---|---|---|
| 1 | diff 扁平侧静态预分裂并行化 | 性能 | P0 |
| 2 | 删除 Phase 5 死代码 / 未用枚举变体 / 空 stub | 简化 | P0 |
| 3 | `auto_hints` 模块改名 + `--no-auto-hints` 改名（带兼容别名） | 简化 | P0 |
| 4 | 修正 OSS 基准文档里的 `auto-hints` 过时示例 | 文档 | P0 |
| 5 | 自适应并发 / 限流退避（需实测背书） | 性能 | P1 |
| 6 | 收敛帮助类子命令（recipes/cheatsheet/quickstart） | 简化 | P1 |
| 7 | README 与 tuning.md 去重，单一事实源 | 文档 | P1 |

P2 项（并行化 startup discovery、评估 hints 工具链去留、validation-results 索引）
作为有余力时的补充。

**一句话**：v0.9.0 的性能故事应是"**diff 现在也能打满并发**"，简化故事是"**删而不是加**"
（清死代码、正名、收敛帮助命令），文档故事是"**单一事实源、无过时示例**"。
