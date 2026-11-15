# 原生向量检索优化实现与阶段验收记录

**当前工作区已实际回滚到本轮优化前的冻结基线。** 候选 B 因批量性能回退被拒绝，本轮后续未验收实验代码也已归档并退出活动源码。617 个基线文件哈希及此前 tracked WIP 补丁哈希已逐一核验一致；未使用 git reset/stash 清除原有改动。严格验收工具、原始结果和本文保留，自动优化目标已暂停，避免重新启用被拒绝候选。以下实现与测试信息均为已归档实验的历史记录，不代表当前启用功能。

`V = LAKESOUL_VECTOR_MEMORY_BYTES` 始终是可配置值；512MiB 仅为以下实验设置。普通单查询与原生批量、index-only 与 exact 都必须分别通过无回退验收。

## 可追溯基线与第一版候选

保留的完整源码基线含 617 个文件，SHA256 为 `0908853f9f71c8172092248980f760242597592375e65a5d6162bb339512deb7`。第一版候选含 623 个文件，SHA256 为 `3be11788b5cef25b900cabc94f5226adcbee86f661036214dfff8bb955fae791`。两者均完成相同编译器、`--locked --release`、`-C target-cpu=native` 构建，并检查构建前后源码哈希。

| 产物 | 基线 A | 第一版候选 B |
|---|---|---|
| Python 扩展 SHA256 | `d9e12efac34e626253fa013e4ac62a8b8457a9d28a816db138b5fe97ec2c4cf6` | `b9c5a516111ee6e16c00a0e8fc6567e3d785a126230a6df1dc5e60f22fd02d1e` |
| PG binary SHA256 | `08fbed82c4a43472094cc60c0b0dec918d8aab8c3cc899223ca3b1e4623e97b9` | `bd2529bbf9d8372e3f9277418a907f195abec6206d7247b0e78ccbfec9101004` |

第一版 B 实验包含 native 权威配置匹配后的重复 SET 跳过，以及阶段观测指标。后续新增的物理 pin、范围读、流式 SEG2、资源上下文等实现，不属于该次已构建 B 的性能结论。已安装扩展未被替换，既有报告及原始数据未覆盖。

## SIFT Python index-only：三组交错 A/B

实验使用同一份实际物理索引，保持 7 bits、4 buckets、K=100、nprobe=64、overfetch=1、16 CPU。每个进程从独立 cache 开始，顺序执行前 200 条真实普通 single（包含首条），然后一次调用完整 10,000-query native batch；chunk 百分位另行调用，不替代完整 batch 计时。单查仍只投影 ID。

| Pair | Single QPS A → B | Single QPS 变化 | 热 single P95 A → B (ms) | Full batch QPS A → B | Batch QPS 变化 |
|---|---:|---:|---:|---:|---:|
| 0 | 208.43 → 230.09 | +10.40% | 3.579 → 3.151 | 4527.27 → 4412.02 | −2.55% |
| 1 | 207.73 → 238.15 | +14.64% | 3.490 → 2.986 | 4391.15 → 4266.10 | −2.85% |
| 2 | 214.68 → 231.43 | +7.80% | 3.473 → 3.033 | 4566.33 → 4484.04 | −1.80% |

**验收判定：拒绝该候选。** Single QPS 的配对中位数提高 10.40%，热 single P95 降低 11.96%–14.44%，但完整 batch QPS 的配对中位数下降 2.55%。按用户指定的硬门槛，普通单查询和完整批量查询都必须无性能回退，任何一条路径、配置或已观测配对回退就拒绝候选，收益不能互相抵消，也不再允许 5% 的延迟回退。三次配对的峰值 RSS 增加均不足 1MiB，不能因此抵消吞吐回退。原始结果保留为失败实验，后续候选需重新验证。

single、full batch、独立 chunk 合计 60,600 个查询组、6,060,000 个带名次的 ID 已逐项比较完全一致，artifact SHA256 也已检查。Single recall 均为 0.9747，full batch recall 均为 0.974692。此结果仅覆盖 SIFT/Python/index-only，不外推到 exact、高维数据或其他前端。

完整运行和额外核对证据位于会话私有目录：

- `/tmp/lakesoul-vector-optimization/phase0-noop-sift-results/vector-optimization-ab-v1-20261115T014352Z-af98e4756fa843eb8103f7af3f1b10bc/run-complete.json`
- `/tmp/lakesoul-vector-optimization/phase0-noop-sift-reviewed-results.json`

两个专属表已验证不存在，1,030 个专属对象已删除并核验前缀为空，scratch/cache 已清理。首次未计时的尝试因 catalog 控制面位于 PG 而非 S3 manifest，被校验拒绝；其资源也已先行清理，没有混入上述结果。

## 当前实现与验证范围

- Native SET no-op：逐项转换参数后读取当前 native 配置，保持顺序、SQL interleave 和转换回调语义。16 项实际配置/查询回归通过；测试中 ANN 排名与独立 SQL 同模式结果比较，exact 另对照精确结果。
- 向量专属 raw alias/range store：显式弱别名、不推测 opaque wrapper；沿用同一共享 L2 容量，通用 4MiB 分页策略不变。范围读组件和登记分别通过 12 项测试。
- 受保护 loader：normal store 仍作为 L1 key，目录走已知 raw backend、payload 走范围缓存；expert index SSD 路径避免叠加缓存。相关 IO search 回归 11 项通过，包括 6 项新增 counting 测试。
- SEG2：新增有界 CRC/layout 首遍和最多 32 logical-row 的二遍读取组件，未改变格式、量化和原 full reader。完整 core suite 已通过 120 项，2 项沿用原有忽略；尚未将此组件接入全部超大 cluster/delta 搜索路径。
- 资源上下文：新增单进程可配置 V 根、显式请求交集与共享 backing owner。11 项测试通过，包括 cache-off 正 V、Legacy/V0、失败回滚、输出保活和替换重叠。全入口和全部缓存/codec/mmap 尚未接通，因此目前不宣称整条链路已满足 V 或 RSS 上限。
- 物理 pin：新 vector 专属 admission 原子检查当前 commit 与原始文件退休 fence，5 项 PG 测试通过；权威 current-ID/精确 view 窄查询另有 6 项 PG 测试通过；manager 的 15 项生命周期测试通过。
- 普通查询实际 worker：已在独立 owner 数据库中显式运行 warm pin 复用、force-lazy logical drop/cache invalidation，以及 generic deferred reader 两遍同快照测试，**3 项均通过**。测试确认跨 Session 复用同一服务、缓存不保留 active grant、实际 probe/reader worker 持有 pin，逻辑 drop 后旧查询仍完成并保持结果一致。它们不等同于已经验证物理 GC、续租失败和所有取消场景。

DB 测试使用独立 UUID 数据库，创建前后检查 owner COMMENT，结束后删除。共享测试库存在一张无关表，未运行全库 metadata cleanup。

## 尚待完成的验收

后续工作包括全路径资源票据/admission、PK 首次构建与 mmap 的有界处理、超大 legacy/delta 搜索的流式接入、有界 CPU/I/O 与预取、exact 块级评分及仅最终结果物化。新连接后的产物需要重新冻结源码和构建，完成普通 single/native batch、index-only/exact、各数据集和前端的至少三次交错 A/B，以及 RSS 峰值和增长趋势检查。

高维 flat ex-code/LUT 等改动仍以 profile 证据为前提。最终报告必须同时列出收益和未达标项，不降低 nprobe/bits/K/召回，也不使用并发查询或 batch1 替代普通单查。
