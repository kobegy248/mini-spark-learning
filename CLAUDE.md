# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目目标

Mini Spark 是一个**学习项目**：用 Python 从零实现一个迷你版 Apache Spark，每次只增加一个能力，目的是理解 Spark 的执行模型，而不是停留在 API 使用层面。完整路线和规则见 `计划.txt`。当前进度：第一阶段（RDD 基础）和第二阶段（Transformation）已完成；第三到十一阶段（Action、Lineage、Partition、Scheduler、DAG、Shuffle、Cache、容错、Spark SQL）尚未开始。

## 工作原则（来自 计划.txt，必须遵守）

- **一次只实现一个能力**。禁止一次生成完整 Mini Spark 或捆绑多个功能。每一步只新增最小能力，并保持项目始终可运行、可测试。
- **每个模块必须配套**：`docs/` 下的学习笔记、`tests/` 下的测试，以及一段说明——对应真实 Spark 的哪个类、Spark 为什么实现得更复杂、Spark 做了哪些工程优化、当前实现有哪些不足。最后附 3~5 个思考题帮助学习者验证理解。
- **学习者理解后才能进入下一阶段**。要解释设计理由和备选方案，而不是只给代码。
- 笔记用中文、教程式风格写作，必须面向 Spark 小白，参考 `docs/01-rdd-basics.md` 和 `docs/02-transformations.md` 的语气和格式。

## 学习文档编写准则

学习文档不是“知识点摘要”，而是“小白跟学教程”。写每一章时，必须放慢解释速度，先建立直觉，再讲代码，再对照真实 Spark。

文档还必须体现“设计推导”。不要只写“Spark 有这个东西，所以 Mini Spark 也实现一个”。要回答：为什么需要这个设计？如果不用它会怎样？朴素方案哪里不行？Spark 的选择解决了什么问题，又付出了什么代价？

每篇阶段文档建议使用以下结构：

```text
# 章节标题

## 0. 这一章先学什么，不学什么
## 1. 用人话理解这个概念
## 2. 如果没有这个设计，会遇到什么问题
## 3. 最朴素的方案是什么
## 4. 朴素方案的问题是什么
## 5. Spark 为什么这样设计
## 6. 这个设计的收益和代价
## 7. Mini Spark 当前如何简化实现
## 8. 最小示例：先跑起来
## 9. 逐行解释代码
## 10. Mini Spark 内部发生了什么
## 11. 对照真实 Spark：真实世界复杂在哪里
## 12. 亲手实验
## 13. 常见误解
## 14. 本章掌握标准
## 15. 思考题
```

具体要求：

- 开头必须说明“这一章学什么、不学什么”，避免一次塞太多概念。
- 先用人话解释，再给术语。不要一上来堆 RDD、Action、Lineage、DAG 这类词。
- 必须加入设计推导：从“没有这个设计会怎样”开始，讲朴素方案、朴素方案的问题、Spark 的设计选择、收益和代价。
- 必须体现思考过程，而不是只模仿 Spark API 或源码结构。
- 能类比就类比，例如把 Transformation 类比成“点菜”，把 Action 类比成“上菜”。
- 示例代码必须能直接运行，并给出明确的预期输出。
- 关键代码要逐行解释：这一行创建了什么对象、保存了什么状态、什么时候真正执行。
- 必须解释 Mini Spark 内部状态，并区分“本阶段教学模型”和“最终代码模型”。例如早期阶段可以用 `_data`、`_parent`、`_transform` 帮助理解，但当前最终代码已经演进为 `_partitions`、`_parent`、`_transform`、`_wide_transform`、`_dependency_kind`、缓存状态和丢失分区状态。
- 必须对照真实 Spark，但要说清楚“当前 Mini Spark 简化了什么”，不要让学习者误以为这就是 Spark 全貌。
- 必须加入“亲手实验”，让学习者能通过运行命令观察现象。
- 必须加入“常见误解”，主动拆掉新手容易误会的点。
- 必须加入“本章掌握标准”，用几句话说明学完后应该能讲清什么。
- 思考题要检验理解，不要只问概念定义。
- 学习内容默认使用中文，包括阶段文档、计划说明、可视化页面文案和示例解释。
- 如果某个概念涉及执行流程、数据流动、父子依赖、DAG、Stage、Shuffle、Cache 命中或 SQL Plan 转换，应优先考虑提供 HTML 可视化。可视化不追求复杂炫技，优先做成“点击下一步”的学习页，让学习者按自己的节奏观察每个细节。
- HTML 可视化放在 `docs/visualizations/` 下，并在对应阶段文档中链接。页面应尽量不依赖外部网络或复杂前端框架，直接打开即可学习。

## 常用命令

测试用 pytest（已装在本地 `.venv` 中）。在仓库根目录执行：

```bash
.venv/Scripts/python.exe -m pytest            # 运行全部测试
.venv/Scripts/python.exe -m pytest tests/test_stage_2_transformations.py   # 运行单个文件
.venv/Scripts/python.exe -m pytest tests/test_stage_2_transformations.py::test_map_is_lazy_until_action_runs   # 运行单个测试
```

没有构建步骤、没有配置 linter、也没有 `src/` 布局——`mini_spark` 作为顶层包直接导入（在仓库根目录运行即可）。

## 架构

执行模型是 **RDD 的惰性血缘（lazy lineage）**，对应 Spark 的核心思想：

- `mini_spark/context.py` → `SparkContext.parallelize(data)` 是入口，返回一个根 `RDD`。
- `mini_spark/rdd.py` → `RDD` 是唯一的核心抽象。一个 RDD 要么是**根 RDD**（持有 `_partitions`，由 `parallelize(data, num_slices=...)` 把源数据切成不可变分区），要么是**派生 RDD**（持有 `_parent`，再持有窄依赖 `_transform` 或宽依赖 `_wide_transform` 之一）。构造函数维护这些不变式：根 RDD 和派生 RDD 互斥，窄转换和宽转换互斥。
- Transformation（`map`、`filter`、`flat_map`、`group_by_key`、`reduce_by_key`）**不执行计算**——它们返回新的派生 RDD，只记录父 RDD、操作名、依赖类型和计算函数。窄依赖逐分区处理父分区，宽依赖会拉取上游多个分区并重新分组。
- Action 中 `collect()` 和 `count()` 会创建 `LocalScheduler`，由 scheduler 把 RDD 的分区变成 Task 并交给本地 Executor 顺序执行；`first()`、`take()`、`reduce()` 仍直接通过 `_compute()` 拉取数据，用于展示惰性求值和提前停止。
- `_compute()` 现在只是把 `_compute_partitions()` 的分区结果串起来。真正的核心路径是 `_compute_partitions()`：先处理 cache 命中和丢失分区恢复，再根据根 RDD、窄依赖、宽依赖分别计算分区。
- 后续文档如果需要讲早期阶段的 `_data` / `_transform` 简化模型，必须明确写成“第二阶段教学模型”或“到本阶段为止”，不要写成“当前最终代码就是这样”。

`计划.txt` 中的每个新阶段都在扩展这个模型（Lineage → Partition → Scheduler → DAG → Shuffle → …）。延续现有风格：保持 `RDD` 不可变，新阶段通过增加字段 / transform 种类来扩展，而不是重写 `_compute`。

## 约定

- 测试命名为 `tests/test_stage_N_<主题>.py`，从 `mini_spark` 导入（`from mini_spark import RDD, SparkContext`）。新增阶段时沿用此命名。
- 文档命名为 `docs/NN-<主题>.md`（阶段号零填充）。各阶段的计划/规格放在 `docs/superpowers/plans/` 和 `docs/superpowers/specs/` 下。
- 全程使用类型注解 + `Generic[T]`/`TypeVar`，保持这一风格。
