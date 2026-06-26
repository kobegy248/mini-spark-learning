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

每篇阶段文档建议使用以下结构：

```text
# 章节标题

## 0. 这一章先学什么，不学什么
## 1. 用人话理解这个概念
## 2. 最小示例：先跑起来
## 3. 逐行解释代码
## 4. Mini Spark 内部发生了什么
## 5. 对照源码：我们写了什么
## 6. 对照真实 Spark：真实世界复杂在哪里
## 7. 亲手实验
## 8. 常见误解
## 9. 本章掌握标准
## 10. 思考题
```

具体要求：

- 开头必须说明“这一章学什么、不学什么”，避免一次塞太多概念。
- 先用人话解释，再给术语。不要一上来堆 RDD、Action、Lineage、DAG 这类词。
- 能类比就类比，例如把 Transformation 类比成“点菜”，把 Action 类比成“上菜”。
- 示例代码必须能直接运行，并给出明确的预期输出。
- 关键代码要逐行解释：这一行创建了什么对象、保存了什么状态、什么时候真正执行。
- 必须解释 Mini Spark 内部状态，例如 `_data`、`_parent`、`_transform` 分别代表什么。
- 必须对照真实 Spark，但要说清楚“当前 Mini Spark 简化了什么”，不要让学习者误以为这就是 Spark 全貌。
- 必须加入“亲手实验”，让学习者能通过运行命令观察现象。
- 必须加入“常见误解”，主动拆掉新手容易误会的点。
- 必须加入“本章掌握标准”，用几句话说明学完后应该能讲清什么。
- 思考题要检验理解，不要只问概念定义。

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
- `mini_spark/rdd.py` → `RDD` 是唯一的核心抽象。一个 RDD 要么是**根 RDD**（持有 `_data`，构造时物化成不可变的 `tuple`，这样源列表后续的修改不会泄漏进来），要么是**派生 RDD**（持有 `_parent` 和一个 `_transform` 可调用对象）。构造函数强制维护这一不变式：根 与 父+transform 二者互斥。
- Transformation（`map`、`filter`、`flat_map`）**不执行计算**——它们返回一个新的派生 RDD，其 `_transform` 是一个产出生成器的 lambda。惰性来自生成器：在 action 执行前不会迭代任何数据。
- 目前唯一的 action 是 `collect()`，它调用 `_compute()`。`_compute()` 沿父链向上走，通过 `self._transform(self._parent._compute())` 把数据经每一步 transform 的生成器拉取过来。这个递归拉取就是整个执行引擎——目前还没有调度器、没有分区、没有 shuffle（那些是后续阶段才有的）。

`计划.txt` 中的每个新阶段都在扩展这个模型（Lineage → Partition → Scheduler → DAG → Shuffle → …）。延续现有风格：保持 `RDD` 不可变，新阶段通过增加字段 / transform 种类来扩展，而不是重写 `_compute`。

## 约定

- 测试命名为 `tests/test_stage_N_<主题>.py`，从 `mini_spark` 导入（`from mini_spark import RDD, SparkContext`）。新增阶段时沿用此命名。
- 文档命名为 `docs/NN-<主题>.md`（阶段号零填充）。各阶段的计划/规格放在 `docs/superpowers/plans/` 和 `docs/superpowers/specs/` 下。
- 全程使用类型注解 + `Generic[T]`/`TypeVar`，保持这一风格。
