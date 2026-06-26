# Python Mini Spark 学习设计

## 目标

构建一个基于 Python 的 Mini Spark 项目，帮助学习 Spark，达到能够使用、调优和排查真实 Spark 作业的水平。本项目不是 Spark 的完整复刻，而是一个学习脚手架，用来把 Spark 的执行模型变得可见、可测试、可解释。

学习路径采用双轨方式：

- Mini Spark 实现：从零实现 Spark 执行模型中的小组件。
- PySpark 实战对照：把每个组件对应到真实 Spark 使用、执行计划、调优和常见问题排查。

## 学习对象

学习者的目标是成为 Spark 实战高手，而不是一开始就成为 Spark 源码贡献者。理解源码思想有价值，但主要成果应该是能够分析 Spark 作业、性能、Shuffle、分区、Join、缓存和故障。

## 语言与技术栈

主语言：Python。

选择 Python 是因为它能让实现保持简洁，把注意力放在核心思想上。必要时，每个模块都会说明真实 Spark 的差异，包括 PySpark 实际上会调用 JVM 上的 Spark 引擎。

Web 可视化作为可选辅助，只在能明显提升理解的地方加入。Web 页面应保持轻量，专注于图示，不把项目变成复杂的前端产品。

## 学习结构

每个阶段包含四类产出：

1. 可运行的 Python 代码。
2. 聚焦的小型测试。
3. 一篇学习笔记，解释设计思路和真实 Spark 对照。
4. 必要时提供一个简单的 Web 可视化页面。

每个阶段最后提供 3 到 5 个思考题。只有当学习者能够解释当前阶段后，才进入下一阶段。

## 阶段计划

### 第一阶段：RDD 基础

实现：

- `SparkContext`
- `RDD`
- `parallelize`
- `collect`

学习：

- RDD 表示什么。
- 为什么 `collect` 是 Action。
- Driver 扮演什么角色。
- 当前实现和真实分布式 Spark 有什么区别。

PySpark 对照：

- `SparkContext.parallelize`
- `RDD.collect`
- 为什么把大数据集 `collect` 到 Driver 很危险。

可视化：

- 简单展示 Driver 到 RDD 再到结果返回的流程。

### 第二阶段：Transformation

实现：

- `map`
- `filter`
- `flat_map`

学习：

- Lazy Evaluation。
- 为什么 Transformation 会返回新的 RDD。
- 为什么直到 Action 被调用前都不会真正执行。

PySpark 对照：

- RDD Transformation。
- 期望 Transformation 立即执行时容易产生的常见误解。

可视化：

- 从源 RDD 到转换后 RDD 的 Lineage 链条。

### 第三阶段：Action

实现：

- `count`
- `first`
- `take`
- `reduce`

学习：

- Action 是执行触发器。
- 最终结果如何产生。
- 为什么 Action 可能很昂贵。

PySpark 对照：

- Driver 端结果收集。
- 查看小样本和收集完整数据的区别。

可视化：

- Action 触发执行的路径。

### 第四阶段：Lineage 与依赖

实现：

- Parent RDD 引用。
- Dependency 元数据。
- Lineage 查看能力。

学习：

- 为什么 Spark 要记录 Lineage。
- Lineage 如何支持重算。
- 为什么不可变 RDD 有助于容错。

PySpark 对照：

- `toDebugString`
- 调试过长的 Lineage 链。

可视化：

- 可交互的 Lineage 图。

### 第五阶段：Partition

实现：

- `Partition`
- 支持分区的 `parallelize`
- 按分区计算。

学习：

- 为什么需要分区。
- Partition 和 Task 的关系。
- 分区数量如何影响并行度。

PySpark 对照：

- `repartition`
- `coalesce`
- 分区大小的基本判断。

可视化：

- 数据集被拆成多个 Partition，并作为 Task 执行。

### 第六阶段：本地 Scheduler

实现：

- `Task`
- `Scheduler`
- 本地 Executor 模拟。

学习：

- Job 提交流程。
- Driver、Scheduler、Task、Executor 的角色。
- 为什么调度要和 RDD API 分离。

PySpark 对照：

- Spark UI 的 Jobs 和 Stages 页面。
- 如何阅读 Task 数量与耗时。

可视化：

- Job 到 Task 的执行时间线。

### 第七阶段：DAG 与 Stage 边界

实现：

- DAG 表示。
- 窄依赖。
- 宽依赖标记。
- Stage 划分规则。

学习：

- 窄依赖与宽依赖。
- 为什么 Shuffle 会产生 Stage 边界。
- Spark 如何把 Lineage 转换成 Stage。

PySpark 对照：

- 阅读 DAG 可视化图。
- 理解为什么一个 Action 可能产生多个 Stage。

可视化：

- 用不同颜色展示 Stage 的 DAG 图。

### 第八阶段：Shuffle

实现：

- Key-Value RDD 基础。
- `group_by_key`
- `reduce_by_key`
- 简化版 Shuffle Write / Shuffle Read。
- 基础 Partitioner。

学习：

- 为什么 Shuffle 昂贵。
- 为什么 `reduceByKey` 通常优于 `groupByKey`。
- Map Side Combine 是什么。

PySpark 对照：

- Shuffle 密集型作业。
- 聚合 API 的选择。
- 数据倾斜的症状。

可视化：

- 记录如何从 Map Partition 移动到 Reduce Partition。

### 第九阶段：Cache

实现：

- `cache`
- `persist` 作为简化别名。
- Cache 命中与未命中记录。

学习：

- 什么时候 Cache 有帮助。
- 为什么 Cache 也可能带来问题。
- Cache 生命周期。

PySpark 对照：

- `cache`
- `persist`
- Spark UI Storage 页面基础。

可视化：

- 第一次 Action 计算并存储，第二次 Action 复用缓存的 Partition。

### 第十阶段：Fault Tolerance

实现：

- 模拟 Partition 丢失。
- 基于 Lineage 重算。

学习：

- 为什么 Spark 不需要急切复制所有数据也能恢复。
- 重算的成本是什么。
- 基于 Lineage 容错的边界。

PySpark 对照：

- Executor 失败。
- Task 丢失。
- 过长 Lineage 的风险。

可视化：

- 丢失的 Partition 如何从父 Lineage 重算。

### 第十一阶段：Spark SQL 桥接

实现：

- 最小表抽象。
- 极简 Logical Plan 对象。
- 将简单 Physical Plan 映射到类 RDD 执行。

学习：

- SQL 到 Logical Plan 再到 Physical Plan。
- 为什么 Spark SQL 比原始 RDD 代码更容易优化。
- Catalyst 的概念层理解。

PySpark 对照：

- DataFrame API。
- `explain`
- Join 策略和 Physical Plan。

可视化：

- SQL 查询如何转换为 Logical Plan 和 Physical Plan。

## Web 可视化范围

Web 页面按阶段选择性使用，只在图示能明显帮助理解时加入。重点展示：

- Lineage 图。
- DAG 与 Stage 边界。
- Partition 到 Task 的映射。
- Shuffle 数据移动。
- Cache 命中与未命中流程。
- SQL Plan 转换。

Web 页面不应发展成完整学习平台、认证系统、复杂仪表盘或大型前端应用。

## 项目结构

建议结构：

```text
mini-spark-learning/
  mini_spark/
    context.py
    rdd.py
    partition.py
    scheduler.py
    shuffle.py
  tests/
  docs/
    01-rdd-basics.md
    02-transformations.md
    ...
  web/
    visualizations/
  examples/
```

实际文件结构可以随着项目演进调整，但每个模块都应保持小而清晰，便于解释。

## 测试策略

测试应在实现变复杂前验证核心行为：

- `parallelize(...).collect()`
- Lazy Transformation 行为。
- Action 结果。
- Lineage 结构。
- Partition 执行。
- Shuffle 分组与聚合。
- Cache 命中与未命中。
- 容错重算模拟。

测试应该短小、可读，因为测试本身也是学习材料。

## 每阶段完成标准

一个阶段完成需要满足：

- 代码可运行。
- 测试通过。
- 学习笔记解释当前模块。
- 包含真实 Spark 对照。
- 写出思考题。
- 学习者能用自己的话解释执行流程。

## 非目标

- 重新实现 Apache Spark。
- 构建生产级分布式计算引擎。
- 匹配 Spark 性能。
- 覆盖所有 Spark API。
- 一开始就学习 Scala 或深入 JVM 内核。
- 在执行模型建立前先构建复杂 Web 应用。

## 第一轮实现切片

从第一阶段开始：

```python
from mini_spark import SparkContext

sc = SparkContext()
rdd = sc.parallelize([1, 2, 3])
assert rdd.collect() == [1, 2, 3]
```

第一阶段应刻意保持很小。它负责引入基础词汇，并为第二阶段的 Lazy Transformation 打基础。
