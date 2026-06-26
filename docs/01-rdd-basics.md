# 01 - RDD 基础

## 本次目标

本阶段实现 Mini Spark 的最小可运行版本：

- `SparkContext`
- `RDD`
- `parallelize`
- `collect`

目标不是实现分布式计算，而是先建立 Spark 最核心的调用感觉：

```python
from mini_spark import SparkContext

sc = SparkContext()
rdd = sc.parallelize([1, 2, 3])
print(rdd.collect())
```

输出：

```text
[1, 2, 3]
```

## 新增类

### SparkContext

`SparkContext` 是用户进入 Mini Spark 的入口。

当前只提供一个方法：

- `parallelize(data)`：把本地 Python 可迭代对象包装成一个 `RDD`。

在真实 Spark 中，`SparkContext` 负责连接集群、申请资源、创建 RDD、提交 Job 等。当前版本只保留“创建 RDD”这个最小能力。

### RDD

`RDD` 是弹性分布式数据集的抽象。

当前版本的 `RDD` 只保存一份本地不可变数据：

- 构造时把输入数据转成 `tuple`，避免外部列表变化影响 RDD。
- `collect()` 返回新的 `list`，避免调用方修改返回值后影响 RDD 内部数据。

真实 Spark 中，RDD 不直接保存所有数据，而是保存分区、依赖关系和计算函数。数据通常分布在多个 Executor 上。

## 调用流程

```text
用户代码
  ↓
SparkContext.parallelize([1, 2, 3])
  ↓
创建 RDD
  ↓
RDD.collect()
  ↓
把 RDD 中的数据返回到 Driver
```

当前版本没有 Scheduler、Task、Executor。它是单进程本地模拟。

## 对应真实 Spark 概念

| Mini Spark | 真实 Spark |
| --- | --- |
| `SparkContext` | `SparkContext` |
| `parallelize` | `SparkContext.parallelize` |
| `RDD` | `RDD` |
| `collect` | `RDD.collect` |
| 本地 `tuple` | 分布式 Partition |

## 为什么 collect 是 Action

`collect()` 会把 RDD 的数据真正取出来并返回给 Driver。

在真实 Spark 中，Transformation 只是描述计算，Action 才会触发 Job 执行。虽然当前版本还没有 Lazy Evaluation，但先把 `collect` 定义成 Action，有助于后续引入 `map`、`filter` 和 Lineage。

## 当前版本的不足

- 没有 Lazy Evaluation。
- 没有 Transformation。
- 没有 Partition。
- 没有 Scheduler。
- 没有 Executor。
- 没有分布式执行。

这些不足不是问题，而是后续阶段要逐步补上的学习点。

## PySpark 实战提醒

真实 PySpark 中，`collect()` 会把所有分区的数据拉回 Driver。

如果数据量很大，`collect()` 可能导致：

- Driver 内存溢出。
- 网络传输压力过大。
- 作业运行很慢。

实战中更常用：

- `take(n)` 查看少量样本。
- `show(n)` 查看 DataFrame 样本。
- 写入外部存储，而不是全部拉回本地。

## 思考题

1. 为什么 `SparkContext` 适合作为创建 RDD 的入口？
2. 为什么当前实现要把输入数据转成 `tuple`？
3. 为什么 `collect()` 返回的是新的 `list`，而不是直接返回内部数据？
4. 真实 Spark 中，`collect()` 为什么可能很危险？
5. 当前 Mini Spark 的 `RDD` 和真实 Spark 的 `RDD` 最大区别是什么？
