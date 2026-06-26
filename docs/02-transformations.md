# 02 - Transformation 与 Lazy Evaluation

## 本次目标

本阶段新增三个 Transformation：

- `map`
- `filter`
- `flat_map`

它们都不会立即执行计算，而是返回一个新的 `RDD`，把“未来要怎么计算”记录下来。真正执行发生在 Action，比如 `collect()`。

## 新增能力

### map

`map(function)` 对 RDD 中的每个元素应用函数，并返回新的 RDD。

```python
rdd = sc.parallelize([1, 2, 3])
mapped = rdd.map(lambda value: value * 10)
print(mapped.collect())
```

输出：

```text
[10, 20, 30]
```

### filter

`filter(function)` 保留满足条件的元素，并返回新的 RDD。

```python
rdd = sc.parallelize([1, 2, 3, 4])
even = rdd.filter(lambda value: value % 2 == 0)
print(even.collect())
```

输出：

```text
[2, 4]
```

### flat_map

`flat_map(function)` 先把一个元素变成多个元素，再把结果拍平。

```python
rdd = sc.parallelize(["ab", "cd"])
chars = rdd.flat_map(lambda text: list(text))
print(chars.collect())
```

输出：

```text
['a', 'b', 'c', 'd']
```

## 调用流程

```text
用户代码
  ↓
SparkContext.parallelize([1, 2, 3])
  ↓
Root RDD
  ↓
.map(lambda value: value * 10)
  ↓
Derived RDD，只记录 parent + transform，不执行
  ↓
.collect()
  ↓
递归计算 parent
  ↓
执行 transform
  ↓
结果返回 Driver
```

## 为什么 Transformation 不立即执行

如果 `map`、`filter` 一调用就执行，Spark 就无法看到完整计算链路。

Lazy Evaluation 的好处是：

- 可以把多个步骤合成一个执行计划。
- 可以等到 Action 出现时再决定如何调度。
- 可以避免不必要的计算。
- 可以基于依赖关系构建 Lineage。

当前 Mini Spark 还没有调度优化，但已经保留了这个核心思想：Transformation 只描述计算，Action 才触发计算。

## 为什么返回新的 RDD

RDD 是不可变的。

`map` 不会修改原来的 RDD，而是返回新的 RDD：

```python
source = sc.parallelize([1, 2, 3])
mapped = source.map(lambda value: value + 1)

print(source.collect())  # [1, 2, 3]
print(mapped.collect())  # [2, 3, 4]
```

这样做的好处是：

- 原始数据不会被意外修改。
- Lineage 可以清楚表达父子关系。
- 后续容错时可以从父 RDD 重新计算。

## 对应真实 Spark 概念

| Mini Spark | 真实 Spark |
| --- | --- |
| `RDD.map` | `RDD.map` |
| `RDD.filter` | `RDD.filter` |
| `RDD.flat_map` | PySpark `RDD.flatMap` |
| parent RDD | RDD dependency |
| transform 函数 | 每个分区上的计算函数 |
| `collect()` 触发 `_compute()` | Action 触发 Job |

## 当前版本的不足

- 还没有 Partition。
- 还没有每个分区独立执行。
- 还没有 Scheduler。
- 还没有 DAG 和 Stage。
- 每次 `collect()` 都会重新计算，没有 Cache。

这些不足会在后续阶段逐步补上。

## PySpark 实战提醒

PySpark 中 Transformation 也是 Lazy 的。下面这段代码不会立刻执行：

```python
rdd2 = rdd.map(lambda value: value * 10)
```

只有遇到 Action 才会触发执行：

```python
rdd2.collect()
```

这解释了一个常见现象：你写了很多 `map`、`filter`，Spark UI 里却暂时看不到 Job。因为 Job 还没有被 Action 触发。

## 思考题

1. 为什么 `map` 不应该直接修改原来的 RDD？
2. 为什么 Transformation 不立即执行有利于 Spark 优化？
3. `map` 和 `flat_map` 的核心区别是什么？
4. 如果同一个 RDD 调用两次 `collect()`，当前 Mini Spark 会发生什么？
5. Lazy Evaluation 和 Lineage 之间有什么关系？
