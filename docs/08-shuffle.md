# 08 - Shuffle：为什么跨分区重组数据很贵

## 0. 这一章先学什么，不学什么

这一章学习 Shuffle。

前面我们已经知道 Partition 是并行计算的基础，窄依赖让每个 Partition 自己算自己的。现在遇到一个新问题：

> 如果同一个 key 的数据分散在不同 Partition，怎么把它们聚到一起？

答案是：Shuffle。

本章新增：

- `group_by_key`
- `reduce_by_key`

可视化页面：

[Shuffle 可视化](visualizations/08-shuffle.html)

## 1. 先从最朴素的方案开始

假设我们要按 key 聚合：

```text
Partition 0: ("a", 1), ("b", 2)
Partition 1: ("a", 3), ("b", 4)
```

想把 "a" 的值合在一起、"b" 的值合在一起。

朴素方案会怎么做？既然每个 Partition 能独立算，那就**每个 Partition 自己先聚合**：

```text
Partition 0 自己聚合 → ("a", 1), ("b", 2)
Partition 1 自己聚合 → ("a", 3), ("b", 4)
```

这能并行，看起来挺合理。

## 2. 朴素方案会遇到什么问题

### 问题 1：同一个 key 还散落在不同分区

朴素方案只做了“分区内聚合”，但 "a" 仍然同时出现在 Partition 0 和 Partition 1。要得到 "a" 的最终结果，必须把两边的 "a" 再合一次——而这一步**必须跨 Partition 看数据**，没法再“各算各的”了。

### 问题 2：不知道该把数据送到哪

要让所有 "a" 聚到一起，就得把 Partition 0 的 "a" 和 Partition 1 的 "a" 送到同一个目的地。但谁来决定目的地？朴素方案没有这个机制。

### 问题 3：分区内聚合的“部分结果”无法直接拼成最终结果

哪怕每个分区先 reduce 出 ("a", 1) 和 ("a", 3)，也得有一步把它们再 reduce 成 ("a", 4)。这一步本质上是**全局重组**，绕不开。

## 3. Spark 为什么需要 Shuffle

只要操作要求“相同 key 的数据聚到一起”（`group_by_key`、`reduce_by_key`、`join`、`distinct` 等），就必然要跨 Partition 移动数据。这个跨 Partition 重新分发数据的过程，就是 **Shuffle**。

Shuffle 的本质是：**打破窄依赖“子分区只看父分区对应一份”的假设，让一个子分区依赖所有父分区**。这就是上一章说的“宽依赖”。

Spark 必须显式做 Shuffle，因为：

- 要决定每个 key 去哪个目标 Partition（用 `hash(key) % num_partitions` 之类）。
- 要把数据真正从上游 Partition 搬到下游 Partition。
- 要在边界处停下来（切 Stage），上游全写完下游才能读。

## 4. 这个设计的核心思想

Shuffle 把“按 key 聚合”拆成两阶段：

```text
阶段 1（map side，上游各分区并行）：
  每个上游分区把自己的数据按 key 分好类，写到对应的“目标桶”里。

阶段 2（reduce side，下游各分区并行）：
  每个下游分区从所有上游桶里取回属于自己的那部分，再做聚合。
```

决定 key 去哪用的是分区函数：

```python
@staticmethod
def _partition_for_key(key, num_partitions: int) -> int:
    return hash(key) % num_partitions
```

同一个 key 永远落到同一个目标 Partition，于是相同 key 的数据就被聚到了一起。

### 一个生活类比：重新分拣快递

原来快递按发货顺序堆在不同 Partition：

```text
Partition 0: ("a", 1), ("b", 2)
Partition 1: ("a", 3), ("b", 4)
```

现在要按收件人（key）重新分拣，让同一个收件人的包裹去同一个分拣筐：

```text
"a" 的包裹 → 筐 0
"b" 的包裹 → 筐 1
```

这个跨筐搬运就是 Shuffle。搬完之后，每个筐里的包裹就是同一个 key 的全部数据，可以放心聚合了。

## 5. 这个设计解决了什么问题

- **让按 key 的全局聚合成为可能**：相同 key 必然聚到同一目标分区。
- **保留了并行性**：上游各分区可以并行写，下游各分区可以并行读 + 聚合。
- **提供了 Stage 边界**：Shuffle 天然是宽依赖，把 DAG 切成多个 Stage。

## 6. 这个设计付出了什么代价

Shuffle 是 Spark 里**最贵的操作**，代价很大：

- **网络传输**：数据要跨 Partition（真实集群里是跨机器）搬运。
- **磁盘 IO**：真实 Spark 会把 shuffle 数据写盘（防止内存不够），下游再读盘。
- **排序/序列化**：数据要序列化、可能要排序、可能要 spill。
- **切断 pipeline**：Shuffle 处必须物化，上游写完下游才能读，不能再融合成一次遍历。
- **重算更贵**：宽依赖下重算一个下游分区，可能要把多个上游分区重算一遍。

所以工程上有一条经验：**能不 Shuffle 就不 Shuffle，必须 Shuffle 就尽量减少数据量**。

## 7. Mini Spark 当前如何实现

本阶段给 RDD 新增了宽依赖 Transformation。

窄依赖的 transform 是：

```text
一个父 Partition -> 一个子 Partition（一对一）
```

Shuffle 的宽依赖 transform 是：

```text
所有父 Partition -> 重新分发到多个子 Partition
```

`group_by_key` 的 `wide_transform` 大致是：

```text
1. 把所有父 Partition 的数据拉到一起，按 key 收集 value 列表。
2. 准备 target_partitions 个空桶。
3. 对每个 key，用 hash(key) % num_partitions 算出目标桶，把 (key, value_list) 放进去。
4. 每个桶就是一个新的子 Partition。
```

`reduce_by_key` 类似，但第 1 步不是收集成列表，而是**边读边合并**：

```text
对每个 key 维护一个累加值，
每来一个 value 就用 function 合并一次。
```

两者都把 `dependency_kind` 设成 `"wide"`，于是上一章的 Stage 划分会在它们这里切一刀。

## 8. group_by_key 与 reduce_by_key

### group_by_key

```python
from mini_spark import SparkContext

sc = SparkContext()
rdd = sc.parallelize(
    [("a", 1), ("b", 2), ("a", 3), ("b", 4)],
    num_slices=2,
)

print(sorted(rdd.group_by_key(num_partitions=2).collect()))
```

输出：

```text
[('a', [1, 3]), ('b', [2, 4])]
```

`group_by_key` 会把相同 key 的所有 value 收集成列表。

### reduce_by_key

```python
print(sorted(rdd.reduce_by_key(lambda left, right: left + right).collect()))
```

输出：

```text
[('a', 4), ('b', 6)]
```

`reduce_by_key` 会把相同 key 的 value 合并成一个结果。

## 9. 为什么 reduce_by_key 通常比 group_by_key 好

`group_by_key` 会保留所有 value：

```text
("a", [1, 3, 5, 7, 9])
```

如果 value 很多，这个列表会很大，Shuffle 要搬运的数据量也大。

`reduce_by_key` 会边读边合并：

```text
1 + 3 + 5 + 7 + 9 = 25
```

每个 key 只保留一个累加结果，Shuffle 数据量大幅减少。

真实 Spark 中，`reduceByKey` 还可以做 **map-side combine**（上游写 shuffle 之前先在本地预聚合），进一步减少跨网络搬运的数据量。而 `groupByKey` 没法做 map-side combine，因为它必须把所有 value 原样搬过去才能组成列表。

所以经验是：**只要能用 `reduce_by_key`，就别用 `group_by_key`**。

## 10. Mini Spark 内部发生了什么

以 `reduce_by_key(lambda a,b: a+b)` 为例：

```text
上游 Partition 0: ("a",1),("b",2)
上游 Partition 1: ("a",3),("b",4)
  ↓ wide_transform 拉取所有上游数据
遍历全部 (key,value)：
  "a" → 累加 1，再 3 → 4
  "b" → 累加 2，再 4 → 6
  ↓ 按 hash(key) % num_partitions 分桶
目标桶里得到 ("a",4) 和 ("b",6)
  ↓
collect() 收集 → [('a',4),('b',6)]
```

注意：Mini Spark 的 Shuffle 是**纯内存**的——把所有上游数据拉到一个地方一次性分桶。真实 Spark 远比这复杂（写盘、排序、网络、spill）。

## 11. 对照真实 Spark：真实世界复杂在哪里

| Mini Spark | 真实 Spark |
| --- | --- |
| `group_by_key` | `groupByKey` |
| `reduce_by_key` | `reduceByKey` |
| `dependency_kind = "wide"` | `ShuffleDependency`（宽依赖） |
| 纯内存、一次性分桶 | Shuffle Write（写盘/内存）+ Shuffle Read（跨节点拉取） |
| 没有 map-side combine | `reduceByKey` 有 map-side combine，`groupByKey` 没有 |
| 没有 spill | 数据超内存时 spill 到磁盘，按 key 排序归并 |
| hash 分桶 | 支持 hash / sort / range 多种 shuffle 方式 |

真实 Spark 的 Shuffle 很复杂，涉及磁盘、网络、排序、聚合、内存管理、数据序列化和失败重试。它往往是 Spark 作业的性能瓶颈，调优的大部分精力都花在减少 Shuffle 上。

Mini Spark 的简化点：纯内存、无网络、无 spill、无 map-side combine（`reduce_by_key` 虽然边读边合并，但那是 reduce-side 的合并，不是真正的 map-side combine）。

## 12. 亲手实验

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc=SparkContext(); rdd=sc.parallelize([('a',1),('b',2),('a',3),('b',4)], num_slices=2); print(sorted(rdd.group_by_key().collect())); print(sorted(rdd.reduce_by_key(lambda a,b:a+b).collect()))"
```

观察宽依赖如何切开 Stage：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; from mini_spark.dag import ExecutionDAG; sc=SparkContext(); rdd=sc.parallelize([('a',1),('b',2)], num_slices=2).reduce_by_key(lambda a,b:a+b); dag=ExecutionDAG.from_rdd(rdd); print(dag.stages())"
```

预期输出：

```text
[Stage(id=0, operations=['parallelize']), Stage(id=1, operations=['reduce_by_key'])]
```

`reduce_by_key` 是宽依赖，把 DAG 切成了两个 Stage。

## 13. 常见误解

### 误解 1：Shuffle 只是普通 map

不是。普通 `map` 是窄依赖，Partition 自己算自己的，不搬数据。Shuffle 会跨 Partition 重新搬运数据，是宽依赖。

### 误解 2：group_by_key 和 reduce_by_key 一样

不一样。`group_by_key` 保留所有 value（列表），`reduce_by_key` 直接合并 value（单个结果）。后者数据量小得多，通常更高效。

### 误解 3：Shuffle 不影响性能

恰恰相反。真实 Spark 中 Shuffle 往往是性能瓶颈，涉及网络和磁盘。调优的核心目标之一就是减少 Shuffle。

### 误解 4：Mini Spark 的 Shuffle 和真实 Spark 一样重

不是。Mini Spark 是纯内存一次性分桶，没有网络、没有写盘、没有 spill。真实 Spark 的 Shuffle 重量级得多。

### 误解 5：reduce_by_key 在 Mini Spark 里也做了 map-side combine

没有。Mini Spark 的 `reduce_by_key` 是把所有上游数据拉到一起后边读边合并，这是 reduce-side 的合并，不是真正的 map-side combine。真实 Spark 的 `reduceByKey` 会在上游写 shuffle 之前先在本地预聚合。

## 14. 本章掌握标准

如果你能讲清楚下面这些话，就算这一章过关：

- 朴素方案“每个分区自己聚合”绕不开“同一 key 散落多处”的问题，必须跨 Partition 重组。
- Shuffle 是跨 Partition 按 key 重新分发数据，本质是宽依赖。
- 相同 key 通过 `hash(key) % num_partitions` 落到同一目标分区，从而聚到一起。
- Shuffle 是 Spark 最贵的操作，会切断 pipeline、强制物化、切 Stage。
- `reduce_by_key` 通常比 `group_by_key` 好，因为它能减少搬运的数据量（真实 Spark 还有 map-side combine）。

## 15. 思考题

1. 如果只做“分区内聚合”不做 Shuffle，为什么得不到正确的全局聚合结果？
2. 为什么 Shuffle 是宽依赖？宽在哪里？
3. 为什么 `group_by_key` 容易占用很多内存和网络？它和 `reduce_by_key` 的数据量差别从哪来？
4. 真实 Spark 的 map-side combine 为什么对 `reduceByKey` 有效，对 `groupByKey` 却帮不上忙？
5. Shuffle 为什么会切开 Stage？如果上游没写完就开始读，会发生什么？
