# 08 - Shuffle：为什么跨分区重组数据很贵

## 0. 这一章先学什么，不学什么

这一章学习 Shuffle。

前面我们已经知道 Partition 是并行计算的基础。现在遇到一个新问题：

> 如果同一个 key 的数据分散在不同 Partition，怎么把它们聚到一起？

答案是：Shuffle。

本章新增：

- `group_by_key`
- `reduce_by_key`

可视化页面：

[Shuffle 可视化](visualizations/08-shuffle.html)

## 1. 用人话理解 Shuffle

Shuffle 就像重新分拣快递。

原来数据按输入顺序分在不同 Partition：

```text
Partition 0: ("a", 1), ("b", 2)
Partition 1: ("a", 3), ("b", 4)
```

如果要按 key 聚合，就必须让相同 key 的数据去同一个地方：

```text
"a" 的数据聚到一起
"b" 的数据聚到一起
```

这个跨 Partition 重新搬运数据的过程，就是 Shuffle。

## 2. group_by_key

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

## 3. reduce_by_key

```python
print(sorted(rdd.reduce_by_key(lambda left, right: left + right).collect()))
```

输出：

```text
[('a', 4), ('b', 6)]
```

`reduce_by_key` 会把相同 key 的 value 合并成一个结果。

## 4. Mini Spark 内部发生了什么

本阶段给 RDD 新增了宽依赖 Transformation。

窄依赖的 transform 是：

```text
一个父 Partition -> 一个子 Partition
```

Shuffle 的宽依赖是：

```text
所有父 Partition -> 重新分发到多个子 Partition
```

所以 `group_by_key` 和 `reduce_by_key` 都会：

1. 读取所有父 Partition。
2. 按 key 分组或聚合。
3. 根据 key 决定目标 Partition。
4. 生成新的子 Partition。

## 5. 为什么 reduce_by_key 通常比 group_by_key 好

`group_by_key` 会保留所有 value：

```text
("a", [1, 3, 5, 7, 9])
```

如果 value 很多，这个列表会很大。

`reduce_by_key` 会边读边合并：

```text
1 + 3 + 5 + 7 + 9 = 25
```

真实 Spark 中，`reduceByKey` 还可以做 map-side combine，减少 Shuffle 数据量。

## 6. 对照真实 Spark

| Mini Spark | 真实 Spark |
| --- | --- |
| `group_by_key` | `groupByKey` |
| `reduce_by_key` | `reduceByKey` |
| `dependency_kind = "wide"` | 宽依赖 |
| 简化内存 shuffle | Shuffle Write / Shuffle Read |

真实 Spark 的 Shuffle 很复杂，会涉及磁盘、网络、排序、聚合、内存管理和失败重试。

## 7. 亲手实验

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc=SparkContext(); rdd=sc.parallelize([('a',1),('b',2),('a',3),('b',4)], num_slices=2); print(sorted(rdd.group_by_key().collect())); print(sorted(rdd.reduce_by_key(lambda a,b:a+b).collect()))"
```

## 8. 常见误解

### 误解 1：Shuffle 只是普通 map

不是。Shuffle 会跨 Partition 移动数据。

### 误解 2：group_by_key 和 reduce_by_key 一样

不一样。`group_by_key` 保留所有 value，`reduce_by_key` 直接合并 value。

### 误解 3：Shuffle 不影响性能

真实 Spark 中 Shuffle 往往是性能瓶颈。

## 9. 本章掌握标准

- Shuffle 是跨 Partition 重组数据。
- Shuffle 会产生宽依赖。
- 宽依赖会切开 Stage。
- `reduce_by_key` 通常比 `group_by_key` 更适合聚合。

## 10. 思考题

1. 为什么按 key 聚合通常需要 Shuffle？
2. 为什么 Shuffle 是宽依赖？
3. 为什么 `group_by_key` 容易占用很多内存？
4. `reduce_by_key` 为什么通常更高效？
5. Shuffle 为什么会影响 Stage 划分？
