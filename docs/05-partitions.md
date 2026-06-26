# 05 - Partition：为什么数据要切成几份

## 0. 这一章先学什么，不学什么

这一章学习 Partition，也就是分区。

前面我们的 RDD 看起来像一整份数据。现在要引入一个更接近 Spark 的想法：

> 一个 RDD 不是一整块数据，而是由多个 Partition 组成。

这一章先不学 Scheduler、Task、Executor。我们只先把数据切成多份，并让 Transformation 在每个 Partition 内部执行。

可视化页面：

[Partition 可视化](visualizations/05-partitions.html)

## 1. 用人话理解 Partition

如果 RDD 是一本书，Partition 就是这本书拆出来的几章。

真实 Spark 要并行计算，就不能让所有数据挤在一个地方。它会把数据拆成多个 Partition，将来每个 Partition 可以交给一个 Task 去处理。

当前 Mini Spark 还没有 Task，但我们先把“数据拆成多份”这个基础打好。

## 2. 最小示例：创建两个分区

```python
from mini_spark import SparkContext

sc = SparkContext()
rdd = sc.parallelize([1, 2, 3, 4, 5], num_slices=2)

print(rdd.collect_partitions())
print(rdd.collect())
```

输出：

```text
[[1, 2, 3], [4, 5]]
[1, 2, 3, 4, 5]
```

`collect_partitions()` 让你看到分区结构。

`collect()` 仍然返回拍平后的完整数据。

## 3. 逐行解释代码

```python
rdd = sc.parallelize([1, 2, 3, 4, 5], num_slices=2)
```

这行表示：

```text
请把 [1, 2, 3, 4, 5] 切成 2 个 Partition。
```

当前 Mini Spark 会尽量平均切：

```text
Partition 0: [1, 2, 3]
Partition 1: [4, 5]
```

```python
rdd.collect_partitions()
```

返回：

```text
[[1, 2, 3], [4, 5]]
```

```python
rdd.collect()
```

返回：

```text
[1, 2, 3, 4, 5]
```

## 4. Transformation 如何处理分区

```python
rdd = sc.parallelize([1, 2, 3, 4], num_slices=2).map(lambda value: value * 10)
print(rdd.collect_partitions())
```

输出：

```text
[[10, 20], [30, 40]]
```

这说明 `map` 是在每个 Partition 内部执行的：

```text
Partition 0: [1, 2] -> [10, 20]
Partition 1: [3, 4] -> [30, 40]
```

当前 `map`、`filter`、`flat_map` 都是窄依赖，所以它们不会打乱分区数量。

## 5. Mini Spark 内部发生了什么

我们新增了 `Partition`：

```python
@dataclass(frozen=True)
class Partition(Generic[T]):
    index: int
    data: tuple[T, ...]
```

每个 Partition 有：

- `index`：分区编号。
- `data`：这个分区里的数据。

Root RDD 保存多个 Partition。

Derived RDD 不直接保存数据，而是对父 RDD 的每个 Partition 应用 transform。

## 6. 对照真实 Spark

| Mini Spark | 真实 Spark |
| --- | --- |
| `num_slices` | 分区数量 |
| `Partition(index, data)` | `Partition` 抽象 |
| `collect_partitions()` | 学习用观察工具 |
| Transformation 在每个 Partition 内部执行 | Spark Task 通常处理一个 Partition |

真实 Spark 中，Partition 是并行计算的基础。分区太少，并行度不够；分区太多，调度开销变大。

## 7. 亲手实验

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc = SparkContext(); rdd = sc.parallelize([1, 2, 3, 4], num_slices=2).filter(lambda x: x > 2); print(rdd.collect_partitions()); print(rdd.collect())"
```

你会看到：

```text
[[], [3, 4]]
[3, 4]
```

第一个分区被过滤空了，但分区结构仍然保留。

## 8. 常见误解

### 误解 1：Partition 会改变数据内容

不会。Partition 只是把数据分组，不改变数据本身。

### 误解 2：分区越多越好

不一定。分区多可以增加并行度，但也会增加调度开销。

### 误解 3：collect_partitions 是 Spark 标准 API

不是。这是 Mini Spark 为学习提供的观察工具。

## 9. 本章掌握标准

- RDD 由多个 Partition 组成。
- Partition 是后续 Task 并行执行的基础。
- 窄依赖 Transformation 会在每个 Partition 内部执行。
- 当前 Mini Spark 还没有真正并行，只是先保留分区结构。

## 10. 思考题

1. 为什么 Spark 不把所有数据都放在一个 Partition？
2. Partition 和 Task 之间可能是什么关系？
3. 为什么 `filter` 可能让某个 Partition 变空？
4. 分区数量太少有什么问题？
5. 分区数量太多有什么问题？
