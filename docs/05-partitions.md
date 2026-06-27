# 05 - Partition：为什么数据要切成几份

## 0. 这一章先学什么，不学什么

这一章学习 Partition，也就是分区。

前面我们的 RDD 看起来像一整份数据。现在要引入一个更接近 Spark 的想法：

> 一个 RDD 不是一整块数据，而是由多个 Partition 组成。

这一章先不学 Scheduler、Task、Executor。我们只先把数据切成多份，并让 Transformation 在每个 Partition 内部执行。

可视化页面：

[Partition 可视化](visualizations/05-partitions.html)

## 1. 先从最朴素的方案开始

到目前为止，Mini Spark 的 RDD 在内部就是一整块数据：

```text
Root RDD
  _data = (1, 2, 3, 4, 5)
```

处理时，`map` 对这一整块数据挨个转换。这就像一个厨师独自把所有菜做完。

朴素方案就是：**永远一整块数据，一个进程从头算到尾**。

对小数据这没问题。但 Spark 的目标是“把数据分到多台机器上一起算”。

## 2. 朴素方案会遇到什么问题

### 问题 1：没法并行

一整块数据意味着只能一个人算。哪怕你有 100 台机器，也用不上——因为数据没拆开，没法分给多台机器同时处理。

### 问题 2：没法分布式存放

一整块数据默认在一个进程的内存里。但真实 Spark 的数据往往有几十上百 GB，单机内存放不下，必须分散到多台机器。

### 问题 3：容错粒度太粗

如果数据是一整块，丢了就是整块丢，重算就是整块重算。没法做到“只重算丢的那一小份”。

### 问题 4：负载没法均衡

有的数据可能天然分布不均（比如按 key 聚合后某些 key 特别多）。如果是一整块，没法把“重的那部分”单独交给更强的机器。

## 3. Spark 为什么需要 Partition

Spark 的选择是：**把数据切成若干份，每一份叫一个 Partition**。

```text
一个 RDD = 多个 Partition 的集合
Partition 0 | Partition 1 | Partition 2 | ...
```

这一刀切下去，好处立刻就来了：

- **可并行**：每个 Partition 可以交给一个 Task，多个 Task 同时跑。
- **可分布**：不同 Partition 可以放在不同机器上。
- **可容错**：丢一个 Partition 只重算那一个。
- **可均衡**： Partition 太大的可以再切（repartition），太小的可以合并。

所以 Partition 不是“多此一举的包装”，而是 Spark 实现并行、分布、容错、负载均衡的**最小工作单元**。

## 4. 这个设计的核心思想

核心思想：**Partition 是并行计算的最小单位**。

- 一个 RDD 有 N 个 Partition，理想情况下就能用 N 个 Task 并行处理。
- 窄依赖 Transformation（`map`、`filter`、`flat_map`）在每个 Partition **内部**独立执行，不需要看别的 Partition。
- 因为“每个 Partition 自己算自己的”，所以它们天然可以并行。

这一章只把“切分”做出来，**还不真正并行**（没有 Scheduler/Executor，下一章才有）。但分区结构已经为并行打好了地基。

## 5. 这个设计解决了什么问题

- 解决“没法并行”：分区数 ≈ 并行度。
- 解决“没法分布”：分区可以分散到不同机器。
- 解决“容错粒度太粗”：以 Partition 为单位丢失/重算。
- 为后续 Scheduler 把“Partition → Task”铺好路。

## 6. 这个设计付出了什么代价

- **调度开销**：Partition 越多，要管理的 Task 越多，调度本身有成本。所以不是越多越好。
- **数据倾斜**：如果切分不均，某个 Partition 特别大，并行度就会被这一个慢 Partition 拖垮（木桶效应）。
- **跨 Partition 操作变贵**：一旦要让相同 key 聚到一起（Shuffle），就要跨 Partition 搬数据，这是 Spark 最贵的操作（第 08 章）。

## 7. Mini Spark 当前如何实现

我们新增了 `Partition`：

```python
@dataclass(frozen=True)
class Partition(Generic[T]):
    index: int
    data: tuple[T, ...]
```

每个 Partition 有：

- `index`：分区编号。
- `data`：这个分区里的数据（不可变 tuple）。

Root RDD 在 `parallelize` 时按 `num_slices` 把数据切成多个 Partition 保存。

Derived RDD 不直接保存数据，而是对父 RDD 的**每个 Partition** 分别应用 transform。因为窄依赖下“子 Partition i 只依赖父 Partition i”，分区数量和边界都保持不变。

## 8. 最小示例：创建两个分区

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

`collect_partitions()` 让你看到分区结构（这是 Mini Spark 为学习加的观察工具）。

`collect()` 仍然返回拍平后的完整数据。

### 逐行解释

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

## 9. Transformation 如何处理分区

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

当前 `map`、`filter`、`flat_map` 都是窄依赖，所以它们不会打乱分区数量——子 RDD 的 Partition i 只看父 RDD 的 Partition i。

## 10. Mini Spark 内部发生了什么

调用 `collect_partitions()` 时，Mini Spark 会对每个 Partition 分别调用 `_compute_partition(i)`：

```text
对 Partition 0：沿父链算出 Partition 0 的数据
对 Partition 1：沿父链算出 Partition 1 的数据
把各分区的结果收集成 list
```

注意：当前是**顺序地**逐个分区计算，并不是真正并行。真正并行要等下一章的 Scheduler + Executor。这一章只保证“数据结构上已经分好了份”。

## 11. 对照真实 Spark：真实世界复杂在哪里

| Mini Spark | 真实 Spark |
| --- | --- |
| `num_slices` | 分区数量 |
| `Partition(index, data)` | `Partition` 抽象（只持有位置信息，数据在 Executor 上） |
| `collect_partitions()` | 学习用观察工具 |
| Transformation 在每个 Partition 内部执行 | 一个 Spark Task 处理一个 Partition |
| 顺序逐个算 Partition | 多个 Task 在多个 Executor 上真正并行 |

真实 Spark 中 Partition 是并行计算的基础。分区太少，并行度不够；分区太多，调度开销变大。

真实 Spark 比 Mini Spark 复杂的地方：

- Partition 对象**不持有数据**，只持有“数据在哪台机器的哪个块”的位置信息（`Partition` + `PreferredLocation`）。数据真正存在 Executor 的 BlockManager 里。
- Partition 的切分方式多样：HDFS 文件块天然就是分区、`range` 分区、`hash` 分区等。
- 数据本地性：Scheduler 会尽量把 Task 调度到数据所在的机器，避免网络传输。

Mini Spark 的 Partition 直接把数据 `tuple` 装在对象里，相当于“位置和数据合一”，这是为了学习简化。

## 12. 亲手实验

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc = SparkContext(); rdd = sc.parallelize([1, 2, 3, 4], num_slices=2).filter(lambda x: x > 2); print(rdd.collect_partitions()); print(rdd.collect())"
```

你会看到：

```text
[[], [3, 4]]
[3, 4]
```

第一个分区被过滤空了，但**分区结构仍然保留**（空分区还在，数量不变）。这一点很重要：窄依赖不会改变分区数量，哪怕某个分区变空了。

## 13. 常见误解

### 误解 1：Partition 会改变数据内容

不会。Partition 只是把数据分组，不改变数据本身。同样的数据切 2 份和切 4 份，`collect()` 出来结果一样。

### 误解 2：分区越多越好

不一定。分区多可以增加并行度，但也会增加调度开销，而且每个分区太小会让 Task 启动成本占比过高。一般经验是每个 Partition 至少几 MB 到上百 MB。

### 误解 3：有了 Partition 就已经并行了

没有。这一章只切了数据，计算还是顺序的。真正并行要等下一章的 Scheduler 和 Executor。

### 误解 4：collect_partitions 是 Spark 标准 API

不是。这是 Mini Spark 为学习提供的观察工具，真实 Spark 没有同名 API。

### 误解 5：filter 之后分区数会减少

不会。窄依赖保持分区数不变，哪怕某个分区被过滤空了，空分区也还在。

## 14. 本章掌握标准

如果你能讲清楚下面这些话，就算这一章过关：

- 一整块数据的朴素方案在“并行、分布、容错、负载均衡”四方面都撑不住。
- Partition 是并行计算的最小工作单元，不是多余的包装。
- 窄依赖 Transformation 在每个 Partition 内部独立执行，不改变分区数。
- 当前 Mini Spark 只切了数据、还没真正并行；真实并行要等 Scheduler。
- 分区数 ≈ 并行度，但不是越多越好。

## 15. 思考题

1. 如果数据永远是一整块，Spark 还能叫“分布式”计算吗？为什么？
2. Partition 和 Task 之间可能是什么关系？为什么通常一个 Partition 对应一个 Task？
3. 为什么 `filter` 可能让某个 Partition 变空，但分区数量不变？
4. 分区数量太少有什么问题？太多又有什么问题？
5. 真实 Spark 的 Partition 对象不持有数据本身，只持有位置信息。这样做有什么好处？
