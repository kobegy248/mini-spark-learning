# 10 - Fault Tolerance：数据丢了为什么还能重新算

## 0. 这一章先学什么，不学什么

这一章学习容错。

前面我们实现了 Cache。现在模拟一个问题：

> 如果缓存里的某个 Partition 丢了，怎么办？

Mini Spark 的答案是：沿着 Lineage 重新计算丢失的 Partition。

可视化页面：

[Fault Tolerance 可视化](visualizations/10-fault-tolerance.html)

## 1. 用人话理解容错

你可以把 Lineage 想成菜谱。

菜丢了没关系，只要菜谱还在，就可以重新做一份。

Spark 也是类似：

```text
Partition 数据丢失
  ↓
找到它的 parent 和 transform
  ↓
重新计算这个 Partition
```

## 2. 最小示例

```python
from mini_spark import SparkContext

sc = SparkContext()
rdd = sc.parallelize([1, 2, 3, 4], num_slices=2).map(lambda value: value * 10).cache()

print(rdd.collect())
rdd.simulate_partition_loss(1)
print(rdd.lost_partitions())
print(rdd.collect())
```

输出：

```text
[10, 20, 30, 40]
[1]
[10, 20, 30, 40]
```

第二次 `collect()` 时，Mini Spark 会重新计算丢失的 Partition。

## 3. Mini Spark 内部发生了什么

RDD 新增：

- `simulate_partition_loss(index)`
- `lost_partitions()`
- `recover_lost_partitions()`

当缓存存在且某个 Partition 被标记丢失时：

```text
collect()
  ↓
发现缓存有丢失分区
  ↓
沿 parent 链重新计算对应 Partition
  ↓
写回缓存
  ↓
返回完整结果
```

当前 Mini Spark 对窄依赖可以只重算丢失的 Partition。对于宽依赖，真实 Spark 会更复杂，因为 Shuffle 数据可能来自多个上游 Partition。

## 4. 对照真实 Spark

| Mini Spark | 真实 Spark |
| --- | --- |
| `simulate_partition_loss` | Executor / Block 丢失的模拟 |
| `recover_lost_partitions` | 基于 Lineage 重算 |
| 只重算窄依赖 Partition | Spark 按依赖关系重新提交 Task |
| 缓存在本地内存 | BlockManager 管理缓存块 |

真实 Spark 不会为了容错无脑复制所有中间数据。它依赖 Lineage，在需要时重算。

## 5. 亲手实验

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc=SparkContext(); calls=[]; rdd=sc.parallelize([1,2,3,4], num_slices=2).map(lambda x: calls.append(x) or x*10).cache(); print(rdd.collect(), calls); rdd.simulate_partition_loss(1); print(rdd.lost_partitions()); print(rdd.collect(), calls)"
```

你会看到第二次只追加了丢失分区对应的数据。

## 6. 常见误解

### 误解 1：容错一定要复制所有数据

不一定。Spark 的一个核心思想是：能用 Lineage 重算，就不一定提前复制。

### 误解 2：重算没有成本

有成本。Lineage 很长或 Shuffle 很重时，重算也可能很慢。

## 7. 本章掌握标准

- Lineage 是容错基础。
- 丢失的 Partition 可以从父 RDD 重新计算。
- Cache 丢失后可以重新填充。
- 重算省存储，但会付出计算成本。

## 8. 思考题

1. 为什么 Lineage 能帮助容错？
2. 为什么 Spark 不总是复制中间数据？
3. 重算有什么成本？
4. 窄依赖和宽依赖的重算难度有什么不同？
5. Cache 和 Fault Tolerance 有什么关系？
