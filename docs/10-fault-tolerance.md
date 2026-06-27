# 10 - Fault Tolerance：数据丢了为什么还能重新算

## 0. 这一章先学什么，不学什么

这一章学习容错。

前面我们实现了 Cache。现在模拟一个问题：

> 如果缓存里的某个 Partition 丢了，怎么办？

Mini Spark 的答案是：沿着 Lineage 重新计算丢失的 Partition。

可视化页面：

[Fault Tolerance 可视化](visualizations/10-fault-tolerance.html)

## 1. 先从最朴素的方案开始

数据会丢，这在分布式系统里不是“会不会”的问题，而是“什么时候”的问题——机器会挂、进程会崩、磁盘会坏、网络会断。

朴素方案怎么应对数据丢失？最常见的两种：

- **方案 A：全部复制**。每个 Partition 都存两三份副本，丢一份还有备份。
- **方案 B：从头重跑**。不存副本，丢了就把整个作业从头再跑一遍。

## 2. 朴素方案会遇到什么问题

### 方案 A（全部复制）的问题

- **存储翻倍**：存两份副本，存储成本直接 ×2 或 ×3。Spark 处理的数据本来就大，复制成本惊人。
- **复制不能解决所有问题**：副本也要同步、也要管理，复杂度高。
- **对中间结果也复制太奢侈**：RDD 链路中间的临时结果本就是“算得出”的，没必要都存副本。

### 方案 B（从头重跑）的问题

- **粒度太粗**：只丢了一个 Partition，却要把整条链路、所有 Partition 重跑，浪费巨大。
- **可能重跑很久**：链路长、Shuffle 重时，从头跑可能要几十分钟。

两个方案都不理想：复制太贵，重跑太粗。

## 3. Spark 为什么这样设计容错

Spark 的关键洞察是：**RDD 不可变 + Lineage 记录了“怎么算出来的”**。

既然每个 Partition 都能沿 Lineage 从父 RDD 重新算出来，那就没必要提前复制所有数据。丢失时，**只重算丢失的那一个 Partition**就够了。

这就是 Spark 的容错哲学：

> 用 Lineage 重算，替代提前复制。

- 平时：不复制中间数据，省存储。
- 丢失时：沿 Lineage 找到这个 Partition 的父 RDD 和 transform，只重算它一个。
- 如果这条 Lineage 太长、重算太贵：用 Cache 把关键中间结果存下来，缩短重算路径。

所以容错、Lineage、Cache 三者是配合的：Lineage 让重算成为可能，Cache 让重算不至于太贵，容错机制把“丢失 → 重算”自动化。

## 4. 这个设计的核心思想

核心思想：**以 Partition 为粒度，沿 Lineage 精准重算丢失部分**。

```text
某个 Partition 丢失
  ↓
找到它的 parent 和 transform
  ↓
只重新计算这一个 Partition
  ↓
写回缓存
  ↓
返回完整结果
```

这依赖两个前提（前几章打下的地基）：

- **RDD 不可变**：父 RDD 不会变，重算结果和第一次一致、可预测。
- **Lineage 完整**：能沿 parent 链找到完整的“配方”。

## 5. 这个设计解决了什么问题

- **省存储**：不必为所有数据存副本。
- **粒度细**：只重算丢失的 Partition，不重跑整条链。
- **自动化**：用户无需手写恢复逻辑，系统沿 Lineage 自动重算。
- **可调**：通过 Cache 控制重算成本（缓存越多，重算越短，但存储越贵）。

## 6. 这个设计付出了什么代价

- **重算有计算成本**：丢一个 Partition 就要重算它。Lineage 长、Shuffle 重时，重算也可能很慢。
- **宽依赖重算更贵**：宽依赖下一个子 Partition 依赖多个父 Partition，重算它可能要把多个上游 Partition 也重算一遍。
- **依赖 Lineage 完整**：如果 Lineage 被切断（比如 RDD 被回收又没缓存），就没法重算，只能从头来。
- **不保证“零丢失”**：Spark 不追求数据绝不丢，而是追求“丢了能算回来”。这和数据库的强持久化是不同的取舍。

## 7. Mini Spark 当前如何实现

RDD 新增：

- `simulate_partition_loss(index)`：模拟某个 Partition 丢失（标记它丢失）。
- `lost_partitions()`：查看哪些 Partition 被标记丢失。
- `recover_lost_partitions()`：重算丢失的 Partition 并写回缓存。

当缓存存在且某个 Partition 被标记丢失时，`_compute_partitions()` 会先调用 `recover_lost_partitions()`：

```text
collect()
  ↓
检查缓存：发现 _lost_partition_indexes 非空
  ↓
recover_lost_partitions()
  ↓
对每个丢失的 index：沿 Lineage 重新算这一个 Partition
  ↓
把重算结果写回 _cached_partitions 对应位置
  ↓
清空丢失标记
  ↓
返回完整结果
```

`recover_lost_partitions` 的核心：

```python
restored = [tuple(partition) for partition in self._cached_partitions]
for index in sorted(self._lost_partition_indexes):
    restored[index] = tuple(self._compute_partition_uncached(index))
self._cached_partitions = tuple(restored)
```

人话翻译：

```text
把缓存里没丢的 Partition 保留下来，
只对丢失的那几个 index 调用 _compute_partition_uncached 重新算，
塞回原来的位置。
```

注意是 `_compute_partition_uncached(index)`——只重算一个 Partition，而不是整条链。这正是“精准重算”的体现。

当前 Mini Spark 对窄依赖可以只重算丢失的 Partition。对于宽依赖，真实 Spark 会更复杂：一个宽依赖的子 Partition 可能依赖多个上游 Partition，重算范围会更大。

## 8. 最小示例

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

过程：

```text
第一次 collect()  → 算出 [10,20,30,40]，缓存 Partition 0=[10,20]、Partition 1=[30,40]
simulate_partition_loss(1) → 标记 Partition 1 丢失，lost_partitions() 返回 [1]
第二次 collect()  → 发现 Partition 1 丢失，沿 Lineage 重算它 → [30,40]，写回缓存
                  → 返回完整的 [10,20,30,40]
```

第二次 `collect()` 时，Partition 0 命中缓存没重算，只有 Partition 1 被重算。

## 9. Mini Spark 内部发生了什么

用 `calls` 列表验证“只重算了丢失的 Partition”：

```python
calls = []
rdd = sc.parallelize([1,2,3,4], num_slices=2).map(
    lambda x: calls.append(x) or x*10
).cache()
print(rdd.collect(), calls)   # 第一次：算全部，calls=[1,2,3,4]
rdd.simulate_partition_loss(1)
print(rdd.collect(), calls)   # 第二次：只重算 Partition 1（数据 3,4），calls 追加 [3,4]
```

第二次 `collect()` 后，`calls` 只追加了丢失 Partition 1 对应的数据，没有把 Partition 0 也重算一遍。这就是精准重算。

## 10. 对照真实 Spark：真实世界复杂在哪里

| Mini Spark | 真实 Spark |
| --- | --- |
| `simulate_partition_loss` | Executor 挂掉 / Block 丢失的模拟 |
| `recover_lost_partitions` | 基于 Lineage 重算丢失 Partition |
| 只重算窄依赖 Partition | 按依赖关系重新提交 Task；宽依赖重算范围更大 |
| 缓存在本地内存 | BlockManager 管理缓存块，分布在 Executor 上 |
| 主动模拟丢失 | 真实场景是被动丢失（节点故障），Task 失败后由 TaskScheduler 重试 |

真实 Spark 不会为了容错无脑复制所有中间数据。它依赖 Lineage，在需要时重算：

- 一个 Task 失败 → TaskScheduler 自动重试这个 Task（换一个 Executor）。
- 一个 Executor 挂掉 → 它上面的缓存 Partition 丢失，依赖这些 Partition 的下游 Task 会被重新调度，沿 Lineage 重算。
- Stage 级别失败 → DAGScheduler 重新提交整个 Stage。

宽依赖重算的复杂之处：一个 Shuffle 后的 Partition 依赖所有上游 Partition，重算它可能要把上游 Stage 的相关 Task 都重跑。所以真实 Spark 会尽量把 Shuffle 数据落盘（Shuffle 文件），这样上游不用重算，只要重新读 shuffle 文件即可——这是对“宽依赖重算贵”的工程缓解。

Mini Spark 没有持久化的 Shuffle 文件，所以宽依赖重算会真正重跑上游。

## 11. 亲手实验

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc=SparkContext(); calls=[]; rdd=sc.parallelize([1,2,3,4], num_slices=2).map(lambda x: calls.append(x) or x*10).cache(); print(rdd.collect(), calls); rdd.simulate_partition_loss(1); print(rdd.lost_partitions()); print(rdd.collect(), calls)"
```

预期输出：

```text
[10, 20, 30, 40] [1, 2, 3, 4]
[1]
[10, 20, 30, 40] [1, 2, 3, 4, 3, 4]
```

第二次 `collect()` 后 `calls` 只追加了 `[3, 4]`——正是丢失的 Partition 1 对应的数据，Partition 0 命中缓存没重算。

## 12. 常见误解

### 误解 1：容错一定要复制所有数据

不一定。Spark 的核心思想是：能用 Lineage 重算，就不必提前复制。复制太贵，重算更划算（尤其对中间结果）。

### 误解 2：重算没有成本

有成本。Lineage 很长或 Shuffle 很重时，重算也可能很慢。所以才需要 Cache 来缩短重算路径。

### 误解 3：丢了一个 Partition 要重跑整个作业

不需要。Mini Spark 和真实 Spark 都尽量只重算丢失的 Partition（窄依赖下）。宽依赖下范围会扩大，但仍不是“从头重跑整个作业”。

### 误解 4：Cache 是为了容错

不完全是。Cache 主要为了**避免重复计算**（第 09 章）。但它客观上缩短了重算路径，所以也起到缓解容错重算成本的作用。两者是配合关系。

### 误解 5：Mini Spark 的容错和真实 Spark 一样

不一样。Mini Spark 是“主动模拟丢失 + 本地重算”，没有真实的节点故障、没有 TaskScheduler 重试、没有持久化的 Shuffle 文件。真实 Spark 的容错是被动应对分布式故障，机制复杂得多。

## 13. 本章掌握标准

如果你能讲清楚下面这些话，就算这一章过关：

- 朴素方案“全复制”太贵、“从头重跑”太粗，都不理想。
- Spark 用 Lineage 重算替代提前复制，以 Partition 为粒度精准重算。
- 这依赖 RDD 不可变（重算结果可预测）和 Lineage 完整（能找到配方）。
- Cache 不是为了容错，但能缩短重算路径，缓解容错成本。
- 宽依赖重算比窄依赖贵，真实 Spark 用持久化 Shuffle 文件来缓解。

## 14. 思考题

1. 为什么 Spark 选择“沿 Lineage 重算”而不是“提前复制所有数据”？各自的代价是什么？
2. 丢了一个 Partition，为什么不必重跑整个作业？这依赖哪两个前提？
3. 重算有什么成本？Cache 如何缓解这个成本？
4. 窄依赖和宽依赖的重算难度有什么不同？为什么宽依赖更贵？
5. 真实 Spark 把 Shuffle 数据落盘，这对宽依赖重算有什么帮助？
