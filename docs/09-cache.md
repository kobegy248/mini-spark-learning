# 09 - Cache：为什么第二次 Action 可以少算一次

## 0. 这一章先学什么，不学什么

这一章学习 Cache。

前面我们知道：每次 Action 都会触发计算。如果同一条 RDD 链被多次使用，重复计算就会浪费时间。

Cache 的作用是：

> 第一次 Action 计算并保存结果，第二次 Action 直接复用保存的结果。

可视化页面：

[Cache 可视化](visualizations/09-cache.html)

## 1. 先从最朴素的方案开始

到目前为止，Mini Spark 每次 Action 都会沿 Lineage 现算一遍：

```python
rdd = sc.parallelize(big).map(很贵的转换)

rdd.count()    # 算一遍
rdd.collect()  # 又算一遍
```

朴素方案会怎么解决重复计算？最直接的想法：

```text
既然怕重复算，那就每个 RDD 算完都自动存起来，下次直接用。
```

也就是说，朴素方案是**默认全部物化、全部缓存**。

## 2. 朴素方案会遇到什么问题

### 问题 1：内存根本不够

如果每个 RDD 都自动缓存，一条长链路上每一步的中间结果都要存下来。数据量大时，内存瞬间爆满。这又回到了第 01、02 章我们拼命想避免的“中间结果物化”问题。

### 问题 2：大部分缓存根本用不上

很多中间 RDD 后面根本没人再用。提前缓存它们，纯粹是浪费内存。

### 问题 3：用户没法表达“哪个值得缓存”

哪些 RDD 值得缓存，是业务决定的——比如一个 expensive 转换的结果、一个被多次复用的中间结果。朴素方案“全部缓存”抹掉了这个选择权，用户没法说“我只想缓存这一个”。

### 问题 4：和 Lazy 的初衷冲突

Lazy 的目标就是“不提前算、不提前存”。朴素方案又走回“提前存”的老路。

## 3. Spark 为什么需要 Cache

Spark 的选择是：**默认不缓存，把缓存权交给用户**。

- 平时：每次 Action 沿 Lineage 现算（省内存，但重复 Action 会重复算）。
- 用户觉得某个 RDD 会被反复使用、且重算很贵时，显式调用 `cache()` 给它做个标记。
- 第一次 Action 算完后，把这个 RDD 的各 Partition 结果存起来。
- 之后的 Action 命中缓存，直接复用，不再重算。

所以 Cache 是一个**用户主动选择的、按需的、按 RDD 粒度的**优化，而不是全局自动的。

## 4. 这个设计的核心思想

核心思想：**Cache 是惰性的标记 + 首次计算时的物化**。

```python
rdd = sc.parallelize([1,2,3]).map(lambda x: x*10).cache()
#         ↑ cache() 只是打开开关，此刻什么都没算、没存
```

`cache()` 不触发计算。它只是给 RDD 挂上一个“下次算完请把结果存起来”的标记。

真正的缓存发生在**第一次 Action**：算完之后，把每个 Partition 的结果写进 `_cached_partitions`。

第二次 Action 时，发现缓存已有，直接返回缓存的 Partition，跳过整条 Lineage 的重算。

### 一个生活类比：草稿纸

第一次算一道大题，你把中间结果抄在草稿纸上。第二次再要用这个中间结果时，直接看草稿纸，不必从头再算。但草稿纸有限，你只会把“反复要用的”关键中间结果抄上去，不会把每一步都抄——这就是用户显式 `cache()` 的含义。

## 5. 这个设计解决了什么问题

- **避免重复计算**：被多次 Action 使用的 RDD，只算一次。
- **内存可控**：默认不缓存，只缓存用户指定的，不会全局爆内存。
- **和容错配合**：缓存的 Partition 丢了，可以沿 Lineage 重算（下一章）。

## 6. 这个设计付出了什么代价

- **占内存**：缓存要占存储资源。数据只用一次就别缓存，否则纯浪费。
- **用户判断负担**：要缓存谁、不缓存谁，需要用户对数据流和重算成本有判断。
- **缓存失效/丢失**：真实 Spark 中 Executor 可能挂掉，缓存随之丢失，需要重算。
- **一致性问题**：RDD 不可变所以缓存结果稳定；但如果误用可变状态，缓存可能掩盖问题。

## 7. Mini Spark 当前如何实现

RDD 新增了：

- `_cache_enabled`
- `_cached_partitions`
- `_cache_hits`
- `_cache_misses`

调用 `cache()` 后，只是打开缓存开关，并不会立刻计算：

```python
def cache(self) -> "RDD[T]":
    self._cache_enabled = True
    return self
```

第一次 Action：

```text
缓存为空（cache miss）
  ↓
沿 Lineage 真正计算
  ↓
把每个 Partition 的结果存进 _cached_partitions
  ↓
hits 不变，misses +1
```

第二次 Action：

```text
发现缓存已有结果（cache hit）
  ↓
直接返回缓存的 Partition，不再走 Lineage
  ↓
hits +1
```

`cache_info()` 返回当前命中/未命中统计：

```python
{"enabled": True, "hits": 0, "misses": 1}   # 第一次
{"enabled": True, "hits": 1, "misses": 1}   # 第二次
```

## 8. cache 和 persist

当前 Mini Spark 中：

```python
rdd.persist()
```

就是：

```python
rdd.cache()
```

真实 Spark 里 `persist` 可以指定**存储级别**，比如：

- `MEMORY_ONLY`：只存内存，存不下就重算。
- `MEMORY_AND_DISK`：内存存不下时溢写到磁盘。
- `DISK_ONLY`：只存磁盘。
- 序列化/反序列化级别等。

Mini Spark 现在只实现最简单的内存缓存，没有存储级别概念。

## 9. 最小示例

```python
from mini_spark import SparkContext

sc = SparkContext()
rdd = sc.parallelize([1, 2, 3]).map(lambda value: value * 10).cache()

print(rdd.collect())
print(rdd.collect())
print(rdd.cache_info())
```

第一次 `collect()` 是 cache miss，会真正计算并把结果存起来；第二次 `collect()` 命中缓存，不再重算。

输出：

```text
[10, 20, 30]
[10, 20, 30]
{'enabled': True, 'hits': 1, 'misses': 1}
```

## 10. Mini Spark 内部发生了什么

```text
cache()                    → 打开 _cache_enabled，返回自己（没算）
第一次 collect()            → miss：算 → 存 _cached_partitions → misses=1
第二次 collect()            → hit：直接用 _cached_partitions → hits=1
```

注意缓存是**按 Partition 保存**的。这一点很关键：因为容错（下一章）和并行都关心 Partition 粒度。丢了某一个 Partition，可以只重算那一个，而不必清掉整个缓存。

## 11. 对照真实 Spark：真实世界复杂在哪里

| Mini Spark | 真实 Spark |
| --- | --- |
| `_cached_partitions` | BlockManager 中的缓存块 |
| `cache()` | `RDD.cache()`（等价于 `persist(MEMORY_ONLY)`） |
| `persist()` | `RDD.persist(StorageLevel)`，可选存储级别 |
| 命中/未命中计数 | Spark UI Storage 页面，显示每个 RDD 的缓存大小/分区数 |
| 第九阶段里缓存只放本地内存 | 内存不足时按存储级别溢写磁盘或丢弃 |
| 第九阶段还不模拟缓存丢失 | Executor 挂掉时缓存丢失，需重算 |

真实 Spark 的缓存更复杂，会考虑：

- **内存不足时的淘汰**：`MEMORY_ONLY` 存不下就直接丢弃（用时重算）；`MEMORY_AND_DISK` 存不下则写盘。
- **缓存块管理**：由 BlockManager 统一管理，跨 Executor。
- **Executor 丢失**：缓存在 Executor 内存里，Executor 挂了缓存就没了，后续 Action 会触发重算（和下一章容错呼应）。
- **序列化**：可选择以序列化形式存储，省内存但读取要反序列化。

到第九阶段为止，Mini Spark 的缓存只放在本地内存里，不模拟丢失，也没有淘汰策略——这是为了先把“缓存命中/未命中”和“避免重复计算”讲清楚。

下一章会故意加入 `simulate_partition_loss()` 和血缘重算，用来学习“缓存块丢了以后，为什么 RDD 还能靠 lineage 恢复”。

## 12. 亲手实验

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc=SparkContext(); calls=[]; rdd=sc.parallelize([1,2,3]).map(lambda x: calls.append(x) or x*10).cache(); print(rdd.collect(), calls); print(rdd.collect(), calls); print(rdd.cache_info())"
```

预期输出：

```text
[10, 20, 30] [1, 2, 3]
[10, 20, 30] [1, 2, 3]
{'enabled': True, 'hits': 1, 'misses': 1}
```

你会看到第二次 `collect()` 后，`calls` 不再增加——说明 `map` 没有被重新执行，缓存命中了。

对比：去掉 `cache()` 再跑一次，你会发现第二次 `collect()` 后 `calls` 变成了 `[1,2,3,1,2,3]`，即重算了一遍。

## 13. 常见误解

### 误解 1：cache 一调用就执行

不是。`cache()` 只是做标记，第一次 Action 才真正计算并缓存。这是为了保持 Lazy——标记本身零成本。

### 误解 2：cache 永远有好处

不一定。缓存会占内存。如果数据只用一次，缓存反而浪费资源。值得缓存的是“会被多次复用且重算昂贵”的 RDD。

### 误解 3：cache 之后 RDD 就永远不变了

RDD 本来就不可变，所以缓存结果稳定。但缓存**可能丢失**（真实 Spark 中 Executor 挂掉），丢了就要沿 Lineage 重算。cache 不是永久保险。

### 误解 4：缓存必须缓存整个 RDD 的全部数据

不一定。Mini Spark 按 Partition 保存，真实 Spark 也是按 Block 保存。这样丢一个可以只重算一个，也能部分命中。

### 误解 5：persist 和 cache 完全一样

在 Mini Spark 里目前一样。但在真实 Spark 里，`cache()` 是 `persist(MEMORY_ONLY)` 的简写，`persist` 可以指定更丰富的存储级别。

## 14. 本章掌握标准

如果你能讲清楚下面这些话，就算这一章过关：

- 朴素方案“默认全部缓存”会爆内存、浪费、抹掉用户选择权。
- Cache 是用户显式选择的、惰性的、按 RDD 粒度的优化。
- `cache()` 只做标记，第一次 Action 才物化缓存（miss），第二次 Action 命中（hit）。
- 缓存按 Partition 保存，便于部分命中和容错。
- Cache 省计算但占存储；数据只用一次就别缓存。

## 15. 思考题

1. 为什么 `cache()` 不应该立刻执行，而要等第一次 Action？（提示：Lazy）
2. 朴素方案“默认全部缓存”会有哪些问题？为什么把缓存权交给用户更好？
3. 什么情况下缓存有帮助？什么情况下缓存可能有害？
4. 为什么缓存要按 Partition 保存，而不是一整块？（提示：容错、部分命中）
5. 如果真实 Spark 的某个 Executor 丢了，它上面缓存的 Partition 会发生什么？后续 Action 怎么办？
