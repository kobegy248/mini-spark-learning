# 09 - Cache：为什么第二次 Action 可以少算一次

## 0. 这一章先学什么，不学什么

这一章学习 Cache。

前面我们知道：每次 Action 都会触发计算。如果同一条 RDD 链被多次使用，重复计算就会浪费时间。

Cache 的作用是：

> 第一次 Action 计算并保存结果，第二次 Action 直接复用保存的结果。

可视化页面：

[Cache 可视化](visualizations/09-cache.html)

## 1. 用人话理解 Cache

Cache 就像做题时把中间结果写在草稿纸上。

第一次算：

```text
源数据 -> map -> 结果
```

第二次再要结果时：

```text
直接看草稿纸
```

这能省掉重复计算。

## 2. 最小示例

```python
from mini_spark import SparkContext

sc = SparkContext()
rdd = sc.parallelize([1, 2, 3]).map(lambda value: value * 10).cache()

print(rdd.collect())
print(rdd.collect())
print(rdd.cache_info())
```

第二次 `collect()` 会命中缓存。

## 3. Mini Spark 内部发生了什么

RDD 新增了：

- `_cache_enabled`
- `_cached_partitions`
- `_cache_hits`
- `_cache_misses`

调用 `cache()` 后，只是打开缓存开关，并不会立刻计算。

第一次 Action：

```text
缓存为空
  ↓
真正计算
  ↓
把每个 Partition 的结果存起来
```

第二次 Action：

```text
发现缓存已有结果
  ↓
直接返回缓存 Partition
```

## 4. cache 和 persist

当前 Mini Spark 中：

```python
rdd.persist()
```

就是：

```python
rdd.cache()
```

真实 Spark 里 `persist` 可以指定存储级别，比如内存、磁盘、序列化等。我们现在先只实现最简单的内存缓存。

## 5. 对照真实 Spark

| Mini Spark | 真实 Spark |
| --- | --- |
| `_cached_partitions` | BlockManager 中的缓存块 |
| `cache()` | `RDD.cache()` |
| `persist()` | `RDD.persist(StorageLevel)` |
| 命中/未命中计数 | Spark UI Storage 相关信息 |

真实 Spark 的缓存更复杂，会考虑内存不足、磁盘溢写、缓存淘汰和 Executor 丢失。

## 6. 亲手实验

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc=SparkContext(); calls=[]; rdd=sc.parallelize([1,2,3]).map(lambda x: calls.append(x) or x*10).cache(); print(rdd.collect(), calls); print(rdd.collect(), calls); print(rdd.cache_info())"
```

你会看到第二次 `collect()` 后，`calls` 不再增加。

## 7. 常见误解

### 误解 1：cache 一调用就执行

不是。`cache()` 只是做标记，第一次 Action 才真正计算并缓存。

### 误解 2：cache 永远有好处

不一定。缓存会占内存。如果数据只用一次，缓存反而浪费资源。

## 8. 本章掌握标准

- `cache()` 只是打开缓存开关。
- 第一次 Action 是 cache miss。
- 第二次 Action 可以 cache hit。
- Cache 能减少重复计算，但会占用存储资源。

## 9. 思考题

1. 为什么 `cache()` 不应该立刻执行？
2. 什么情况下缓存有帮助？
3. 什么情况下缓存可能有害？
4. 为什么缓存要按 Partition 保存？
5. 如果 Executor 丢了，真实 Spark 的缓存会发生什么？
