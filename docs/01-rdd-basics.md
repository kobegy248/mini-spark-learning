# 01 - RDD 基础：先把 Spark 的入口跑起来

## 0. 这一章先学什么，不学什么

这一章我们先不学集群、不学调度器、不学 Shuffle，也不学复杂优化。

我们只解决一个最小问题：

> 如何把一份普通 Python 数据，包装成一个 Mini Spark 里的 `RDD`，然后通过 `collect()` 把数据取回来？

也就是这段代码：

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

你先把这一章理解成 Spark 学习的“开机仪式”：我们还没开始真正分布式计算，但先把 Spark 最基本的调用方式搭起来。

## 1. 用人话理解这一章

如果完全不懂 Spark，可以先这样理解：

- `SparkContext`：Mini Spark 的入口。
- `parallelize`：把普通 Python 数据放进 Mini Spark 世界。
- `RDD`：Mini Spark 世界里的数据对象。
- `collect`：把 Mini Spark 世界里的数据拿回 Python 程序。

可以类比成：

```text
普通 Python 列表
  ↓ parallelize
Mini Spark 里的 RDD
  ↓ collect
普通 Python 列表
```

现在我们的 Mini Spark 还很小，所以 `RDD` 里面只是保存了一份本地数据。

真实 Spark 里的 RDD 要复杂得多：它的数据可能分散在很多机器上，并且 RDD 通常不直接保存完整数据，而是保存“怎么得到这些数据”的计算信息。

## 2. 最小示例：先跑起来

你可以运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc = SparkContext(); rdd = sc.parallelize([1, 2, 3]); print(rdd.collect())"
```

应该看到：

```text
[1, 2, 3]
```

这说明目前的最小流程已经跑通：

```text
创建 SparkContext
  ↓
parallelize 创建 RDD
  ↓
collect 取回数据
```

## 3. 逐行解释代码

### 第 1 行：导入 SparkContext

```python
from mini_spark import SparkContext
```

这行表示：从我们自己的 `mini_spark` 包里拿到 `SparkContext`。

在真实 PySpark 里，你也会经常看到类似入口，只是实际项目中更多会通过 `SparkSession` 使用 Spark。

### 第 2 行：创建入口对象

```python
sc = SparkContext()
```

`sc` 是 SparkContext 的实例。

你可以把它理解成“Mini Spark 的控制台”。后面我们要创建 RDD，就从它开始。

真实 Spark 中，`SparkContext` 背后会连接集群、管理配置、申请资源、提交任务。现在我们的版本只做一件事：创建 RDD。

### 第 3 行：把 Python 列表变成 RDD

```python
rdd = sc.parallelize([1, 2, 3])
```

`[1, 2, 3]` 原本只是普通 Python 列表。

调用 `parallelize` 后，它被包装成一个 `RDD` 对象。

当前阶段你可以先理解成：

```text
[1, 2, 3]  ->  RDD(_data=(1, 2, 3))
```

这里 `_data=(1, 2, 3)` 是我们 Mini Spark 内部保存的数据。

### 第 4 行：把 RDD 数据取回来

```python
print(rdd.collect())
```

`collect()` 的意思是：把 RDD 中的数据收集回来，变成普通 Python 列表。

当前输出是：

```text
[1, 2, 3]
```

在真实 Spark 中，`collect()` 很重要，也很危险。因为它会把分布在 Executor 上的数据全部拉回 Driver。如果数据很大，Driver 可能直接内存爆掉。

## 4. Mini Spark 内部发生了什么

### 4.1 SparkContext 做了什么

代码在 [mini_spark/context.py](../mini_spark/context.py)：

```python
from collections.abc import Iterable
from typing import TypeVar

from mini_spark.rdd import RDD

T = TypeVar("T")


class SparkContext:
    """Entry point for creating Mini Spark RDDs."""

    def parallelize(self, data: Iterable[T]) -> RDD[T]:
        return RDD(data)
```

重点只有这一行：

```python
return RDD(data)
```

意思是：`parallelize` 不做复杂事情，它只是把输入数据交给 `RDD` 类，创建一个 RDD 对象。

### 4.2 RDD 做了什么

代码在 [mini_spark/rdd.py](../mini_spark/rdd.py)。

第一阶段最重要的是这两个能力：

```python
self._data = tuple(data) if data is not None else None
```

以及：

```python
def collect(self) -> list[T]:
    return list(self._compute())
```

你可以先抓住两个点：

1. RDD 内部把数据转成 `tuple`。
2. `collect()` 把内部数据转回 `list`。

为什么要转成 `tuple`？

因为 `tuple` 不可变。我们想让 RDD 有“不可变”的感觉。

试一下：

```python
from mini_spark import SparkContext

sc = SparkContext()
source = [1, 2, 3]

rdd = sc.parallelize(source)
source.append(4)

print(rdd.collect())
```

输出是：

```text
[1, 2, 3]
```

虽然外面的 `source` 被改成了 `[1, 2, 3, 4]`，但 RDD 不受影响。

这就是不可变性的第一层意义：创建好的 RDD 不应该被外部数据变化偷偷影响。

## 5. 什么是 RDD

RDD 全称是 Resilient Distributed Dataset，通常翻译成“弹性分布式数据集”。

这个名字很吓人，我们拆开看：

- Dataset：它表示一组数据。
- Distributed：真实 Spark 中，这组数据可以分布在多台机器上。
- Resilient：如果一部分数据丢了，Spark 可以根据依赖关系重新算出来。

但在第一阶段，我们只实现了最简单的 Dataset：

```text
RDD = 包着一份本地数据的对象
```

后面我们会逐步补上：

- Transformation：RDD 怎么生成新的 RDD。
- Lineage：RDD 怎么记录父子关系。
- Partition：RDD 的数据怎么拆分。
- Scheduler：RDD 的计算怎么被调度。
- Fault Tolerance：RDD 丢了怎么重算。

## 6. 什么是 Driver

现在你写的 Python 程序就是 Driver。

比如：

```python
sc = SparkContext()
rdd = sc.parallelize([1, 2, 3])
print(rdd.collect())
```

这段代码所在的进程，就是 Driver。

Driver 负责：

- 写 Spark 程序。
- 创建 RDD。
- 触发 Action。
- 接收 `collect()` 返回的结果。

真实 Spark 中还会有 Executor。Executor 负责真正处理分区数据。当前 Mini Spark 还没有 Executor，所以所有事情都在 Driver 本地完成。

## 7. 为什么 collect 是 Action

Spark 里有两类常见操作：

- Transformation：描述要怎么变，比如 `map`、`filter`。
- Action：真的要结果，比如 `collect`、`count`。

`collect()` 是 Action，因为它会要求 Spark：

> 现在请真的把数据算出来，并返回给我。

在第一阶段，我们还没有 Transformation，所以 `collect()` 看起来只是返回内部数据。

但先把它定义成 Action 很重要。因为第二阶段开始，`map`、`filter` 不会立即执行，只有 `collect()` 才会触发计算。

## 8. 对照真实 Spark

| Mini Spark 当前版本 | 真实 Spark |
| --- | --- |
| `SparkContext` 只负责创建 RDD | `SparkContext` 还负责连接集群、提交 Job、管理资源 |
| `RDD` 内部保存本地 `tuple` | RDD 通常保存分区、依赖和计算逻辑 |
| `collect()` 直接返回本地数据 | `collect()` 会触发 Job，把 Executor 上的数据拉回 Driver |
| 没有 Partition | 真实 RDD 由多个 Partition 组成 |
| 没有 Executor | 真实 Spark 在 Executor 上并行执行任务 |

所以你要记住：

> Mini Spark 是为了学习 Spark 思想，不是为了模拟完整 Spark 工程实现。

## 9. 亲手实验

### 实验 1：确认 RDD 不受外部列表影响

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc = SparkContext(); source = [1, 2, 3]; rdd = sc.parallelize(source); source.append(4); print(source); print(rdd.collect())"
```

你会看到：

```text
[1, 2, 3, 4]
[1, 2, 3]
```

这说明 RDD 创建时已经保存了自己的数据。

### 实验 2：确认 collect 返回的是新列表

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc = SparkContext(); rdd = sc.parallelize([1, 2, 3]); result = rdd.collect(); result.append(4); print(result); print(rdd.collect())"
```

你会看到：

```text
[1, 2, 3, 4]
[1, 2, 3]
```

这说明你修改 `collect()` 返回的列表，不会影响 RDD 内部数据。

## 10. 常见误解

### 误解 1：RDD 就是 Python 列表

不是。

当前 Mini Spark 的 RDD 很像列表，是因为我们还在第一阶段。

真实 Spark 的 RDD 更像一张“计算说明书”：它知道数据分几份、从哪里来、怎么计算出来。

### 误解 2：collect 只是打印数据

不是。

`collect()` 的含义是“把 RDD 的数据收集到 Driver”。

打印只是我们为了观察结果额外做的事情。

### 误解 3：collect 很安全

不一定。

小数据可以 `collect()`。大数据不要随便 `collect()`，因为真实 Spark 会把所有数据拉回 Driver。

实战中更常用：

- `take(n)`：取少量数据。
- `show(n)`：DataFrame 中查看少量数据。
- 写入文件或表，而不是全部拉回本地。

## 11. 本章掌握标准

如果你能用自己的话说清楚下面几句话，就算这一章过关：

- `SparkContext` 是 Mini Spark 的入口。
- `parallelize` 把普通 Python 数据包装成 RDD。
- 当前 RDD 内部用 `tuple` 保存数据，是为了避免外部修改影响 RDD。
- `collect()` 把 RDD 数据取回 Driver。
- 真实 Spark 中，RDD 不是简单列表，而是带有分区、依赖和计算逻辑的数据抽象。
- 真实 Spark 中，`collect()` 拉回大数据可能导致 Driver 内存问题。

## 12. 思考题

1. 为什么 `SparkContext` 适合作为创建 RDD 的入口？
2. 为什么当前实现要把输入数据转成 `tuple`？
3. 为什么 `collect()` 返回的是新的 `list`，而不是直接暴露内部数据？
4. 真实 Spark 中，`collect()` 为什么可能很危险？
5. 当前 Mini Spark 的 `RDD` 和真实 Spark 的 `RDD` 最大区别是什么？
