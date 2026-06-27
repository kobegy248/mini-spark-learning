# 01 - RDD 基础：为什么 Spark 需要 RDD 这种设计

## 0. 这一章先学什么，不学什么

这一章不是为了背诵“RDD 是弹性分布式数据集”这个定义。

这一章真正要解决的问题是：

> 如果我们想处理大规模数据，为什么不能只用普通列表、普通函数和普通循环？Spark 为什么要设计出 RDD 这个抽象？

我们会先从最朴素的方案开始，然后一步一步看它为什么不够用，最后推导出 RDD 的设计思想。

这一章先不学：

- 真正的集群通信
- Scheduler
- Shuffle
- Cache
- Spark SQL

我们只先抓住 RDD 的核心设计动机：

```text
RDD 不是简单的数据容器，
而是一份可分区、可延迟计算、可记录血缘、可失败重算的数据计算描述。
```

当前 Mini Spark 代码还很简单，但学习重点不是“代码多复杂”，而是“为什么要朝这个方向设计”。

## 1. 先从最朴素的方案开始

假设我们有一份数据：

```python
data = [1, 2, 3, 4]
```

我们想做：

```text
每个元素乘以 10
保留大于 20 的元素
统计剩下几个
```

用普通 Python 很容易写：

```python
data = [1, 2, 3, 4]

mapped = [x * 10 for x in data]
filtered = [x for x in mapped if x > 20]
result = len(filtered)

print(result)
```

输出：

```text
2
```

这个方案对小数据很好理解，也很好用。

它的执行过程是：

```text
[1, 2, 3, 4]
  ↓ 立刻执行 map，生成 mapped
[10, 20, 30, 40]
  ↓ 立刻执行 filter，生成 filtered
[30, 40]
  ↓ count
2
```

这就是最朴素的设计：每一步都马上计算，并生成一个新的中间结果。

## 2. 朴素方案遇到大数据会怎样

如果数据只有 4 条，刚才的方案没有问题。

但 Spark 要处理的不是 4 条数据，而可能是：

- 4 亿条日志
- 10 TB 明细数据
- 分散在 100 台机器上的文件
- 需要跑几十步转换的 ETL 链路

这时朴素方案会遇到几个根本问题。

### 问题 1：数据不能都放在 Driver 内存里

普通 Python 列表默认在当前进程里。

但真实 Spark 中，数据通常分布在很多机器上：

```text
Executor 1: 一部分数据
Executor 2: 一部分数据
Executor 3: 一部分数据
...
Driver: 只负责指挥
```

如果所有数据都拉到 Driver 变成一个大列表，Driver 很容易内存溢出。

所以 Spark 需要一个抽象，它不能要求“数据都在本地”。

### 问题 2：每一步都生成中间结果，代价太高

朴素方案会生成：

```text
data
mapped
filtered
```

如果每份数据都很大，中间结果也会很大。

这意味着：

- 占用大量内存
- 产生大量临时数据
- 可能发生磁盘写入
- 计算还没结束，资源已经被中间结果吃光

Spark 需要一个抽象，能够先描述计算，而不是每一步都立刻物化中间结果。

### 问题 3：系统看不到完整计算链路

如果每一步都立刻执行，系统只看到：

```text
先 map
再 filter
再 count
```

但它很难站在全局看：

```text
这整个作业到底从哪里开始？
中间有哪些依赖？
哪些步骤可以合并？
哪些步骤必须等待 Shuffle？
哪些地方可以并行？
失败后从哪里重算？
```

Spark 要做调度和优化，就必须先看到完整的计算描述。

### 问题 4：失败后不知道怎么恢复

假设 `mapped` 这个中间结果丢了。

朴素方案通常只能说：

```text
那就从头再跑吧。
```

但 Spark 希望更聪明：

```text
哪个 Partition 丢了？
它来自哪个父 RDD？
用哪个函数能重新算出来？
能不能只重算丢失的那一份？
```

这就要求数据抽象必须记录“我是怎么来的”。

这个“怎么来的”，后面就叫 Lineage。

## 3. RDD 是怎么被推导出来的

现在我们把问题汇总一下。

Spark 需要一种抽象，它至少要满足这些要求：

```text
1. 不能假设所有数据都在本地。
2. 能把数据拆成多个分区，方便并行处理。
3. Transformation 不急着执行，可以先记录计算计划。
4. 能记录父子依赖，知道自己从哪里来。
5. 失败后可以根据依赖关系重新计算。
6. 对用户来说，还要像集合一样容易使用。
```

这就是 RDD 出现的背景。

所以 RDD 不是“高级列表”。

更准确地说，RDD 是：

```text
一份分布式数据的计算描述。
```

它描述的不只是“数据是什么”，还包括：

- 数据分成几份
- 每份数据怎么计算
- 依赖哪个父 RDD
- 用什么函数转换而来
- 如果丢了怎么重新算

这就是 RDD 的设计思想。

## 4. 为什么 RDD 要不可变

RDD 的一个重要特点是不可变。

我们先反过来想：

> 如果 RDD 可以被随便修改，会发生什么？

假设：

```python
rdd1 = sc.parallelize([1, 2, 3])
rdd2 = rdd1.map(lambda x: x * 10)
```

如果后面 `rdd1` 被修改了：

```text
rdd1 从 [1, 2, 3] 变成 [1, 2, 3, 4]
```

那 `rdd2` 到底应该基于哪个版本的 `rdd1`？

```text
基于修改前的 rdd1？
还是基于修改后的 rdd1？
```

如果这个问题说不清，Lineage 就不可信。

而 Lineage 是容错的基础：

```text
rdd2 的某个 Partition 丢了
  ↓
回到 rdd1
  ↓
重新执行 map
```

如果 `rdd1` 会变，重算出来的结果可能和第一次不一样。

所以 RDD 不可变不是一种编程洁癖，而是为了解决这些问题：

- 依赖关系稳定
- 重算结果可预测
- 多个子 RDD 可以安全共享同一个父 RDD
- Scheduler 可以放心分析 DAG
- 容错机制可以基于 Lineage 工作

Mini Spark 第一阶段把输入数据转成 `tuple`，就是在模拟这种不可变思想。

## 5. 为什么不是直接用 Iterator

你可能会想：

> 如果不想生成中间结果，用 Iterator 不就行了吗？

Iterator 确实能解决一部分问题，比如延迟计算。

但 Iterator 不够表达 Spark 需要的全部信息。

Iterator 通常只知道：

```text
怎么一个接一个地产生数据。
```

但 Spark 还需要知道：

```text
数据分几个 Partition？
这个计算依赖哪个父 RDD？
这是窄依赖还是宽依赖？
失败后从哪里恢复？
要切几个 Stage？
要生成多少 Task？
```

所以 RDD 可以使用 Iterator 做分区内计算，但 RDD 本身不能退化成 Iterator。

RDD 是更高层的执行计划抽象。

## 6. 为什么不是直接用分布式文件

还有一种想法：

> 数据本来就在 HDFS 或对象存储里，直接读文件不就行了吗？

文件只描述“数据在哪里”，但不描述“数据将如何被计算”。

Spark 需要的不只是文件路径，而是完整计算链：

```text
读取文件
  ↓
解析
  ↓
过滤
  ↓
聚合
  ↓
输出
```

RDD 把这些步骤串成可分析、可调度、可重算的链路。

所以 RDD 不是替代文件系统，而是在文件系统之上表达计算过程。

## 7. Mini Spark 当前怎么体现 RDD 思想

现在 Mini Spark 已经比第一阶段复杂了，但你仍然可以从最小入口理解：

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

这段代码看起来很简单，但它已经埋下了后续设计的入口：

```text
普通 Python 数据
  ↓ parallelize
Root RDD
  ↓ Transformation
Derived RDD
  ↓ Action
触发执行
```

当前 `SparkContext.parallelize` 的职责是创建 Root RDD。

现在的 RDD 已经可以记录：

- Partition
- parent
- transform
- operation
- dependency kind
- cache
- lineage

但第一章只需要先理解：

> RDD 是 Spark 用来描述分布式计算的核心抽象，不是普通列表。

## 8. 跑一个最小例子

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc = SparkContext(); rdd = sc.parallelize([1, 2, 3]); print(rdd.collect())"
```

输出：

```text
[1, 2, 3]
```

这个例子只证明一件事：

```text
我们已经有了创建 RDD 和取回结果的最小闭环。
```

它不是完整 Spark，但它是理解后续所有设计的起点。

## 9. 逐行解释代码

### 创建 SparkContext

```python
sc = SparkContext()
```

`SparkContext` 是入口。

在真实 Spark 中，它背后会连接集群、管理配置、申请资源、提交 Job。

在 Mini Spark 中，它先只负责创建 RDD。

### 创建 RDD

```python
rdd = sc.parallelize([1, 2, 3])
```

这行把普通 Python 数据变成 RDD。

重点不是“包装了一层对象”。

重点是：

```text
从这一刻开始，这份数据进入了 Spark 的计算模型。
```

它后面可以继续接：

```python
rdd.map(...)
rdd.filter(...)
rdd.count()
```

这些操作就不再只是普通 Python 列表操作，而是会形成一条可追踪的计算链。

### collect

```python
rdd.collect()
```

`collect()` 是 Action。

它的意思不是“打印”，而是：

```text
请把 RDD 的结果真正计算出来，并收集回 Driver。
```

真实 Spark 中，`collect()` 会把分布在 Executor 上的数据拉回 Driver，所以大数据上要谨慎使用。

## 10. 设计收益与设计代价

### 收益

RDD 设计带来的收益：

- 可以表达分布式数据。
- 可以按 Partition 并行处理。
- 可以用 Transformation 描述计算链。
- 可以用 Lineage 做容错。
- 可以让 Scheduler 分析 DAG。
- 可以把用户 API 和底层执行解耦。

### 代价

RDD 也不是没有代价：

- 对新手来说，比普通列表更抽象。
- Lazy Evaluation 会让“代码写了但没执行”这件事变得不直观。
- 用户需要理解 Action、Transformation、Partition、Shuffle 等概念。
- 如果使用不当，比如乱 `collect()` 或乱 `groupByKey`，仍然会有性能问题。

好的设计通常不是“没有代价”，而是在一组矛盾中做权衡。

RDD 的权衡是：

```text
牺牲一点直观性，
换来分布式执行、容错和调度优化的可能性。
```

## 11. 对照真实 Spark

| 设计问题 | Mini Spark 当前做法 | 真实 Spark 做法 |
| --- | --- | --- |
| 数据太大不能放 Driver | 后续用 Partition 表达多份数据 | 数据分布在 Executor 的 Partition 上 |
| 每步立刻执行代价高 | Transformation 记录 transform | Lazy Evaluation 构建完整 DAG |
| 失败后如何恢复 | Lineage 可重算 | 根据 RDD dependency 重新提交 Task |
| 如何并行 | 本地 Scheduler 模拟 Task | DAGScheduler + TaskScheduler + Executor |
| 如何跨分区聚合 | 简化 Shuffle | Shuffle Write / Shuffle Read / Map-side Combine |

Mini Spark 不是为了复刻 Spark 所有工程细节，而是把 Spark 的设计思想拆成能理解的小块。

## 12. 常见误解

### 误解 1：RDD 就是分布式列表

这个说法太浅。

RDD 看起来像集合，但它更重要的是记录计算关系。

更准确地说：

```text
RDD 是分布式数据的计算描述。
```

### 误解 2：RDD 只是老 API，学 DataFrame 就不用懂了

不对。

现在很多 Spark 开发确实更多使用 DataFrame 和 Spark SQL，但底层的很多概念仍然绕不开：

- Partition
- Shuffle
- Stage
- Task
- Cache
- Fault Tolerance

理解 RDD，是理解 Spark 执行模型的地基。

### 误解 3：RDD 不可变只是函数式编程风格

不只是风格。

不可变是为了让依赖关系稳定，让 Lineage 和重算可信。

## 13. 本章掌握标准

如果你能讲清楚下面这些话，就算这一章过关：

- 普通列表方案在大数据、分布式、容错场景下不够用。
- RDD 不是普通数据容器，而是分布式计算描述。
- RDD 需要记录 Partition、父依赖、转换逻辑和血缘关系。
- RDD 不可变是为了让依赖和重算稳定可靠。
- Lazy Evaluation、Lineage、Partition、Scheduler 都是围绕 RDD 这个抽象展开的。

## 14. 思考题

1. 如果每个 Transformation 都立刻生成中间结果，大数据场景会有什么问题？
2. 为什么 RDD 不能只是一个 Iterator？
3. 为什么 RDD 不可变对容错很重要？
4. 为什么 Spark 需要知道完整计算链路？
5. 你现在怎么用自己的话解释 RDD，而不是背定义？
