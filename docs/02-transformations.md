# 02 - Transformation 与 Lazy Evaluation：为什么 map 不会马上执行

## 0. 这一章先学什么，不学什么

这一章我们学习 Spark 里非常重要的一个思想：Transformation 是懒执行的。

本章新增三个方法：

- `map`
- `filter`
- `flat_map`

这一章先不学：

- Partition
- Scheduler
- DAG
- Stage
- Shuffle
- Cache

我们只解决一个核心问题：

> 为什么 `rdd.map(...)` 调用之后，并不会立刻处理数据？

这是 Spark 入门最重要的坎之一。很多新手第一次用 Spark 时都会疑惑：

> 我明明写了 `map` 和 `filter`，为什么 Spark UI 里没有 Job？

答案就是：Transformation 不触发执行，Action 才触发执行。

如果你觉得文字抽象，可以先打开这个可视化页面：

[Lazy Evaluation 可视化](visualizations/02-lazy-evaluation.html)

它会用“下一步”的方式展示：`map`、`filter`、`flat_map` 如何先创建 RDD 链，直到 `collect()` 才真正执行。

## 1. 先从最朴素的方案开始

假设我们要对一份数据做几步转换：

```python
data = [1, 2, 3, 4]

mapped = [x * 10 for x in data]       # 每个元素乘以 10
filtered = [x for x in mapped if x > 20]  # 保留大于 20 的
```

这是最直觉的写法：**写一行，就立刻算一行，并生成一个中间结果**。

对应到 RDD 上，朴素方案会是这样：

```python
rdd = sc.parallelize([1, 2, 3, 4])
mapped = rdd.map(lambda x: x * 10)     # 朴素想法：这里就把数据全算出来
filtered = mapped.filter(lambda x: x > 20)  # 朴素想法：这里再算一次
```

朴素方案的潜台词是：

```text
map 这一行 = 立刻遍历所有数据，生成一份新数据
filter 这一行 = 再立刻遍历一遍，生成又一份新数据
```

这就是“立即执行（eager）”模型。

## 2. 朴素方案会遇到什么问题

对小数据，立即执行没什么问题。但 Spark 面对的是 TB 级、分布在几十台机器上的数据。立即执行会撞上三堵墙。

### 问题 1：每一步都物化中间结果，太贵

立即执行意味着每一步都要把中间结果完整地存下来：

```text
data      → 100GB
mapped    → 100GB（存下来）
filtered  → 50GB（再存下来）
```

中间结果会吃光内存、撑爆磁盘，而其中很多中间结果后面根本没人再用。

### 问题 2：系统看不到完整链路，没法优化

如果每写一行就立刻执行，系统只能“走一步看一步”。它没法在全局上发现：

```text
map 之后紧跟 filter，能不能把两步合并成一次遍历？
这条链路里哪一步是窄依赖、哪一步要 Shuffle？
能不能先看到整条链，再决定怎么切 Stage？
```

优化需要“先看见全局，再动手”。立即执行剥夺了这个机会。

### 问题 3：重复计算没法避免

如果同一份数据被两次 Action 使用：

```python
rdd = sc.parallelize(...).map(很贵的转换)
print(rdd.count())   # 第一次：算一遍
print(rdd.collect()) # 第二次：又算一遍
```

立即执行模型里，每条链路各算各的，昂贵转换会被重复执行。

### 问题 4：失败后只能从头再来

立即执行丢掉中间结果后，没法回答“这一步是怎么来的”，只能整条链重跑。

## 3. Spark 为什么需要 Lazy Evaluation

把上面四个问题放一起，Spark 需要的是：

```text
1. 先把“要做什么”记录下来，但不立刻做。
2. 等到真正要结果时，再一次性看清整条链路。
3. 看清之后，可以合并步骤、规划 Stage、决定并行度。
4. 同一条链路被复用时，可以缓存（后面章节会讲）。
5. 失败时，可以根据记录的“来历”重新计算。
```

这就是 Transformation 设计成**懒执行**的根本原因。

所以 Spark 把操作分成两类：

- **Transformation**（`map`、`filter`、`flat_map`）：只记录计划，返回一个新 RDD，不动数据。
- **Action**（`collect`、`count`）：才真正触发计算。

这不是 Spark 的任性，而是分布式计算在“数据大、要优化、要容错”这三重压力下的必然选择。

## 4. 用人话理解 Transformation

Transformation 可以先理解成：

> 不是真的干活，而是记录“以后要怎么干活”。

比如你写：

```python
rdd2 = rdd.map(lambda value: value * 10)
```

这句话不是马上把每个数字乘以 10。

它更像是在创建一张新的任务单：

```text
如果以后有人要结果，
请先拿到 parent RDD 的数据，
然后把每个 value 乘以 10。
```

真正开始干活的是 Action，比如：

```python
rdd2.collect()
```

## 5. 一个生活类比：点菜和上菜

你可以把 Spark 程序想象成餐厅点菜：

```python
rdd.map(...)
rdd.filter(...)
rdd.flat_map(...)
```

这些就像你在菜单上写：

```text
我要这道菜。
这道菜不要辣。
这道菜切小份。
```

但厨房还没有开始做。

当你说：

```python
collect()
```

这才像告诉厨房：

```text
现在真的开始做，并把菜端上来。
```

所以：

- Transformation：点菜，记录计划。
- Action：上菜，触发执行。

为什么餐厅要先记菜单、而不是点一道做一道？因为厨师要等菜单齐了，才能合并工序、统筹火力。Spark 也是一样：等所有 Transformation 记录完，再统筹执行。

## 6. 最小示例：map

代码：

```python
from mini_spark import SparkContext

sc = SparkContext()
rdd = sc.parallelize([1, 2, 3])
mapped = rdd.map(lambda value: value * 10)

print(mapped.collect())
```

输出：

```text
[10, 20, 30]
```

看起来好像 `map` 把数据变了。

但更准确地说：

```text
map 创建了一个新的 RDD，
这个新 RDD 记录了“以后要把每个元素乘以 10”。
collect 调用时，才真正执行乘以 10。
```

## 7. 逐行解释 map 示例

### 第 1 步：创建源 RDD

```python
rdd = sc.parallelize([1, 2, 3])
```

这会创建一个 Root RDD。

可以想象成：

```text
Root RDD
  data = (1, 2, 3)
```

Root RDD 是最开始的数据来源。

### 第 2 步：调用 map

```python
mapped = rdd.map(lambda value: value * 10)
```

这会创建一个新的 Derived RDD。

Derived 的意思是“派生出来的”。

可以想象成：

```text
Derived RDD
  parent = Root RDD
  transform = 每个元素乘以 10
```

注意：这里还没有计算。

`mapped` 只是记住：

```text
我的父亲是谁？
我要对父亲的数据做什么转换？
```

### 第 3 步：调用 collect

```python
mapped.collect()
```

这时才开始计算：

```text
collect()
  ↓
发现 mapped 是 Derived RDD
  ↓
先计算它的 parent
  ↓
parent 是 Root RDD，拿到 (1, 2, 3)
  ↓
执行 transform：每个元素乘以 10
  ↓
得到 [10, 20, 30]
```

## 8. 用实验确认 map 是懒执行

我们可以用一个列表 `calls` 记录函数是否真的执行。

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc = SparkContext(); calls = []; rdd = sc.parallelize([1, 2, 3]).map(lambda value: calls.append(value) or value * 10); print('collect 前:', calls); print(rdd.collect()); print('collect 后:', calls)"
```

输出：

```text
collect 前: []
[10, 20, 30]
collect 后: [1, 2, 3]
```

这说明：

- `map(...)` 调用后，`calls` 还是空的。
- `collect()` 调用后，`calls` 变成 `[1, 2, 3]`。

所以 `map` 没有马上执行。真正执行发生在 `collect()`。

这就是 Lazy Evaluation。

## 9. filter 与 flat_map

### filter：过滤数据

`filter` 用来保留满足条件的数据。

```python
from mini_spark import SparkContext

sc = SparkContext()
rdd = sc.parallelize([1, 2, 3, 4])
even = rdd.filter(lambda value: value % 2 == 0)

print(even.collect())
```

输出：

```text
[2, 4]
```

逐步看：

```text
[1, 2, 3, 4]
  ↓ filter: value % 2 == 0
[2, 4]
```

但还是一样：`filter` 本身不执行，它只返回一个新的 RDD。真正过滤发生在 `even.collect()`。

### flat_map：一个元素变多个元素

`flat_map` 比 `map` 多一步“拍平”。

先看普通 `map` 的想象结果：

```python
["ab", "cd"] -> [["a", "b"], ["c", "d"]]
```

`flat_map` 会把里面的小列表拍平：

```python
["ab", "cd"] -> ["a", "b", "c", "d"]
```

代码：

```python
from mini_spark import SparkContext

sc = SparkContext()
rdd = sc.parallelize(["ab", "cd"])
chars = rdd.flat_map(lambda text: list(text))

print(chars.collect())
```

输出：

```text
['a', 'b', 'c', 'd']
```

你可以这样记：

- `map`：一个元素变一个元素。
- `flat_map`：一个元素变多个元素，然后把结果摊平。

## 10. 链式调用：多个 Transformation 连起来

代码：

```python
from mini_spark import SparkContext

sc = SparkContext()
rdd = (
    sc.parallelize([1, 2, 3, 4])
    .map(lambda value: value * 2)
    .filter(lambda value: value > 4)
    .flat_map(lambda value: [value, value + 1])
)

print(rdd.collect())
```

输出：

```text
[6, 7, 8, 9]
```

一步一步看：

```text
原始数据:
[1, 2, 3, 4]

map: 每个元素乘以 2
[2, 4, 6, 8]

filter: 保留大于 4 的元素
[6, 8]

flat_map: 每个元素变成 [value, value + 1]
[6, 7, 8, 9]
```

但注意，这些中间结果不是在每一行代码执行时立刻产生的。

真正执行顺序是：

```text
创建 Root RDD
  ↓
创建 map Derived RDD
  ↓
创建 filter Derived RDD
  ↓
创建 flat_map Derived RDD
  ↓
collect 触发整条链计算
```

## 11. 这个设计的收益和代价

### 收益

- **省中间结果**：数据以生成器（流）的方式被拉取，map 的输出不会完整物化，而是边算边被 filter 消费。
- **可全局优化**：Spark 在 Action 触发时才看到完整链路，可以合并窄依赖、规划 Stage。
- **可复用可缓存**：同一 RDD 被多次使用时可以缓存（第 09 章会讲）。
- **可容错**：因为记录了来历，失败可以重算（第 04、10 章）。

### 代价

- **不直观**：新手会疑惑“我写了 map 为什么没执行”。
- **调试更难**：错误被推迟到 Action 才暴露，报错位置离真正出问题的 Transformation 可能很远。
- **需要用户理解惰性**：否则容易写出“以为算了其实没算”或“重复计算不自知”的代码。

好的设计都是在矛盾里做权衡。Lazy Evaluation 的权衡是：

```text
牺牲一点直观性，
换来省内存、可优化、可容错的可能性。
```

## 12. 第二阶段教学模型：Mini Spark 怎么实现 Lazy Evaluation

为了把 Lazy Evaluation 讲清楚，先看第二阶段的**教学模型**。

这一节展示的是“刚学 Transformation 时”的最小实现，不是最终项目里 [mini_spark/rdd.py](../mini_spark/rdd.py) 的完整形态。最终代码在后续阶段已经加入了 Partition、Scheduler、Shuffle、Cache 和容错；但理解 Lazy Evaluation 时，先用这个小模型更容易看清核心思想。

在第二阶段模型里，RDD 有两种形态。

### 12.1 Root RDD

Root RDD 有自己的数据。第二阶段可以把它想成这样：

```python
self._data = tuple(data) if data is not None else None
```

比如：

```python
sc.parallelize([1, 2, 3])
```

可以理解成：

```text
RDD
  _data = (1, 2, 3)
  _parent = None
  _transform = None
```

### 12.2 Derived RDD

Derived RDD 没有自己的结果数据。

它记录两个东西：

```python
self._parent = parent
self._transform = transform
```

比如：

```python
rdd.map(lambda value: value * 10)
```

可以理解成：

```text
RDD
  _data = None
  _parent = 上一个 RDD
  _transform = 每个元素乘以 10
```

这就是 Lazy Evaluation 的核心：

> 新 RDD 不急着保存结果，只保存“父 RDD + 转换函数”。

而且 `_transform` 返回的是**生成器**，不是列表。生成器的特点是“你要一个我才算一个”，这天然就是惰性的。

### 12.3 collect 如何触发计算

在第二阶段模型里，`collect()` 调用：

```python
def collect(self) -> list[T]:
    return list(self._compute())
```

真正计算在 `_compute()`：

```python
def _compute(self) -> Iterable[T]:
    if self._parent is None:
        if self._data is None:
            raise RuntimeError("Root RDD has no source data")
        return self._data

    if self._transform is None:
        raise RuntimeError("Derived RDD has no transform")

    return self._transform(self._parent._compute())
```

这段可以用人话翻译成：

```text
如果我是 Root RDD：
    直接返回自己的数据

如果我是 Derived RDD：
    先让 parent 算出数据
    再对 parent 的数据应用 transform
```

所以链式 Transformation 最后会从尾部往前找 parent，直到找到 Root RDD，然后再一层层把转换函数应用回来。

## 13. 为什么 Transformation 返回新的 RDD

比如：

```python
source = sc.parallelize([1, 2, 3])
mapped = source.map(lambda value: value + 1)
```

`map` 不会修改 `source`。

你可以验证：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc = SparkContext(); source = sc.parallelize([1, 2, 3]); mapped = source.map(lambda value: value + 1); print(source.collect()); print(mapped.collect())"
```

输出：

```text
[1, 2, 3]
[2, 3, 4]
```

为什么要这样设计？

因为 RDD 是不可变的。

不可变的好处是：

- 原来的 RDD 不会被后续操作破坏。
- 每一步转换都能形成清晰的父子关系。
- 后面做 Lineage 和容错时，可以根据父子关系重新计算。

如果 `map` 直接修改原来的 RDD，就很难知道数据是怎么一步步来的，Lineage 也就不可信了。

## 14. 对照真实 Spark：真实世界复杂在哪里

| Mini Spark | 真实 Spark |
| --- | --- |
| `map` 返回新 RDD | `RDD.map` 也返回新 RDD |
| `filter` 返回新 RDD | `RDD.filter` 也返回新 RDD |
| `flat_map` 返回新 RDD | PySpark 中叫 `flatMap` |
| 第二阶段用 `_parent` 记录父 RDD | 真实 Spark 有 Dependency 体系（窄/宽） |
| 第二阶段用 `_transform` 记录转换函数 | 真实 Spark 记录每个分区上的计算函数（`compute`） |
| 第二阶段里 `collect()` 调用 `_compute()` | Action 触发 Job，走 DAGScheduler → TaskScheduler → Executor |
| 生成器串起来一次拉取 | 窄依赖会用 pipeline 把多个算子融合成一个 Task 执行 |

到第二阶段为止，Mini Spark 很简化：

- 没有分区层面的并行。
- 没有真正的 Task 调度。
- 没有 Stage 切分（还没遇到 Shuffle）。
- 没有 Executor。

但它已经抓住了 Spark 的一个核心思想：

> Transformation 只描述计算，Action 才触发计算。

读到完整项目时要注意：当前最终代码已经把 Root RDD 的 `_data` 演进成 `_partitions`，把 `collect()` / `count()` 接入了 `LocalScheduler`。这些升级没有推翻本章思想，只是把“单条数据流”扩展成了“按分区调度的执行流”。

真实 Spark 比这里复杂的地方主要在于：它会把一连串窄依赖 Transformation **融合（pipeline）成一次遍历**，在同一个 Task 内完成，连生成器的边界都被进一步压榨。Mini Spark 已经用生成器表达了“边算边消费”的雏形，但没有显式的 pipeline 调度。

## 15. PySpark 实战提醒

在 PySpark 中，这段代码不会马上执行：

```python
rdd2 = rdd.map(lambda value: value * 10)
```

这段也不会马上执行：

```python
rdd3 = rdd2.filter(lambda value: value > 10)
```

只有遇到 Action 才执行：

```python
rdd3.collect()
```

所以如果你在 Spark UI 里看不到 Job，不一定是代码没运行，可能只是你还没有触发 Action。

常见 Action 包括：

- `collect()`
- `count()`
- `take(n)`
- `first()`
- `reduce(...)`
- DataFrame 的 `show()`
- 写出数据，比如 `write.parquet(...)`

## 16. 常见误解

### 误解 1：map 会立刻处理数据

不会。

`map` 创建新的 RDD，记录转换函数。Action 才触发执行。

### 误解 2：filter 之后原 RDD 变了

不会。

RDD 是不可变的。`filter` 返回新 RDD，原 RDD 不变。

### 误解 3：flat_map 只是 map 的别名

不是。

`map` 通常是一进一出。

`flat_map` 是一进多出，并把结果拍平。

### 误解 4：Lazy Evaluation 只是为了偷懒

不是。

Lazy Evaluation 让 Spark 可以先看到完整计算链路，再统一规划执行。这是后面 DAG、Stage、优化和容错的基础。

### 误解 5：Transformation 报错会在我写 map 那一行就报

不会。因为 `map` 根本没执行函数体，函数里的错误要等到 Action 触发真正执行时才会暴露。这是惰性带来的调试代价。

## 17. 本章掌握标准

如果你能讲清楚下面这些话，就算这一章过关：

- 立即执行（朴素方案）在“中间结果大、要全局优化、要容错”这三点上撑不住。
- Transformation 不立即执行，是为了让 Spark 先记录计划、看清全局再动手。
- `map`、`filter`、`flat_map` 都是 Transformation，返回新 RDD，不改原 RDD。
- 新 RDD 会记录 parent 和 transform，惰性来自生成器。
- `collect()` 是 Action，会触发整条链路计算。
- Lazy Evaluation 是 Spark 构建 Lineage、DAG 和执行计划的基础。

## 18. 思考题

1. 如果每个 Transformation 都立即执行并物化中间结果，大数据场景会撞上哪几堵墙？
2. 为什么把 map 和 filter 都设计成“返回新 RDD”而不是“原地修改”？
3. `map` 和 `flat_map` 的核心区别是什么？举一个 `map` 做不了、必须用 `flat_map` 的例子。
4. 如果同一个 RDD 调用两次 `collect()`，当前 Mini Spark 会发生什么？为什么？（提示：还没有 Cache）
5. Lazy Evaluation 和后面要讲的 Lineage、DAG 之间是什么关系？为什么说 Lazy 是它们的前提？
