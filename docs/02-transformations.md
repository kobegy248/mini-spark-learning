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

## 1. 用人话理解 Transformation

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

## 2. 一个生活类比：点菜和上菜

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

## 3. 最小示例：map

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

## 4. 逐行解释 map 示例

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

## 5. 用实验确认 map 是懒执行

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

## 6. filter：过滤数据

`filter` 用来保留满足条件的数据。

代码：

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

但还是一样：`filter` 本身不执行，它只返回一个新的 RDD。

真正过滤发生在：

```python
even.collect()
```

## 7. flat_map：一个元素变多个元素

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

## 8. 链式调用：多个 Transformation 连起来

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

## 9. Mini Spark 内部怎么实现 Lazy Evaluation

现在的 [mini_spark/rdd.py](../mini_spark/rdd.py) 里，RDD 有两种形态。

### 9.1 Root RDD

Root RDD 有自己的数据：

```python
self._data = tuple(data) if data is not None else None
```

比如：

```python
sc.parallelize([1, 2, 3])
```

会得到：

```text
RDD
  _data = (1, 2, 3)
  _parent = None
  _transform = None
```

### 9.2 Derived RDD

Derived RDD 没有自己的数据。

它记录两个东西：

```python
self._parent = parent
self._transform = transform
```

比如：

```python
rdd.map(lambda value: value * 10)
```

会得到：

```text
RDD
  _data = None
  _parent = 上一个 RDD
  _transform = 每个元素乘以 10
```

这就是 Lazy Evaluation 的核心：

> 新 RDD 不急着保存结果，只保存“父 RDD + 转换函数”。

### 9.3 collect 如何触发计算

`collect()` 调用：

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

## 10. 为什么 Transformation 返回新的 RDD

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

如果 `map` 直接修改原来的 RDD，就很难知道数据是怎么一步步来的。

## 11. 对照真实 Spark

| Mini Spark | 真实 Spark |
| --- | --- |
| `map` 返回新 RDD | `RDD.map` 也返回新 RDD |
| `filter` 返回新 RDD | `RDD.filter` 也返回新 RDD |
| `flat_map` 返回新 RDD | PySpark 中叫 `flatMap` |
| `_parent` 记录父 RDD | 真实 Spark 有 Dependency |
| `_transform` 记录转换函数 | 真实 Spark 会记录每个分区上的计算逻辑 |
| `collect()` 调用 `_compute()` | Action 触发 Job 执行 |

当前 Mini Spark 很简化：

- 没有分区。
- 没有任务调度。
- 没有 Executor。
- 没有 Stage。
- 没有 Shuffle。

但它已经抓住了 Spark 的一个核心思想：

> Transformation 只描述计算，Action 才触发计算。

## 12. PySpark 实战提醒

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

## 13. 常见误解

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

## 14. 本章掌握标准

如果你能讲清楚下面这些话，就算这一章过关：

- `map`、`filter`、`flat_map` 都是 Transformation。
- Transformation 不立即执行。
- Transformation 返回新的 RDD，不修改原 RDD。
- 新 RDD 会记录 parent 和 transform。
- `collect()` 是 Action，会触发整条链路计算。
- Lazy Evaluation 是 Spark 构建 Lineage、DAG 和执行计划的基础。

## 15. 思考题

1. 为什么 `map` 不应该直接修改原来的 RDD？
2. 为什么 Transformation 不立即执行有利于 Spark 优化？
3. `map` 和 `flat_map` 的核心区别是什么？
4. 如果同一个 RDD 调用两次 `collect()`，当前 Mini Spark 会发生什么？
5. Lazy Evaluation 和 Lineage 之间有什么关系？
