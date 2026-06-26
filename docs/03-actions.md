# 03 - Action：真正触发计算的开关

## 0. 这一章先学什么，不学什么

这一章学习 Action。

前一章我们已经知道，`map`、`filter`、`flat_map` 这些 Transformation 只是记录计划，不会马上执行。这一章新增四个 Action：

- `count`
- `first`
- `take`
- `reduce`

这一章先不学 Scheduler、Partition、DAG、Stage。我们只关注一个问题：

> 什么操作会让 Mini Spark 真的开始计算？

答案是：Action。

如果你觉得 Action 触发执行这个过程抽象，可以先打开这个可视化页面：

[Action 触发执行可视化](visualizations/03-actions.html)

## 1. 用人话理解 Action

Transformation 像是在写计划。

Action 像是在说：

> 计划写好了，现在给我结果。

比如：

```python
rdd = sc.parallelize([1, 2, 3]).map(lambda value: value * 10)
```

这行只是记录计划：

```text
以后需要结果时，把每个 value 乘以 10。
```

当你调用：

```python
rdd.count()
```

Mini Spark 才会真的执行 `map`，然后数有多少个结果。

## 2. 最小示例：count

```python
from mini_spark import SparkContext

sc = SparkContext()
rdd = sc.parallelize([1, 2, 3]).map(lambda value: value * 10)

print(rdd.count())
```

输出：

```text
3
```

`count()` 不返回数据本身，而是返回元素个数。

## 3. 逐行解释代码

```python
rdd = sc.parallelize([1, 2, 3]).map(lambda value: value * 10)
```

这一行创建了一条 RDD 链：

```text
Root RDD: [1, 2, 3]
  ↓
map RDD: 每个元素乘以 10
```

此时还没有执行。

```python
rdd.count()
```

这一行是 Action。它会触发 `_compute()`，让数据真的流过 `map`。

计算过程是：

```text
[1, 2, 3]
  ↓ map
[10, 20, 30]
  ↓ count
3
```

## 4. 新增 Action

### count

`count()` 返回 RDD 中有多少个元素。

```python
sc.parallelize([1, 2, 3]).count()
```

结果：

```text
3
```

### first

`first()` 返回第一个元素。

```python
sc.parallelize([10, 20, 30]).first()
```

结果：

```text
10
```

如果 RDD 是空的，`first()` 会报错：

```text
ValueError: first called on empty RDD
```

### take

`take(n)` 返回前 n 个元素。

```python
sc.parallelize([1, 2, 3, 4]).take(2)
```

结果：

```text
[1, 2]
```

### reduce

`reduce(function)` 会把多个元素合并成一个结果。

```python
sc.parallelize([1, 2, 3, 4]).reduce(lambda left, right: left + right)
```

结果：

```text
10
```

可以想象成：

```text
(((1 + 2) + 3) + 4) = 10
```

## 5. Mini Spark 内部发生了什么

所有 Action 都会调用 `_compute()`。

比如 `count()`：

```python
def count(self) -> int:
    return sum(1 for _ in self._compute())
```

人话解释：

```text
请先把 RDD 真正算出来，
然后每看到一个元素，就计数加 1。
```

`first()`：

```python
def first(self) -> T:
    for value in self._compute():
        return value
    raise ValueError("first called on empty RDD")
```

人话解释：

```text
请先开始计算，
一拿到第一个元素就返回。
如果一个元素都没有，就报错。
```

`take(n)`：

```python
def take(self, count: int) -> list[T]:
    if count < 0:
        raise ValueError("take count must be non-negative")
    return list(islice(self._compute(), count))
```

人话解释：

```text
请开始计算，
但最多只拿前 n 个元素。
```

`reduce()`：

```python
def reduce(self, function: Callable[[T, T], T]) -> T:
    iterator = iter(self._compute())
    try:
        result = next(iterator)
    except StopIteration as exc:
        raise ValueError("reduce called on empty RDD") from exc

    for value in iterator:
        result = function(result, value)
    return result
```

人话解释：

```text
先拿第一个元素当初始结果，
然后把后面的元素一个个合并进去。
```

## 6. 对照真实 Spark

| Mini Spark | 真实 Spark |
| --- | --- |
| `count()` 调用 `_compute()` 并计数 | `RDD.count()` 触发 Job，统计各分区元素数量再汇总 |
| `first()` 取第一个元素 | `RDD.first()` 通常只需要计算足够找到第一个元素的分区 |
| `take(n)` 取前 n 个元素 | `RDD.take(n)` 会尽量少计算分区，但仍可能触发多个 Task |
| `reduce()` 本地顺序合并 | 真实 Spark 会先在分区内 reduce，再跨分区合并 |

当前 Mini Spark 还没有 Partition，所以所有 Action 都是在本地单进程里完成。

真实 Spark 的 Action 会触发：

```text
Action
  ↓
Job
  ↓
Stage
  ↓
Task
  ↓
Executor 执行
  ↓
结果返回 Driver
```

我们后面会逐步实现这些概念。

## 7. 亲手实验

### 实验 1：count 会触发 map

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc = SparkContext(); calls = []; rdd = sc.parallelize([1, 2, 3]).map(lambda value: calls.append(value) or value * 10); print('count 前:', calls); print(rdd.count()); print('count 后:', calls)"
```

你会看到：

```text
count 前: []
3
count 后: [1, 2, 3]
```

这说明 `count()` 触发了 `map` 执行。

### 实验 2：take 只取前几个

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc = SparkContext(); rdd = sc.parallelize([1, 2, 3, 4]).map(lambda value: value * 2); print(rdd.take(2))"
```

你会看到：

```text
[2, 4]
```

## 8. 常见误解

### 误解 1：所有操作都会触发计算

不是。Transformation 不触发计算，Action 才触发计算。

### 误解 2：count 不需要真的计算

在 Spark 里，`count()` 通常也需要执行计算。因为 Spark 必须知道 Transformation 之后到底有多少条数据。

### 误解 3：reduce 就是 Python 的 sum

不是。`sum` 是加法求和。`reduce` 是用你提供的函数，把多个元素合并成一个结果。这个函数可以是加法，也可以是乘法、取最大值、拼接字符串等。

## 9. 本章掌握标准

如果你能讲清楚下面这些话，就算这一章过关：

- `count`、`first`、`take`、`reduce` 都是 Action。
- Action 会触发 RDD 链真正执行。
- Transformation 负责描述计算，Action 负责要结果。
- 当前 Mini Spark 的 Action 直接调用 `_compute()`。
- 真实 Spark 的 Action 会触发 Job，并进一步生成 Stage 和 Task。

## 10. 思考题

1. 为什么 `count()` 也是 Action，而不是普通属性？
2. 为什么 `first()` 遇到空 RDD 应该报错？
3. `take(2)` 和 `collect()` 最大区别是什么？
4. `reduce()` 为什么不能直接在空 RDD 上运行？
5. 为什么真实 Spark 的 Action 会触发 Job？
