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

## 1. 先从最朴素的方案开始

上一章我们让 Transformation 变成了“只记计划不干活”。这立刻引出一个新问题：

> 如果所有操作都只记计划，那计算到底什么时候才发生？

最朴素的一种回答是：

```text
不需要专门区分什么操作，
每写一个 RDD 就顺手把它算出来缓存住，下次要用直接拿。
```

也就是说，朴素方案想让“记录计划”和“执行计算”同时发生，甚至每一步都提前算好备着。

听起来挺省心，但这正是上一章我们刚否决掉的“立即执行”模型的变体。

## 2. 朴素方案会遇到什么问题

### 问题 1：很多计算根本没必要发生

```python
rdd = sc.parallelize(huge_data).map(很贵的转换)
# 用户写到这里就走了，根本没要结果
```

如果每一步都提前算好，那这段代码会白白跑掉一次昂贵的转换，而用户根本不需要结果。在大数据场景下，这是巨大的浪费。

### 问题 2：不知道“算到什么程度算够”

`count()` 只要个数，`first()` 只要第一个元素，`take(2)` 只要前两个。

如果朴素方案每次都把整条链完整算出来，那 `first()` 也会被迫算完全部数据——明明拿到第一个就可以停了。

### 问题 3：和 Lazy 的目标自相矛盾

上一章我们辛苦把 Transformation 改成懒的，就是为了“先看清全局再动手”。如果又顺手提前算，那 Lazy 就名存实亡了。

## 3. Spark 为什么需要 Action 这个开关

Spark 的选择是：**把“记计划”和“要结果”彻底分开**。

- Transformation 只记计划，永远不触发计算。
- 只有用户明确表示“我要结果了”的操作，才触发计算。这种操作就叫 **Action**。

这样一来：

```text
写 Transformation  → 零成本，只是建对象
调用 Action         → 这一刻才真正把整条链路跑起来
```

而且 Action 还能告诉系统“我要的是什么程度的结果”：

- `count()`：只要个数，可以不把数据收集全。
- `first()` / `take(n)`：只要前几个，可以提前停止。
- `collect()`：要全部，必须算完整条链。
- `reduce(f)`：要一个聚合值，可以边算边合并。

所以 Action 不只是“开关”，它还携带了“要多少结果”的信息，让 Spark 有机会少算一点。

## 4. 用人话理解 Action

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

延续上一章的餐厅类比：Transformation 是点菜，Action 是喊一声“上菜”。厨房只有听到“上菜”才会开火。

## 5. 最小示例：count

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

## 6. 逐行解释 count 示例

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

## 7. 新增 Action

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

`reduce` 不局限于加法。换成乘法、取最大值、拼接字符串都行——合并规则由你传入的函数决定。

## 8. 这个设计的收益和代价

### 收益

- **按需计算**：没人要结果就不算，避免无意义的大规模计算。
- **按量计算**：`first` / `take` 可以提前停止，不必算完整条链。
- **职责清晰**：Transformation 负责“描述”，Action 负责“触发”，两者解耦。
- **可全局优化**：Action 触发时，Spark 看到的是完整计划，可以一次性规划 Stage、并行度。

### 代价

- **延迟暴露问题**：Transformation 里写错的代码，可能直到 Action 才报错，排查更难。
- **心智负担**：新手要记住“哪些会触发、哪些不会”，否则容易写出“以为算了其实没算”的代码。
- **重复计算风险**：同一 RDD 多次 Action 会重复计算（要等第 09 章的 Cache 才能解决）。

## 9. Mini Spark 内部发生了什么

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

注意这里利用了生成器的惰性：`for` 一拿到第一个值就 `return`，后面的数据根本不会被拉取。这就是 `first` 能“提前停止”的原理。

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

`islice` 同样利用了生成器：拿够 n 个就停。

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

为什么空 RDD 要报错？因为 `reduce` 必须有一个初始值，而空 RDD 提供不了。这和 Python 内置 `reduce` 在空序列上需要 `initializer` 是同一个道理。

## 10. 对照真实 Spark：真实世界复杂在哪里

| Mini Spark | 真实 Spark |
| --- | --- |
| `count()` 调用 `_compute()` 并计数 | `RDD.count()` 触发 Job，统计各分区元素数量再汇总 |
| `first()` 取第一个元素 | `RDD.first()` 通常只需要计算足够找到第一个元素的分区 |
| `take(n)` 取前 n 个元素 | `RDD.take(n)` 会尽量少计算分区，但仍可能触发多个 Task |
| `reduce()` 本地顺序合并 | 真实 Spark 会先在分区内 reduce（map-side combine），再跨分区合并 |
| 所有 Action 直接调用 `_compute()` | Action → Job → Stage → Task → Executor，结果返回 Driver |

当前 Mini Spark 还没有 Partition，所以所有 Action 都是在本地单进程里完成，`first`/`take` 的“提前停止”也只是单线程生成器的提前停止。

真实 Spark 的复杂之处在于：它的“提前停止”是跨分区的——`take(n)` 要在多个分区上调度 Task，拿到够数就取消剩余 Task，这涉及分布式取消和结果回传。Mini Spark 完全没有这层。

## 11. 亲手实验

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

### 实验 3：first 提前停止

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc = SparkContext(); calls = []; rdd = sc.parallelize([1, 2, 3, 4]).map(lambda v: calls.append(v) or v * 10); print('first:', rdd.first()); print('calls:', calls)"
```

预期输出：

```text
first: 10
calls: [1]
```

`first()` 只拉取了第一个元素，后面三个根本没被 `map` 处理。这就是 Action“按量计算”的体现。

## 12. 常见误解

### 误解 1：所有操作都会触发计算

不是。Transformation 不触发计算，Action 才触发计算。

### 误解 2：count 不需要真的计算

在 Spark 里，`count()` 通常也需要执行计算。因为 Spark 必须知道 Transformation 之后到底有多少条数据——它没法只看计划就数出个数（除非有专门的元信息优化）。

### 误解 3：reduce 就是 Python 的 sum

不是。`sum` 是加法求和。`reduce` 是用你提供的函数，把多个元素合并成一个结果。这个函数可以是加法，也可以是乘法、取最大值、拼接字符串等。

### 误解 4：Action 越多越好，反正能拿到结果

不对。每个 Action 都会触发一次完整计算（在没有 Cache 时）。频繁 Action 会导致重复计算，代价很高。

### 误解 5：first 和 take 也会算完全部数据

在 Mini Spark 里不会。它们借助生成器提前停止。真实 Spark 里也会尽量少算分区——虽然分布式下“只算一个分区”比单机复杂得多。

## 13. 本章掌握标准

如果你能讲清楚下面这些话，就算这一章过关：

- 为什么不能让 Transformation 顺手就算：会浪费、没法按量计算、和 Lazy 矛盾。
- Action 是唯一触发计算的开关，并且携带“要多少结果”的信息。
- `count`、`first`、`take`、`reduce` 都是 Action。
- `first`/`take` 能提前停止，靠的是生成器的惰性。
- 当前 Mini Spark 的 Action 直接调用 `_compute()`；真实 Spark 的 Action 会触发 Job → Stage → Task。

## 14. 思考题

1. 如果 Transformation 也顺手把结果算出来备着，会撞上哪些问题？
2. 为什么 `count()` 也是 Action，而不是普通属性？
3. `first()` 为什么能在拿到第一个元素后就停下？这依赖什么机制？
4. `take(2)` 和 `collect()` 最大区别是什么？哪种更省计算？
5. `reduce()` 为什么不能直接在空 RDD 上运行？真实 Spark 是怎么处理分区内 + 跨分区两次 reduce 的？
