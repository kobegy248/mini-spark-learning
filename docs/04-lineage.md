# 04 - Lineage：RDD 怎么记住自己从哪里来

## 0. 这一章先学什么，不学什么

这一章学习 Lineage，也就是 RDD 的“血缘关系”。

我们已经有：

- Root RDD：由 `parallelize` 创建。
- Derived RDD：由 `map`、`filter`、`flat_map` 创建。
- Action：由 `collect`、`count`、`take` 等触发执行。

这一章先不学 Partition、Scheduler、DAG、Stage。我们只回答一个问题：

> 一个 RDD 怎么知道自己是从哪个 RDD 变来的？

如果你觉得父子关系抽象，可以先打开：

[Lineage 血缘可视化](visualizations/04-lineage.html)

## 1. 用人话理解 Lineage

Lineage 可以理解成“数据的来历记录”。

比如：

```python
rdd = (
    sc.parallelize([1, 2, 3, 4])
    .map(lambda value: value * 2)
    .filter(lambda value: value > 4)
)
```

这条链可以读成：

```text
filter RDD
  来自 map RDD
    来自 parallelize RDD
```

也就是说，最后的 RDD 不是凭空出现的。它知道自己的父 RDD，也知道自己是通过什么操作变来的。

## 2. 最小示例：查看 lineage

```python
from mini_spark import SparkContext

sc = SparkContext()
rdd = sc.parallelize([1, 2, 3]).map(lambda value: value * 10).filter(
    lambda value: value > 10
)

print(rdd.lineage())
print(rdd.to_debug_string())
```

输出：

```text
['parallelize', 'map', 'filter']
filter
  map
    parallelize
```

`lineage()` 适合程序读取，`to_debug_string()` 适合人阅读。

## 3. 逐行解释代码

```python
source = sc.parallelize([1, 2, 3])
```

创建 Root RDD：

```text
operation = parallelize
parent = None
```

```python
mapped = source.map(lambda value: value * 10)
```

创建 Derived RDD：

```text
operation = map
parent = source
dependency = narrow
```

```python
filtered = mapped.filter(lambda value: value > 10)
```

继续创建 Derived RDD：

```text
operation = filter
parent = mapped
dependency = narrow
```

## 4. Mini Spark 内部发生了什么

我们新增了一个 `Dependency`：

```python
@dataclass(frozen=True)
class Dependency:
    parent: "RDD[object]"
    kind: str
```

当前只有一种依赖：

```text
narrow
```

也就是窄依赖。你可以先理解成：

> 子 RDD 的一个结果，只依赖父 RDD 中对应的一小部分数据。

`map`、`filter`、`flat_map` 在当前阶段都先算窄依赖。

每个 RDD 现在会记录：

- `_operation`：这个 RDD 是怎么来的，比如 `parallelize`、`map`、`filter`。
- `_parent`：它的父 RDD。
- `_dependency_kind`：它和父 RDD 的依赖类型。

## 5. 对照源码：我们写了什么

`dependencies()`：

```python
def dependencies(self) -> list[Dependency]:
    if self._parent is None:
        return []
    return [Dependency(parent=self._parent, kind=self._dependency_kind)]
```

Root RDD 没有父亲，所以依赖为空。

Derived RDD 有父亲，所以返回一个 Dependency。

`lineage()`：

```python
def lineage(self) -> list[str]:
    if self._parent is None:
        return [self._operation]
    return [*self._parent.lineage(), self._operation]
```

人话解释：

```text
如果我是 Root RDD：
    我的 lineage 只有我自己

如果我是 Derived RDD：
    我的 lineage = 父 RDD 的 lineage + 我自己的操作
```

## 6. 对照真实 Spark

真实 Spark 的 RDD 也会记录依赖关系。

| Mini Spark | 真实 Spark |
| --- | --- |
| `_parent` | 父 RDD |
| `Dependency(kind="narrow")` | `NarrowDependency` |
| `lineage()` | RDD 血缘链 |
| `to_debug_string()` | PySpark 的 `toDebugString()` 类似概念 |

真实 Spark 会利用 Lineage 做容错：如果某个分区数据丢了，可以沿着 Lineage 从父 RDD 重新计算。

## 7. 亲手实验

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc = SparkContext(); rdd = sc.parallelize([1, 2, 3]).map(lambda x: x * 10).filter(lambda x: x > 10); print(rdd.lineage()); print(rdd.to_debug_string())"
```

你会看到：

```text
['parallelize', 'map', 'filter']
filter
  map
    parallelize
```

## 8. 常见误解

### 误解 1：Lineage 就是数据本身

不是。Lineage 是“怎么得到数据”的记录，不是数据本身。

### 误解 2：Lineage 只有调试用

不只是调试。真实 Spark 会用 Lineage 做容错重算。

### 误解 3：所有依赖都一样

不是。后面我们会学习窄依赖和宽依赖。Shuffle 会引入宽依赖，也是 Stage 划分的关键。

## 9. 本章掌握标准

如果你能讲清楚下面这些话，就算这一章过关：

- Derived RDD 会记录自己的 parent。
- Lineage 是 RDD 的来历链。
- 当前 `map`、`filter`、`flat_map` 都是窄依赖。
- Lineage 是后续容错、DAG、Stage 的基础。

## 10. 思考题

1. 为什么 RDD 需要记录 parent？
2. `lineage()` 和 `collect()` 的区别是什么？
3. 为什么 Root RDD 没有依赖？
4. Lineage 为什么能帮助容错？
5. 后面 Shuffle 为什么会让依赖关系变复杂？
