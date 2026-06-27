# 04 - Lineage：RDD 怎么记住自己从哪里来

## 0. 这一章先学什么，不学什么

这一章学习 Lineage，也就是 RDD 的“血缘关系”。

我们已经有：

- Root RDD：由 `parallelize` 创建。
- Derived RDD：由 `map`、`filter`、`flat_map` 创建。
- Action：由 `collect`、`count`、`take` 等触发执行。

这一章先不学 Partition、Scheduler、DAG、Stage。我们只回答一个问题：

> 一个 RDD 怎么知道自己是从哪个 RDD 变来的？这件事为什么重要？

如果你觉得父子关系抽象，可以先打开：

[Lineage 血缘可视化](visualizations/04-lineage.html)

## 1. 先从最朴素的方案开始

前面我们让 Derived RDD 记住 `_parent` 和 `_transform`。这已经能算出结果了。

那朴素方案会怎么处理“数据的来历”？

最朴素的想法是：

```text
没必要专门记录来历。
算结果的时候，把中间结果都存下来不就行了？
要查某个 RDD 的数据，直接看它存的结果。
丢了？那就从头再算。
```

也就是说，朴素方案倾向于“**存结果，不存来历**”——结果是最直接的，来历是多余的元信息。

## 2. 朴素方案会遇到什么问题

### 问题 1：存结果太贵

如果每个 Derived RDD 都把自己算完的结果完整存下来，数据量大时内存根本扛不住。上一章我们好不容易用生成器把中间结果省掉了，现在又存回来，等于白干。

### 问题 2：丢了只能从头算

朴素方案不记录“我是从哪一步变来的”，所以某个中间结果一旦丢失，系统只知道“结果没了”，不知道“它由谁、用什么函数可以重新生成”。唯一的办法是从 Root 重跑整条链。

但如果链路很长，只有一个 Partition 丢了，却要重跑全部，太浪费。

### 问题 3：没法做全局分析

如果不记录父子关系，系统就没法回答：

```text
这条链有多少步？
哪些步骤可以合并？
哪一步是 Shuffle 边界（后面会讲）？
```

而这些正是后面 DAG、Stage、调度要用的信息。

## 3. Spark 为什么需要 Lineage

Spark 的洞察是：

> 与其存“结果”，不如存“配方”。

配方（怎么从父 RDD 算出自己）非常轻量——就是一个父指针 + 一个转换函数。而结果可能很大。

只要配方在：

- 要结果时，按配方现算（Lazy，省存储）。
- 结果丢了，按配方重算（容错）。
- 要分析链路时，沿配方往回走（DAG / Stage）。

这个“配方链”，就是 **Lineage（血缘）**。

所以 Lineage 不是为了好看，而是 Spark 在“存储贵、要容错、要调度”这三件事下的关键设计：用轻量的来历记录，替代昂贵的中间结果物化。

## 4. 这个设计的核心思想

Lineage 的核心是：**每个 Derived RDD 都记住“我是从哪个父 RDD、经过什么操作变来的”**。

可以想象成一张向后指的链表：

```text
filter RDD
  ↑ 我的父亲是
map RDD
  ↑ 我的父亲是
parallelize RDD（Root，没有父亲）
```

注意方向：RDD 是不可变的，父 RDD 一旦创建就不会变，所以这条链是**稳定、可信**的。这也是第 01 章强调“RDD 不可变”的原因之一——不可变才让 Lineage 可信。

## 5. 这个设计解决了什么问题

- **省存储**：只存配方，不存中间结果。
- **可容错**：丢了一个 Partition，沿 Lineage 找到父 RDD 和 transform，重新算这一个就行（不必整条链重跑）。
- **可分析**：沿 Lineage 往回走，就能得到完整计算图，为 DAG、Stage、调度提供输入。
- **可复用**：同一个父 RDD 可以被多个子 RDD 指向，Lineage 自然形成 DAG（不再是线性链）。

## 6. 这个设计付出了什么代价

- **重算有成本**：既然不存结果，每次 Action 都要现算；丢了也要重算。Lineage 越长、转换越贵，重算越痛。（这正是后面 Cache 章节要缓解的。）
- **宽依赖重算更贵**：到 Shuffle 章节会发现，宽依赖下一个子 Partition 依赖多个父 Partition，重算范围会变大。
- **内存里要一直挂着这条链**：RDD 对象不能随便回收，否则 Lineage 断了就没法重算。

## 7. Mini Spark 当前如何实现

Mini Spark 给每个 RDD 增加了“来历记录”字段：

- `_operation`：这个 RDD 是怎么来的，比如 `parallelize`、`map`、`filter`。
- `_parent`：它的父 RDD。
- `_dependency_kind`：它和父 RDD 的依赖类型。

并新增了一个 `Dependency` 概念：

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

`dependencies()`：

```python
def dependencies(self) -> list[Dependency]:
    if self._parent is None:
        return []
    return [Dependency(parent=self._parent, kind=self._dependency_kind)]
```

Root RDD 没有父亲，所以依赖为空。Derived RDD 有父亲，所以返回一个 Dependency。

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

这就是沿 parent 链往回走、把操作名收集起来的过程。

## 8. 最小示例：查看 lineage

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

`lineage()` 返回列表，适合程序读取；`to_debug_string()` 返回缩进树，适合人阅读。

## 9. 逐行解释代码

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

注意：这三步都没有算数据。它们只是把“父亲 + 操作 + 依赖类型”记下来。Lineage 在 Transformation 调用时就建好了，不需要等 Action。

## 10. Mini Spark 内部发生了什么

当调用 `rdd.lineage()` 时：

```text
filter RDD 问自己：我有父亲吗？有，是 map。
  ↓ 去 map RDD
map RDD 问自己：我有父亲吗？有，是 parallelize。
  ↓ 去 parallelize RDD
parallelize RDD：我是 Root，没有父亲。
  ↓ 回溯收集
['parallelize', 'map', 'filter']
```

这就是一次沿 Lineage 的“溯源”。后面 DAG 章节会用同样的方式把整条链整理成执行图。

## 11. 对照真实 Spark：真实世界复杂在哪里

| Mini Spark | 真实 Spark |
| --- | --- |
| `_parent` | 父 RDD |
| `Dependency(kind="narrow")` | `NarrowDependency`（还分 `OneToOneDependency`、`RangeDependency` 等） |
| 只有一种 narrow | 真实 Spark 有 narrow 和 wide 两大类，wide 下还有 `ShuffleDependency` |
| `lineage()` 返回操作名列表 | RDD 血缘链，包含分区级别的依赖映射 |
| `to_debug_string()` | PySpark 的 `toDebugString()`，会显示 RDD 链和缓存级别 |

真实 Spark 的 Lineage 比这里复杂得多：

- 依赖不是简单的“一个父亲”，而是**分区级别**的映射：子分区 3 依赖父分区的哪些分区。
- 宽依赖下一个子分区会依赖**所有**父分区，这正是后面 Stage 切分的依据。
- Lineage 会和缓存级别一起展示在 `toDebugString()` 里。

Mini Spark 只记录了“操作名 + 父 RDD + 依赖类型”，没有分区级映射，这是当前最大的简化。

## 12. 亲手实验

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

再试一个分叉的例子——同一个父 RDD 被两个子 RDD 使用：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc = SparkContext(); src = sc.parallelize([1,2,3]); a = src.map(lambda x: x+1); b = src.filter(lambda x: x>1); print(a.lineage()); print(b.lineage())"
```

预期输出：

```text
['parallelize', 'map']
['parallelize', 'filter']
```

两条 Lineage 共享同一个 Root，这正是 DAG（而非线性链）的雏形。

## 13. 常见误解

### 误解 1：Lineage 就是数据本身

不是。Lineage 是“怎么得到数据”的配方记录，不是数据本身。要数据还得现算。

### 误解 2：Lineage 只有调试用

不只是调试。真实 Spark 会用 Lineage 做容错重算，还会用它划分 Stage、规划调度。Lineage 是执行引擎的输入，不只是给人看的。

### 误解 3：所有依赖都一样

不是。后面我们会学习窄依赖和宽依赖。Shuffle 会引入宽依赖，它让重算范围和 Stage 划分都变复杂。

### 误解 4：Lineage 是 Action 时才建的

不是。Lineage 在每个 Transformation 调用时就已经建好了（记下了 parent 和 operation）。Action 只是触发“沿 Lineage 现算”，而不是“建 Lineage”。

## 14. 本章掌握标准

如果你能讲清楚下面这些话，就算这一章过关：

- 朴素方案“存结果不存来历”在存储、容错、调度三方面都撑不住。
- Lineage 是用轻量的“配方链”替代昂贵的中间结果物化。
- Derived RDD 会记录 parent、operation、dependency_kind。
- 当前 `map`、`filter`、`flat_map` 都是窄依赖。
- Lineage 是后续容错、DAG、Stage 的共同基础。

## 15. 思考题

1. 如果只存结果、不记 Lineage，丢了一个中间结果会怎样？为什么“从头重跑”很浪费？
2. `lineage()` 和 `collect()` 的区别是什么？哪个会触发计算，哪个不会？
3. 为什么 Root RDD 没有依赖？如果它也有“父亲”，会带来什么问题？
4. 为什么说 RDD 不可变是 Lineage 可信的前提？
5. 后面 Shuffle 引入宽依赖后，沿 Lineage 重算一个 Partition 的范围会怎么变？
