# 07 - DAG 与 Stage：Spark 怎么看完整计算图

## 0. 这一章先学什么，不学什么

这一章学习 DAG 和 Stage。

前面我们已经知道 RDD 会形成 Lineage。DAG 可以理解成：

> 把 RDD 血缘链整理成一张执行图。

Stage 可以理解成：

> 在执行图上按依赖类型切出来的一段执行区域。

这一章先不实现 Shuffle 的真正搬运，只先支持 Stage 划分规则：窄依赖放在同一个 Stage，宽依赖会切开新的 Stage。

可视化页面：

[DAG 与 Stage 可视化](visualizations/07-dag.html)

## 1. 先从最朴素的方案开始

我们已经有了 Lineage，也有了 Scheduler。朴素方案会怎么执行一条 RDD 链？

```text
沿 Lineage 从尾巴走到头，
每遇到一个 RDD 就单独算一步、存下来，
再交给下一步。
```

也就是说，朴素方案是：**每个 RDD 各成一个 Stage，各算各的，中间结果全部物化**。

```text
parallelize  → 算出来，存
   ↓
map          → 读上一步，算出来，存
   ↓
filter       → 读上一步，算出来，存
```

这能跑，但对窄依赖来说极其浪费。

## 2. 朴素方案会遇到什么问题

### 问题 1：窄依赖没必要存中间结果

`map`、`filter` 这种窄依赖，子 Partition i 只看父 Partition i。这种情况下完全可以“边读边算、一气呵成”，根本不用把 `map` 的结果完整存下来再交给 `filter`。朴素方案却把每一步都物化，白白浪费内存和磁盘。

### 问题 2：没法决定“哪些步骤可以塞进一个 Task”

如果每个 RDD 各成一个 Stage，那每个 Stage 一个 Task，Task 之间要靠物化中间结果串联。但窄依赖其实可以**融合（pipeline）**成一个 Task：一个 Task 里先把数据 map、再 filter、再 flat_map，一次遍历搞定。朴素方案看不到这个机会。

### 问题 3：遇到 Shuffle 不知道该在哪断

有些操作（后面的 `group_by_key`）必须把所有分区的数据重新打乱。这种“宽依赖”是天然的断点——断点之前可以 pipeline，断点之后必须等所有上游分区写完才能开始读。朴素方案没有“依赖类型”的概念，不知道该在哪断。

## 3. Spark 为什么需要 DAG 和 Stage

Spark 的洞察是：**先从 Lineage 构造一张全局计算图（DAG），再按“依赖类型”把它切成若干段（Stage）**。

- **DAG**：把 RDD 的父子关系整理成一张有向无环图，让系统“看见全局”。
- **Stage**：在 DAG 上按宽依赖边界切开。同一个 Stage 内全是窄依赖，可以 pipeline 成一次遍历；Stage 之间是宽依赖，必须物化（写盘/写内存）作为衔接。

这样：

- 窄依赖不存中间结果，融合成一个 Task 一次跑完。
- 宽依赖处停下来，把上游结果落盘，下游再读。
- 调度以 Stage 为单位：一个 Stage 里的 Task 可以并行，Stage 之间有先后依赖。

## 4. 这个设计的核心思想

核心思想：**Stage 的边界 = 宽依赖边界；Stage 内部 = 可 pipeline 的窄依赖链**。

```text
parallelize → map → filter     [Stage 0：窄依赖，可 pipeline]
   ↓ wide（Shuffle）
reduce_by_key → map             [Stage 1：窄依赖，可 pipeline]
```

为什么必须是“有向无环图（DAG）”而不能有环？因为 RDD 不可变、Lineage 是父子关系，数据总是“从父流向子”，不可能出现“子的结果又回到父”的循环。有环就死锁了。

## 5. 这个设计解决了什么问题

- **省中间结果**：窄依赖融合，不再每步物化。
- **可并行规划**：一个 Stage 内的多个 Task（多个 Partition）可以并行。
- **正确处理 Shuffle**：宽依赖处强制断开，保证上游全写完下游再读。
- **容错粒度**：失败时可以按 Stage 重新调度，而不是整条链重来。

## 6. 这个设计付出了什么代价

- **需要先构建全局图**：在 Action 触发时要花时间建 DAG、切 Stage，有规划开销（但远小于省下的计算）。
- **Stage 之间必须物化**：宽依赖边界处的数据要落盘/落内存，这是 Spark 最贵的开销（Shuffle）。
- **抽象更重**：新手要从 Lineage 进一步理解 DAG、Stage、pipeline，门槛变高。

## 7. Mini Spark 当前如何实现

本阶段新增：

- `DAGNode`
- `Stage`
- `ExecutionDAG`

`ExecutionDAG.from_rdd(rdd)` 会沿着 RDD 的 parent 链向上找，得到从 Root 到当前 RDD 的操作列表，建成节点序列：

```text
parallelize (root) → map (narrow) → filter (narrow)
```

`stages()` 会扫描每个 DAG 节点：

- 如果是窄依赖：继续放在当前 Stage。
- 如果是宽依赖：切开一个新的 Stage，宽依赖节点自己归到新 Stage。

划分规则的关键代码：

```python
for node in self.nodes:
    if node.dependency_kind == "wide" and current_operations:
        stages.append(Stage(id=len(stages), operations=current_operations))
        current_operations = []
    current_operations.append(node.operation)
```

人话翻译：

```text
从前往后扫节点：
  遇到窄依赖 → 放进当前 Stage
  遇到宽依赖 → 先把当前 Stage 封口，再开新 Stage，宽依赖归新 Stage
```

注意：宽依赖节点自己属于**新** Stage 的开头（因为它是新 Stage 的第一步计算）。

## 8. 最小示例

```python
from mini_spark import SparkContext
from mini_spark.dag import ExecutionDAG

sc = SparkContext()
rdd = sc.parallelize([1, 2, 3]).map(lambda value: value * 10).filter(
    lambda value: value > 10
)

dag = ExecutionDAG.from_rdd(rdd)
print(dag.to_debug_string())
print(dag.stages())
```

输出：

```text
0: parallelize (root)
1: map (narrow)
2: filter (narrow)
[Stage(id=0, operations=['parallelize', 'map', 'filter'])]
```

全是窄依赖，所以整条链在一个 Stage 里。

### 逐行解释

```python
dag = ExecutionDAG.from_rdd(rdd)
```

沿 `rdd._parent` 链往回走到 Root，收集每个节点的 `operation` 和 `dependency_kind`，反序后建成 `DAGNode` 列表。

```python
dag.to_debug_string()
```

打印每个节点的编号、操作名、依赖类型（Root 显示 `root`）。

```python
dag.stages()
```

按宽依赖边界切分，返回 Stage 列表。本例没有宽依赖，所以只有一个 Stage。

## 9. Mini Spark 内部发生了什么

再来看一个带宽依赖的例子（虽然 Shuffle 的真正搬运要到下一章，但 Stage 划分规则现在已经能跑）：

```python
rdd = (
    sc.parallelize([("a", 1), ("b", 2)], num_slices=2)
    .reduce_by_key(lambda a, b: a + b)   # wide
    .map(lambda x: x)                     # narrow
)
dag = ExecutionDAG.from_rdd(rdd)
print(dag.to_debug_string())
print(dag.stages())
```

输出：

```text
0: parallelize (root)
1: reduce_by_key (wide)
2: map (narrow)
[Stage(id=0, operations=['parallelize']), Stage(id=1, operations=['reduce_by_key', 'map'])]
```

过程：

```text
扫描 parallelize(root)  → 当前 Stage 0：['parallelize']
扫描 reduce_by_key(wide) → 遇到宽依赖，封口 Stage 0，开 Stage 1，reduce_by_key 归 Stage 1
扫描 map(narrow)         → 放进当前 Stage 1：['reduce_by_key', 'map']
```

所以 Stage 0 只剩 `parallelize`，Stage 1 是 `reduce_by_key` + `map`。宽依赖就是那把“切刀”。

## 10. 对照真实 Spark：真实世界复杂在哪里

| Mini Spark | 真实 Spark |
| --- | --- |
| `ExecutionDAG` | Spark 作业的 DAG 概念 |
| `Stage` | Spark Stage |
| `narrow` | 窄依赖 |
| `wide` | 宽依赖，通常来自 Shuffle |
| 只划分 Stage，不真正 pipeline | 同一 Stage 内的窄依赖会被融合进同一个 Task（whole-stage codegen 更进一步） |
| Stage 之间不落盘 | Stage 之间靠 Shuffle write/read 衔接，数据落盘 |

真实 Spark 的 DAGScheduler 会根据 RDD 依赖划分 Stage，并把同一 Stage 内的窄依赖**融合成一个 Task**——也就是说一个 Task 内部会连续执行 map、filter、flat_map，一次遍历完成，中间不物化。Mini Spark 只实现了“划分 Stage”的规则，并没有真正把窄依赖融合进一个 Task 去执行（执行时各 RDD 仍通过生成器串联，但没做 Stage 级别的 Task 编排）。

## 11. 亲手实验

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; from mini_spark.dag import ExecutionDAG; sc = SparkContext(); rdd = sc.parallelize([1,2,3]).map(lambda x: x*10).filter(lambda x: x>10); dag = ExecutionDAG.from_rdd(rdd); print(dag.to_debug_string()); print(dag.stages())"
```

再看宽依赖切分：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; from mini_spark.dag import ExecutionDAG; sc = SparkContext(); rdd = sc.parallelize([('a',1),('b',2)], num_slices=2).reduce_by_key(lambda a,b:a+b).map(lambda x:x); dag = ExecutionDAG.from_rdd(rdd); print(dag.to_debug_string()); print(dag.stages())"
```

你会看到 Stage 被宽依赖切成两段。

## 12. 常见误解

### 误解 1：DAG 就是 Lineage

不完全是。Lineage 是 RDD 的来历链（一条链），DAG 是把这些依赖整理成**执行图**（可能分叉、合并，是图不是链）。DAG 是从 Lineage 构造出来的，但比 Lineage 更面向“执行规划”。

### 误解 2：每个 RDD 都是一个 Stage

不是。多个**窄依赖** RDD 可以放在同一个 Stage，融合成一次遍历。只有宽依赖才切开 Stage。

### 误解 3：Stage 是随便切的

不是。Stage 的关键切分点是**宽依赖**，也就是 Shuffle 边界。这是由数据依赖决定的，不是人为拍脑袋。

### 误解 4：宽依赖节点属于上一个 Stage

不是。宽依赖节点是**新 Stage 的第一步**（它需要上游全部写完才能开始读，所以它自己开一个新 Stage）。Mini Spark 的代码正是把宽依赖节点放进新 Stage。

### 误解 5：Mini Spark 已经把窄依赖融合成一个 Task 执行了

还没有。Mini Spark 只**划分**了 Stage，执行时仍按 RDD 生成器串联。真正“融合成一个 Task”是真实 Spark 的优化，Mini Spark 暂未实现。

## 13. 本章掌握标准

如果你能讲清楚下面这些话，就算这一章过关：

- 朴素方案“每个 RDD 各成一个 Stage、各物化”对窄依赖是巨大浪费。
- DAG 是从 Lineage 构造的全局执行图，Stage 是按宽依赖边界切出的执行段。
- 窄依赖可以放在同一个 Stage（理想下融合成一次遍历），宽依赖会切开 Stage。
- 宽依赖节点自己属于新 Stage 的开头。
- DAG 必须无环，因为 RDD 不可变、数据只从父流向子。

## 14. 思考题

1. 如果每个 RDD 各成一个 Stage 并物化中间结果，窄依赖链会浪费什么？
2. 为什么 DAG 不能有环？（提示：RDD 不可变 + 数据流向）
3. 为什么窄依赖可以放在一个 Stage，宽依赖却必须切开？（提示：宽依赖要等所有上游分区写完）
4. 宽依赖节点到底属于上一个 Stage 还是新 Stage？为什么？
5. DAG 和 Lineage 有什么区别？为什么说 Lineage 是“来历链”而 DAG 是“执行图”？
