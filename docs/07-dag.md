# 07 - DAG 与 Stage：Spark 怎么看完整计算图

## 0. 这一章先学什么，不学什么

这一章学习 DAG 和 Stage。

前面我们已经知道 RDD 会形成 Lineage。DAG 可以理解成：

> 把 RDD 血缘链整理成一张执行图。

Stage 可以理解成：

> 在执行图上按依赖类型切出来的一段执行区域。

这一章先不实现 Shuffle，只先支持 Stage 划分规则：窄依赖放在同一个 Stage，宽依赖会切开新的 Stage。

可视化页面：

[DAG 与 Stage 可视化](visualizations/07-dag.html)

## 1. 用人话理解 DAG

DAG 是 Directed Acyclic Graph，中文叫有向无环图。

名字很硬，但你可以先理解成：

```text
一张从数据来源到最终结果的路线图。
```

比如：

```text
parallelize
  ↓
map
  ↓
filter
```

这就是一条很简单的 DAG。

## 2. 什么是 Stage

Stage 是 Spark 执行计划中的一段。

当前阶段的规则先记住一句：

> 窄依赖可以放在同一个 Stage，宽依赖会切开 Stage。

现在 `map`、`filter` 都是窄依赖，所以：

```text
parallelize -> map -> filter
```

会在一个 Stage 里。

后面到了 Shuffle，宽依赖会让 DAG 被切成多个 Stage。

## 3. 最小示例

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

## 4. Mini Spark 内部发生了什么

本阶段新增：

- `DAGNode`
- `Stage`
- `ExecutionDAG`

`ExecutionDAG.from_rdd(rdd)` 会沿着 RDD 的 parent 链向上找，得到从 Root 到当前 RDD 的操作列表。

`stages()` 会扫描每个 DAG 节点：

- 如果是窄依赖：继续放在当前 Stage。
- 如果是宽依赖：切开一个新的 Stage。

## 5. 对照真实 Spark

| Mini Spark | 真实 Spark |
| --- | --- |
| `ExecutionDAG` | Spark 作业的 DAG 概念 |
| `Stage` | Spark Stage |
| `narrow` | 窄依赖 |
| `wide` | 宽依赖，通常来自 Shuffle |

真实 Spark 的 DAGScheduler 会根据 RDD 依赖划分 Stage。我们这里只实现了最小规则。

## 6. 亲手实验

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; from mini_spark.dag import ExecutionDAG; sc = SparkContext(); rdd = sc.parallelize([1,2,3]).map(lambda x: x*10).filter(lambda x: x>10); dag = ExecutionDAG.from_rdd(rdd); print(dag.to_debug_string()); print(dag.stages())"
```

## 7. 常见误解

### 误解 1：DAG 就是 Lineage

不完全是。Lineage 是 RDD 的来历链，DAG 是把这些依赖整理成执行图。

### 误解 2：每个 RDD 都是一个 Stage

不是。多个窄依赖 RDD 可以放在同一个 Stage。

### 误解 3：Stage 是随便切的

不是。Stage 的关键切分点通常是宽依赖，也就是 Shuffle 边界。

## 8. 本章掌握标准

- DAG 是完整计算路线图。
- Stage 是 DAG 上按依赖边界切出来的执行段。
- 窄依赖通常可以放在同一个 Stage。
- 宽依赖会切开 Stage。

## 9. 思考题

1. 为什么 DAG 不能有环？
2. 为什么窄依赖可以放在一个 Stage？
3. 为什么宽依赖会切开 Stage？
4. DAG 和 Lineage 有什么区别？
5. 后面 Shuffle 为什么会影响 Stage 划分？
