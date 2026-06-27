# 11 - Spark SQL：SQL 怎么变成执行计划

## 0. 这一章先学什么，不学什么

这一章学习 Spark SQL 的最小链路：

```text
SQL 字符串
  ↓
Logical Plan
  ↓
Physical Plan
  ↓
RDD 执行
```

我们只支持很小的 SQL 子集：

```sql
select name, age from people where age > 18
```

可视化页面：

[Spark SQL Plan 可视化](visualizations/11-spark-sql.html)

## 1. 用人话理解 Spark SQL

SQL 是你想要什么。

Logical Plan 是 Spark 理解后的“逻辑意思”。

Physical Plan 是 Spark 准备怎么执行。

最后，Physical Plan 会落到具体执行上。真实 Spark 里会落到 RDD、WholeStageCodegen、Shuffle、Join 等复杂执行节点。当前 Mini Spark 只把它映射到已有 RDD 的 `filter` 和 `map`。

## 2. 最小示例

```python
from mini_spark.sql import MiniSparkSession

spark = MiniSparkSession()
spark.create_table(
    "people",
    [
        {"name": "alice", "age": 20},
        {"name": "bob", "age": 17},
    ],
)

query = spark.sql("select name, age from people where age > 18")
print(query.collect())
print(query.explain())
```

输出：

```text
[{'name': 'alice', 'age': 20}]
```

## 3. Logical Plan

SQL：

```sql
select name from people where age > 18
```

会变成：

```text
Project[name]
  Filter[age > 18]
    TableScan[people]
```

这表示：

1. 扫描 `people` 表。
2. 过滤 `age > 18`。
3. 只保留 `name` 列。

## 4. Physical Plan

当前 Mini Spark 的 Physical Plan 是：

```text
scan table people
filter age > 18
project name
```

它很简单，只说明执行步骤。

真实 Spark 的 Physical Plan 会复杂得多，比如：

- BroadcastHashJoin
- SortMergeJoin
- Exchange
- HashAggregate
- WholeStageCodegen

## 5. Mini Spark 内部发生了什么

本阶段新增：

- `MiniSparkSession`
- `TableScan`
- `Filter`
- `Project`
- `PhysicalPlan`
- `QueryResult`

执行时：

```text
TableScan -> 拿到表对应的 RDD
Filter -> 调用 RDD.filter
Project -> 调用 RDD.map，只保留需要的列
collect -> 触发 RDD 执行
```

## 6. 对照真实 Spark

| Mini Spark | 真实 Spark |
| --- | --- |
| 简单正则解析 SQL | SQL Parser |
| `Project` / `Filter` / `TableScan` | Logical Plan |
| `PhysicalPlan.steps` | Physical Plan |
| RDD `filter` / `map` | 物理执行节点 |
| `explain()` | Spark DataFrame `explain()` |

真实 Spark 的 Catalyst 会做分析、优化和物理计划选择。我们这里只做最小可理解版本。

## 7. 亲手实验

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark.sql import MiniSparkSession; spark=MiniSparkSession(); spark.create_table('people', [{'name':'alice','age':20},{'name':'bob','age':17}]); q=spark.sql('select name from people where age > 18'); print(q.collect()); print(q.explain())"
```

## 8. 常见误解

### 误解 1：SQL 会直接执行

不是。Spark 会先把 SQL 转成计划。

### 误解 2：Logical Plan 和 Physical Plan 一样

不一样。Logical Plan 表达“要什么”，Physical Plan 表达“怎么做”。

### 误解 3：Spark SQL 和 RDD 完全没关系

不是。Spark SQL 最终也要落到物理执行上，只是它中间有更强的优化器。

## 9. 本章掌握标准

- SQL 会先变成 Logical Plan。
- Logical Plan 再变成 Physical Plan。
- `explain()` 是理解 Spark SQL 的重要工具。
- Spark SQL 强大在于 Catalyst 可以优化执行计划。

## 10. 思考题

1. SQL 字符串为什么不能直接执行？
2. Logical Plan 和 Physical Plan 有什么区别？
3. `Project` 和 `Filter` 分别对应 SQL 里的什么？
4. 为什么 DataFrame 比 RDD 更容易被优化？
5. 真实 Spark 的 Catalyst 可能做哪些优化？
