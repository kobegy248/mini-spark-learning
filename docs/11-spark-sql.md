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

## 1. 先从最朴素的方案开始

我们已经有了 RDD 这套执行引擎。朴素方案怎么加 SQL 支持？

最直接的想法：

```text
写一个函数，直接对表数据执行 SQL 想做的事。
比如看到 "where age > 18" 就立刻 filter，
看到 "select name, age" 就立刻取列，
一步到位，拿到结果。
```

也就是说，朴素方案是：**直接把 SQL 翻译成一连串 RDD 调用，立刻执行**，不搞什么“计划”。

```python
def run_sql(table, sql):
    # 解析出 where 条件和要的列
    # 立刻 filter
    # 立刻取列
    # collect 返回
    ...
```

这对最小子集能跑，而且代码很短。

## 2. 朴素方案会遇到什么问题

### 问题 1：没法优化

朴素方案“看到什么做什么”，没有全局视角。它没法发现：

```text
这个 where 可以先做（减少数据量），再做 select
两个 filter 能不能合并？
同一个表被多次查询，能不能缓存？
join 用哪种策略更划算？
```

优化需要先有一个“逻辑计划”摆在面前，再决定怎么改写。

### 问题 2：SQL 和执行耦合死

朴素方案把“理解 SQL”和“执行”混在一起。换个执行方式（比如换 join 策略）就要改解析逻辑。真实 Spark 想让“逻辑”和“物理执行”解耦，各自演化。

### 问题 3：没法 explain

用户想问“Spark 到底打算怎么跑我的 SQL”，朴素方案答不上来——因为它没有把“打算怎么跑”显式记下来。

### 问题 4：扩展性差

要加 join、group by、子查询、窗口函数时，“直接翻译”的代码会迅速变成一坨面条，没法维护。

## 3. Spark 为什么需要 Logical Plan 和 Physical Plan

Spark 的选择是把 SQL 执行拆成清晰的阶段：

```text
SQL 字符串
  ↓ 解析
Logical Plan（逻辑计划：你要什么，不关心怎么执行）
  ↓ 优化（Catalyst）
优化后的 Logical Plan
  ↓ 转换
Physical Plan（物理计划：具体怎么执行，用什么算子）
  ↓
RDD 执行
```

为什么要分两层？

- **Logical Plan** 只表达“要什么”（扫哪张表、过滤什么、要哪些列），不关心执行细节。它是优化器的工作对象——优化器在逻辑层面改写计划（谓词下推、列裁剪、常量折叠等），不用管底层。
- **Physical Plan** 表达“怎么做”（用什么 join 策略、要不要 Shuffle、要不要广播），它关心执行代价。

分开后：优化在逻辑层做，执行在物理层做，互不干扰，各自可扩展。这就是 Spark SQL 强大的根源——**Catalyst 优化器**。

## 4. 这个设计的核心思想

核心思想：**把“声明式查询”（SQL）经过“逻辑计划 → 优化 → 物理计划”变成“可执行的物理动作”**。

用一句话区分两层计划：

- Logical Plan：**你要什么**（What）。
- Physical Plan：**怎么做**（How）。

在当前 Mini Spark 里：

- Logical Plan 是 `Project` / `Filter` / `TableScan` 组成的树。
- Physical Plan 是一串步骤字符串（`scan table`、`filter`、`project`），最终映射到已有 RDD 的 `filter` 和 `map`。

## 5. 这个设计解决了什么问题

- **可优化**：逻辑计划是优化器的对象，可以全局改写。
- **可解释**：`explain()` 能告诉用户“打算怎么跑”，调试友好。
- **解耦**：SQL 语法、逻辑优化、物理执行三者分离，各自演化。
- **可扩展**：加新算子、新优化规则、新物理策略，互不影响。

## 6. 这个设计付出了什么代价

- **多几层抽象**：新手要理解 SQL → Logical → Physical → RDD 这条链，比“直接翻译”绕。
- **解析/优化有开销**：建计划、跑优化器都要时间（但对大数据量，优化省下的远多于开销）。
- **实现复杂**：真正的 Catalyst 是一棵可改写的规则引擎，远比 Mini Spark 的正则解析复杂。

## 7. Mini Spark 当前如何实现

本阶段新增：

- `MiniSparkSession`
- `TableScan`
- `Filter`
- `Project`
- `PhysicalPlan`
- `QueryResult`

### 解析（SQL → Logical Plan）

用正则解析这一小段 SQL：

```python
match = re.fullmatch(
    r"\s*select\s+(.+?)\s+from\s+(\w+)(?:\s+where\s+(\w+)\s*(=|>|<)\s*(.+?))?\s*",
    query, flags=re.IGNORECASE,
)
```

解析后建成一棵逻辑计划树：

```text
Project[name, age]
  Filter[age > 18]
    TableScan[people]
```

读法是从下往上：先扫 `people` 表，再过滤 `age > 18`，最后只保留 `name, age` 列。

### 逻辑计划 → 物理计划

`_build_physical_plan` 递归访问逻辑树，按 `TableScan → Filter → Project` 的顺序收集物理步骤：

```text
scan table people
filter age > 18
project name, age
```

注意顺序：物理计划是“执行顺序”，所以从最底层的 TableScan 开始 append，最后才是最顶层的 Project。

### 物理计划 → RDD 执行

`_execute` 把逻辑计划映射到 RDD：

```text
TableScan → 拿到表对应的 RDD（create_table 时用 parallelize 建的）
Filter    → 调用 RDD.filter
Project   → 调用 RDD.map，只保留需要的列
collect   → 触发 RDD 执行
```

所以 Spark SQL 最终还是落到我们前几章实现的 RDD 执行引擎上——`filter` 和 `map` 都是 Transformation，`collect` 是 Action。SQL 只是 RDD 之上的一层“声明式前端”。

## 8. 最小示例

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
Logical Plan:
Project[name, age]
  Filter[age > 18]
    TableScan[people]

Physical Plan:
scan table people
filter age > 18
project name, age
```

### 逐行解释

```python
spark.create_table("people", [...])
```

把一组字典行变成一个 RDD 存进表字典。这就是 `TableScan` 将要读的数据源。

```python
spark.sql("select name, age from people where age > 18")
```

这一步**不执行查询**。它做三件事：解析 SQL 成 Logical Plan、构建 Physical Plan、返回一个 `QueryResult`。注意：和 RDD 的 Lazy 一样，`sql()` 只是建计划，不跑。

```python
query.collect()
```

这才是触发执行的 Action。它调用 `_execute(logical_plan)` 把计划映射成 RDD 链，最后 `collect()` 触发 RDD 计算。

```python
query.explain()
```

把 Logical Plan 和 Physical Plan 打印出来，让你看见“Spark 打算怎么跑这条 SQL”。这是理解 Spark SQL 最重要的调试工具。

## 9. Mini Spark 内部发生了什么

```text
"select name, age from people where age > 18"
  ↓ 正则解析
Logical Plan:  Project[name, age] → Filter[age>18] → TableScan[people]
  ↓ 构建
Physical Plan: scan table people / filter age > 18 / project name, age
  ↓ collect() 触发
TableScan → 表对应的 RDD
  ↓
Filter    → RDD.filter(lambda row: row['age'] > 18)
  ↓
Project   → RDD.map(lambda row: {name, age 取列})
  ↓
collect() → [{'name':'alice','age':20}]
```

整条链最终全是我们前几章学过的 RDD：`filter`、`map` 是 Transformation，`collect` 是 Action。Spark SQL 没有凭空造一套执行引擎，它复用了 RDD 引擎。

## 10. 对照真实 Spark：真实世界复杂在哪里

| Mini Spark | 真实 Spark |
| --- | --- |
| 简单正则解析 SQL | ANTLR + Catalyst SQL Parser，支持完整 SQL 语法 |
| `Project` / `Filter` / `TableScan` | 丰富的 Logical Plan 节点（Join、Aggregate、Window、Generate…） |
| 没有优化 | Catalyst 优化器：谓词下推、列裁剪、常量折叠、Join 重排等 |
| `PhysicalPlan.steps` 是字符串列表 | Physical Plan 是真实算子树 |
| 物理执行只映射到 `filter`/`map` | 物理算子：BroadcastHashJoin、SortMergeJoin、Exchange、HashAggregate、WholeStageCodegen… |
| `explain()` | Spark DataFrame `explain()`，可显示逻辑/物理/执行计划 |

真实 Spark 的 Catalyst 会做**分析、逻辑优化、物理计划选择**三件事：

- **分析**：把未解析的字段名绑定到真实表结构（`age` 到底是哪张表的哪一列）。
- **逻辑优化**：谓词下推（把 filter 推到靠近数据源）、列裁剪（只读用到的列）、常量折叠（`1+1` 提前算成 `2`）。
- **物理选择**：join 选 BroadcastHashJoin 还是 SortMergeJoin？要不要 Exchange（Shuffle）？

Mini Spark 完全没有这些——它只是“解析 → 建计划 → 直接映射到 RDD”，没有分析绑定，没有优化规则，没有物理算子选择。这是当前最大的简化。

## 11. 为什么 DataFrame 比 RDD 更容易被优化

回到第 01 章“为什么不是直接用 Iterator”的思路：RDD 的计算逻辑藏在用户的 lambda 里，Spark 看不进去——它不知道 `lambda x: x*2` 在干什么，没法优化。

而 DataFrame/SQL 的计划是**结构化**的：`Filter[age>18]`、`Project[name,age]` 是 Spark 能理解的数据结构。优化器可以改写它（下推、裁剪、合并），因为“要做什么”对 Spark 是透明的。

这就是 Spark SQL 比 RDD API 更容易优化的根本原因：**结构化的计划比不透明的 lambda 更可分析、可改写**。

## 12. 亲手实验

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark.sql import MiniSparkSession; spark=MiniSparkSession(); spark.create_table('people', [{'name':'alice','age':20},{'name':'bob','age':17}]); q=spark.sql('select name, age from people where age > 18'); print(q.collect()); print(q.explain())"
```

再试不带 where 的查询，观察计划变化：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark.sql import MiniSparkSession; spark=MiniSparkSession(); spark.create_table('people', [{'name':'alice','age':20},{'name':'bob','age':17}]); q=spark.sql('select name from people'); print(q.explain())"
```

预期输出：

```text
Logical Plan:
Project[name]
  TableScan[people]

Physical Plan:
scan table people
project name
```

没有 where，逻辑计划里就没有 `Filter` 节点，物理计划里也没有 `filter` 步骤。

## 13. 常见误解

### 误解 1：SQL 会直接执行

不会。Spark 会先把 SQL 转成 Logical Plan，再转成 Physical Plan，最后才落到物理执行。`sql()` 调用本身不执行查询，要等 `collect()` 这类 Action。

### 误解 2：Logical Plan 和 Physical Plan 一样

不一样。Logical Plan 表达“要什么”（What），Physical Plan 表达“怎么做”（How）。前者是优化对象，后者关心执行代价。

### 误解 3：Spark SQL 和 RDD 完全没关系

不是。Spark SQL 最终也要落到物理执行上，在 Mini Spark 里就是落到 RDD 的 `filter`/`map`。真实 Spark 的底层也还是有 RDD 级别的执行（尽管 WholeStageCodegen 把很多算子融合编译了）。Spark SQL 是 RDD 之上的声明式前端 + 优化器。

### 误解 4：Mini Spark 的 SQL 也能优化

不能。Mini Spark 只做了“解析 → 建计划 → 直接映射执行”，没有 Catalyst 那样的优化规则。谓词下推、列裁剪这些它都不会做。

### 误解 5：explain 只是给开发者看的装饰

不是。`explain` 是理解 Spark SQL 行为的核心工具。生产中排查“为什么这么慢”“有没有 Shuffle”“join 用的什么策略”，全靠看物理计划。

## 14. 本章掌握标准

如果你能讲清楚下面这些话，就算这一章过关：

- 朴素方案“直接翻译 SQL 立刻执行”没法优化、没法 explain、扩展性差。
- Spark SQL 把执行拆成 Logical Plan（要什么）→ 优化 → Physical Plan（怎么做）→ RDD 执行。
- 分层是为了让优化在逻辑层做、执行在物理层做，各自演化。
- SQL/DataFrame 比 RDD 更容易优化，因为计划是结构化的、对 Spark 透明，而不像 lambda 是黑盒。
- Mini Spark 只做了最小链路，没有 Catalyst 优化、没有物理算子选择；但最终仍落到前几章的 RDD 引擎。

## 15. 思考题

1. 朴素方案“直接翻译 SQL 立刻执行”会撞上哪些问题？为什么优化需要先有一个逻辑计划？
2. Logical Plan 和 Physical Plan 的区别是什么？为什么要把“要什么”和“怎么做”分开？
3. `Project` 和 `Filter` 分别对应 SQL 里的什么？它们在逻辑计划树里的父子关系是怎样的？
4. 为什么 DataFrame/SQL 比 RDD API 更容易被优化？（提示：结构化 vs 不透明 lambda）
5. 真实 Spark 的 Catalyst 可能做哪些优化？举两个例子（如谓词下推、列裁剪）并说明它们为什么能省计算。
