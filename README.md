# mini-spark-learning

用 Python 从零实现一个 Mini Spark，目标不是“造一个能替代 Spark 的框架”，而是通过亲手实现 RDD、Transformation、Action、Lineage、Partition、Scheduler、DAG、Shuffle、Cache、容错和 SQL，真正理解 Spark 为什么这样设计。

这个项目适合想系统学习 Spark 的初学者。每一章都会尽量回答三个问题：

- 如果没有这个设计，会遇到什么问题？
- Spark 为什么选择这种抽象？
- Mini Spark 为了学习做了哪些简化？

## 当前进度

已完成 11 个阶段：

| 阶段 | 主题 | 学什么 |
| --- | --- | --- |
| 01 | RDD 基础 | 为什么需要 RDD 这种“数据 + 计算描述”的抽象 |
| 02 | Transformation | 为什么 `map` / `filter` 不马上执行 |
| 03 | Action | 为什么 `collect` / `count` 才会触发计算 |
| 04 | Lineage | 为什么 RDD 要记住父子关系，容错从哪里来 |
| 05 | Partition | 为什么大数据要切分，分区和并行是什么关系 |
| 06 | Scheduler | Action 如何变成 Job、Task，再交给 Executor |
| 07 | DAG | 为什么执行计划不是一条链，而是一张图 |
| 08 | Shuffle | 为什么 `groupByKey` / `reduceByKey` 会变贵 |
| 09 | Cache | 为什么缓存能避免重复计算，以及它的代价 |
| 10 | Fault Tolerance | 分区丢失后，如何通过 lineage 重算恢复 |
| 11 | Spark SQL | SQL 如何变成逻辑计划、物理计划和 RDD 计算 |

## 学习方式

推荐按这个顺序学习：

1. 先读对应章节文档，例如 [docs/01-rdd-basics.md](docs/01-rdd-basics.md)。
2. 如果章节有 HTML 可视化，直接打开 `docs/visualizations/` 下的页面。
3. 再看对应测试文件，例如 [tests/test_stage_1_rdd_basics.py](tests/test_stage_1_rdd_basics.py)。
4. 最后看实现代码，例如 [mini_spark/rdd.py](mini_spark/rdd.py)。

重点不要只记 API，而是看清设计推导：朴素方案哪里不够，RDD 这种设计解决了什么，又带来了什么代价。

## 运行环境

项目使用 Python 3.10。当前本机推荐使用项目目录下的虚拟环境：

```bash
.venv/Scripts/python.exe --version
```

运行全部测试：

```bash
.venv/Scripts/python.exe -m pytest tests -q
```

运行单个阶段测试：

```bash
.venv/Scripts/python.exe -m pytest tests/test_stage_8_shuffle.py -q
```

## 快速体验 RDD

```python
from mini_spark import SparkContext

sc = SparkContext()

result = (
    sc.parallelize([1, 2, 3, 4], num_slices=2)
    .map(lambda value: value * 10)
    .filter(lambda value: value > 20)
    .collect()
)

print(result)
```

预期输出：

```text
[30, 40]
```

这段代码的重点是：`map` 和 `filter` 不会立刻计算，它们只记录计算关系；直到 `collect()` 这个 Action 出现，Mini Spark 才开始沿着 RDD 血缘真正拉取数据。

## 快速体验 SQL

```python
from mini_spark.sql import MiniSparkSession

spark = MiniSparkSession()
spark.create_table(
    "users",
    [
        {"name": "Alice", "age": 18},
        {"name": "Bob", "age": 30},
        {"name": "Cindy", "age": 25},
    ],
)

query = spark.sql("select name from users where age > 20")

print(query.explain())
print(query.collect())
```

SQL 章节会展示：一条 SQL 不是直接执行字符串，而是先变成 Logical Plan，再变成 Physical Plan，最后落到 RDD 的 `map` / `filter` / `collect`。

## 文档与可视化

章节文档：

- [01 RDD 基础](docs/01-rdd-basics.md)
- [02 Transformation](docs/02-transformations.md)
- [03 Action](docs/03-actions.md)
- [04 Lineage](docs/04-lineage.md)
- [05 Partition](docs/05-partitions.md)
- [06 Scheduler](docs/06-scheduler.md)
- [07 DAG](docs/07-dag.md)
- [08 Shuffle](docs/08-shuffle.md)
- [09 Cache](docs/09-cache.md)
- [10 Fault Tolerance](docs/10-fault-tolerance.md)
- [11 Spark SQL](docs/11-spark-sql.md)

HTML 可视化页面：

- [Lazy Evaluation](docs/visualizations/02-lazy-evaluation.html)
- [Actions](docs/visualizations/03-actions.html)
- [Lineage](docs/visualizations/04-lineage.html)
- [Partitions](docs/visualizations/05-partitions.html)
- [Scheduler](docs/visualizations/06-scheduler.html)
- [DAG](docs/visualizations/07-dag.html)
- [Shuffle](docs/visualizations/08-shuffle.html)
- [Cache](docs/visualizations/09-cache.html)
- [Fault Tolerance](docs/visualizations/10-fault-tolerance.html)
- [Spark SQL](docs/visualizations/11-spark-sql.html)

这些 HTML 页面不依赖复杂前端框架，直接用浏览器打开即可。

## 项目结构

```text
mini_spark/
  context.py      # SparkContext，创建 RDD 的入口
  rdd.py          # RDD、Partition、Transformation、Action、Cache、容错
  scheduler.py    # LocalScheduler、Task、LocalExecutor
  dag.py          # 按宽依赖切分 Stage
  sql.py          # MiniSparkSession、SQL 解析、逻辑计划、物理计划

tests/            # 每个阶段对应的测试
docs/             # 中文学习文档
docs/visualizations/
                  # HTML 可视化学习页
```

## 和真实 Spark 的关系

Mini Spark 故意做了很多简化：

- 本地单进程执行，不是真正的分布式集群。
- Executor 是本地模拟，不涉及真实进程、机器和网络。
- Shuffle 只模拟核心数据重分布思想，没有磁盘落盘、网络传输和排序优化。
- Cache 只演示命中、未命中和重算，不实现完整 StorageLevel。
- SQL 只支持很小的 `select ... from ... where ...` 子集。

这些简化是为了把学习焦点放在 Spark 的核心思想上：**RDD 是一种把数据分区、计算关系、依赖图和容错能力组织起来的设计。**
