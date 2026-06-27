# 06 - Scheduler：谁把 Partition 变成 Task

## 0. 这一章先学什么，不学什么

这一章学习 Scheduler、Task 和本地 Executor。

上一章我们已经把 RDD 拆成多个 Partition。这一章回答：

> 有了 Partition 之后，谁来安排每个 Partition 的计算？

答案是：Scheduler。

可视化页面：

[Scheduler 可视化](visualizations/06-scheduler.html)

## 1. 先从最朴素的方案开始

上一章我们有了 Partition，`collect_partitions()` 会**顺序地**逐个算 Partition：

```text
算 Partition 0
算 Partition 1
算 Partition 2
...
```

这相当于一个厨师把几份菜一份接一份地做。朴素方案就是：**直接一个循环把所有分区算完，不需要中间抽象**。

这能跑，而且对 Mini Spark 当前的小数据完全够用。

## 2. 朴素方案会遇到什么问题

### 问题 1：循环写死在 Action 里，没法扩展

如果 `collect`、`count` 各自写一个“遍历所有分区”的循环，那么每加一个 Action 就要重写一遍调度逻辑。重复且容易出错。

### 问题 2：没法把“要做什么”和“在哪个分区做”分开

朴素循环里，“对每个分区算 list” 和 “对每个分区算 count” 混在一起。但真实 Spark 里，“调度（决定跑哪些 Task）”和“计算（每个 Task 具体干什么）”是两件事，应该分开。

### 问题 3：没法走向并行和分布式

朴素循环是单进程顺序执行。如果要走向“多 Task 并行 / 分发到多台机器”，就必须有一个**可替换的执行层**：调度器决定跑哪些 Task，执行器决定怎么跑。没有这层抽象，并行和分布式无从下手。

### 问题 4：没法观察“到底跑了几个 Task”

朴素循环跑完就完了，系统没有留下“这次 Job 创建了哪些 Task”的记录。而真实 Spark 的 UI 里你能看到每个 Job 有多少 Task、各跑了多久——这需要调度层显式地把 Task 作为一等公民创建出来。

## 3. Spark 为什么需要 Scheduler

Spark 的选择是把执行拆成两个角色：

- **Scheduler（调度器）**：看到 RDD 有几个 Partition，就为每个 Partition 创建一个 Task，决定它们怎么跑、按什么顺序、发到哪里。
- **Executor（执行器）**：拿到一个 Task，在某个进程/机器上真正把数据算出来。

中间的“合同”就是 **Task**：它描述“对第几个 Partition、执行哪个 Action”。

这样拆分的好处：

- Action 只需要告诉 Scheduler“我要对每个分区做什么”，不关心怎么跑。
- 调度逻辑只写一遍，所有 Action 复用。
- 想从“本地顺序”升级到“分布式并行”，只换 Executor / 调度策略，RDD 和 Action 不用改。

## 4. 这个设计的核心思想

核心思想：**Partition 是数据的切分单位，Task 是计算的执行单位，Scheduler 把前者映射成后者**。

```text
看到 RDD 有 N 个 Partition
  ↓ Scheduler
为每个 Partition 创建一个 Task
  ↓ Executor
逐个（或并行）执行 Task，拿到每个分区的结果
  ↓
汇总返回 Driver
```

理想情况下：**N 个 Partition → N 个 Task → N 路并行**。

Mini Spark 当前是“本地顺序执行”，所以这 N 个 Task 是一个个跑的，不是真并行。但抽象已经搭好，换执行器就能并行。

## 5. 这个设计解决了什么问题

- 解决“调度逻辑重复”：Scheduler 写一遍，`collect`/`count`/… 都复用。
- 解决“要做什么 vs 怎么跑”混淆：Action 只提供 function，Scheduler 负责 Task 编排。
- 解决“没法走向并行/分布式”：Executor 可替换，是并行的扩展点。
- 解决“没法观察”：Task 被显式创建和记录，可以回放“这次跑了几个 Task”。

## 6. 这个设计付出了什么代价

- **多了一层抽象**：新手要先理解 Partition → Task → Executor 这条链，比直接写循环绕。
- **顺序执行时有额外开销**：本地顺序跑时，建 Task 对象比直接写循环多了一点开销（但对学习值得）。
- **Mini Spark 的 Scheduler 很弱**：还没做 Stage、重试、数据本地性、资源协商——这些是真实 Spark Scheduler 的重头戏。

## 7. Mini Spark 当前如何实现

本阶段新增：

- `Task`
- `TaskResult`
- `LocalExecutor`
- `LocalScheduler`

`Task`：

```python
@dataclass(frozen=True)
class Task:
    partition_index: int
    operation: str
```

一个 Task 代表：

```text
请对第几个 Partition 执行哪个 Action。
```

`LocalScheduler.run_job()` 会为每个 Partition 创建 Task，然后交给 `LocalExecutor` 运行：

```python
def run_job(self, rdd, function, operation):
    partition_data = rdd._compute_partitions()
    self.last_tasks = [
        Task(partition_index=index, operation=operation)
        for index, _ in enumerate(partition_data)
    ]
    return [
        self._executor.run(task, values, function)
        for task, values in zip(self.last_tasks, partition_data)
    ]
```

人话翻译：

```text
1. 先算出 RDD 各个 Partition 的数据（生成器列表）。
2. 为每个 Partition 建一个 Task（记下分区号和操作名）。
3. 把每个 Task 连同对应分区的数据，交给 Executor 执行。
4. 收集所有 TaskResult 返回。
```

而 Action 这边变得很干净，比如 `count()`：

```python
def count(self) -> int:
    scheduler = LocalScheduler()
    results = scheduler.run_job(
        self,
        lambda values: sum(1 for _ in values),  # 每个 Task 干的事
        operation="count",
    )
    self._last_job_tasks = scheduler.last_tasks
    return sum(result.value for result in results)
```

Action 只提供“每个分区要算什么”（一个 function），调度细节全交给 Scheduler。最后再把各分区的结果汇总（`sum`）。

## 8. 最小示例

```python
from mini_spark import SparkContext

sc = SparkContext()
rdd = sc.parallelize([1, 2, 3, 4], num_slices=2).map(lambda value: value * 10)

print(rdd.collect())
print(rdd.last_job_tasks())
```

输出类似：

```text
[10, 20, 30, 40]
[Task(partition_index=0, operation='collect'), Task(partition_index=1, operation='collect')]
```

这说明 `collect()` 触发了一个 Job，Scheduler 为两个 Partition 创建了两个 Task。

### 逐行解释

```python
rdd = sc.parallelize([1, 2, 3, 4], num_slices=2).map(lambda value: value * 10)
```

得到一个有 2 个 Partition 的 RDD（还没执行）。

```python
rdd.collect()
```

触发 Action。`collect` 调用 `LocalScheduler.run_job(..., operation="collect")`，于是为 Partition 0、1 各建一个 Task，分别把该分区数据收集成 list，最后拍平成 `[10, 20, 30, 40]`。

```python
rdd.last_job_tasks()
```

返回上一次 Action 创建的 Task 列表。这是 Mini Spark 为学习加的观察接口，让你看见“Partition → Task”的映射。

## 9. Mini Spark 内部发生了什么

```text
collect()
  ↓
LocalScheduler.run_job(rdd, lambda values: list(values), "collect")
  ↓
rdd._compute_partitions()  →  [Partition0 的生成器, Partition1 的生成器]
  ↓
为每个分区建 Task(partition_index=i, operation="collect")
  ↓
LocalExecutor 逐个执行：function(partition_data) → 该分区的 list
  ↓
汇总：chain.from_iterable → [10, 20, 30, 40]
```

关键点：当前是**顺序**执行 Task（列表推导一个接一个）。要变成并行，只需把“逐个执行”换成线程池/进程池/远程分发——RDD 和 Action 代码不用动。

## 10. 对照真实 Spark：真实世界复杂在哪里

| Mini Spark | 真实 Spark |
| --- | --- |
| `LocalScheduler` | `DAGScheduler` + `TaskScheduler` 的极简影子 |
| `Task` | Spark Task |
| `LocalExecutor` | Executor 的本地模拟 |
| 顺序执行 Task | 多 Executor 并行执行 Task |
| 没有 Stage 划分 | DAGScheduler 按 Shuffle 边界切 Stage |
| 没有 Task 重试 | 失败 Task 自动重试，可换 Executor |
| 没有数据本地性 | 优先把 Task 调度到数据所在机器 |
| 没有资源协商 | 由 Cluster Manager 协商 CPU/内存 |

真实 Spark 还会做 Stage 划分、Task 重试、数据本地性、推测执行（speculative）、资源调度等复杂工作。Mini Spark 的 Scheduler 只保留了“一个 Partition 一个 Task”这条最核心的映射。

## 11. 亲手实验

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc = SparkContext(); rdd = sc.parallelize([1,2,3,4], num_slices=2).map(lambda x: x*10); print(rdd.collect()); print(rdd.last_job_tasks())"
```

你会看到每个 Partition 对应一个 Task。

再试 `count`，看 Task 的 operation 字段变化：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc = SparkContext(); rdd = sc.parallelize([1,2,3,4], num_slices=3); print(rdd.count()); print(rdd.last_job_tasks())"
```

预期输出：

```text
4
[Task(partition_index=0, operation='count'), Task(partition_index=1, operation='count'), Task(partition_index=2, operation='count')]
```

3 个 Partition → 3 个 Task，operation 变成了 `count`。

## 12. 常见误解

### 误解 1：有 Partition 就会自动并行

不一定。Partition 是并行的**基础**，但还需要 Scheduler 把它们变成 Task、需要 Executor 真正并行执行。当前 Mini Spark 仍然是本地顺序执行。

### 误解 2：Task 就是 RDD

不是。RDD 是“数据 + 计算逻辑”的抽象，Task 是针对**某个 Partition 的一次执行**。一个 RDD 有 N 个 Partition，一次 Action 会产生 N 个 Task。

### 误解 3：Scheduler 越多 Task 越快

在 Mini Spark 里不会——因为是顺序执行，Task 多了反而多开销。只有真正并行时，多 Task 才能换来多路并行。

### 误解 4：collect 和 count 走的是两套调度

不是。它们都走 `LocalScheduler.run_job`，只是传入的 `function` 和 `operation` 不同。这就是抽出 Scheduler 的好处。

## 13. 本章掌握标准

如果你能讲清楚下面这些话，就算这一章过关：

- 朴素循环在“复用、解耦、并行扩展、可观察”四方面撑不住。
- Scheduler 把 Partition 映射成 Task，Executor 负责真正执行。
- Action 只提供“每个分区算什么”，调度细节交给 Scheduler。
- 当前 Mini Spark 是本地顺序执行，Executor 可替换是走向并行的扩展点。
- 真实 Spark 的 Scheduler 还要做 Stage、重试、数据本地性、资源协商。

## 14. 思考题

1. 为什么要把“调度”和“执行”拆成 Scheduler 和 Executor 两个角色，而不是写在一个循环里？
2. 一个 Partition 通常对应一个 Task，这个映射为什么是 Spark 并行度的关键？
3. 为什么当前 Mini Spark 有 Task 却还不是真正并行？缺的是什么？
4. 如果有 100 个 Partition，可能会创建多少个 Task？在顺序执行下这意味着什么？
5. 真实 Spark 为什么还需要 Task 重试和数据本地性？Mini Spark 现在为什么可以暂时不做？
