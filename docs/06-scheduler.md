# 06 - Scheduler：谁把 Partition 变成 Task

## 0. 这一章先学什么，不学什么

这一章学习 Scheduler、Task 和本地 Executor。

上一章我们已经把 RDD 拆成多个 Partition。这一章回答：

> 有了 Partition 之后，谁来安排每个 Partition 的计算？

答案是：Scheduler。

可视化页面：

[Scheduler 可视化](visualizations/06-scheduler.html)

## 1. 用人话理解 Scheduler

如果 Partition 是“要处理的几份数据”，Task 就是“处理其中一份数据的任务单”。

Scheduler 的工作是：

```text
看到 RDD 有几个 Partition
  ↓
为每个 Partition 创建一个 Task
  ↓
交给 Executor 执行
  ↓
收集每个 Task 的结果
```

当前 Mini Spark 只有本地 Executor，所以 Task 是顺序执行的。真实 Spark 中，Task 会分发到多个 Executor 上并行执行。

## 2. 最小示例

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

## 3. Mini Spark 内部发生了什么

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

`LocalScheduler.run_job()` 会为每个 Partition 创建 Task，然后交给 `LocalExecutor` 运行。

## 4. 对照真实 Spark

| Mini Spark | 真实 Spark |
| --- | --- |
| `LocalScheduler` | `DAGScheduler` + `TaskScheduler` 的极简影子 |
| `Task` | Spark Task |
| `LocalExecutor` | Executor 的本地模拟 |
| 顺序执行 Task | 多 Executor 并行执行 Task |

真实 Spark 还会做 Stage 划分、Task 重试、数据本地性、资源调度等复杂工作。

## 5. 亲手实验

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc = SparkContext(); rdd = sc.parallelize([1,2,3,4], num_slices=2).map(lambda x: x*10); print(rdd.collect()); print(rdd.last_job_tasks())"
```

你会看到每个 Partition 对应一个 Task。

## 6. 常见误解

### 误解 1：有 Partition 就会自动并行

不一定。Partition 是并行的基础，但还需要 Scheduler 和 Executor。当前 Mini Spark 仍然是本地顺序执行。

### 误解 2：Task 就是 RDD

不是。RDD 是数据和计算逻辑的抽象，Task 是针对某个 Partition 的一次执行任务。

## 7. 本章掌握标准

- Scheduler 根据 Partition 创建 Task。
- Executor 执行 Task。
- Action 会触发 Scheduler。
- 当前 Mini Spark 是本地顺序执行，真实 Spark 是分布式并行执行。

## 8. 思考题

1. 为什么一个 Partition 通常会对应一个 Task？
2. Scheduler 和 Executor 分别负责什么？
3. 为什么当前 Mini Spark 还不是真正并行？
4. 如果有 100 个 Partition，可能会创建多少个 Task？
5. 真实 Spark 为什么还需要 Task 重试？
