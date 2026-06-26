# Stage 1 RDD Basics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first runnable Python Mini Spark slice: `SparkContext`, `RDD`, `parallelize`, and `collect`.

**Architecture:** Keep the first stage intentionally small. `SparkContext` is the user entry point and creates root `RDD` instances from local Python iterables. `RDD` stores immutable local data and exposes `collect()` as the first action.

**Tech Stack:** Python 3, pytest, Markdown docs.

---

## File Structure

- Create: `mini_spark/__init__.py`
  - Public package entry point.
  - Exports `SparkContext` and `RDD`.
- Create: `mini_spark/context.py`
  - Defines `SparkContext`.
  - Owns `parallelize`.
- Create: `mini_spark/rdd.py`
  - Defines `RDD`.
  - Owns `collect`.
- Create: `tests/test_stage_1_rdd_basics.py`
  - Verifies the first public behavior.
- Create: `docs/01-rdd-basics.md`
  - Explains the learning goal, classes, call flow, Spark comparison, limitations, and thinking questions.

This stage deliberately does not create `Partition`, `Scheduler`, or `Task`. Those concepts start in later stages after the learner understands the simplest API flow.

---

### Task 1: Create Failing Tests For Stage 1

**Files:**
- Create: `tests/test_stage_1_rdd_basics.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_stage_1_rdd_basics.py` with:

```python
from mini_spark import RDD, SparkContext


def test_parallelize_returns_rdd():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3])

    assert isinstance(rdd, RDD)


def test_collect_returns_original_values():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3])

    assert rdd.collect() == [1, 2, 3]


def test_rdd_is_immutable_from_input_list_changes():
    sc = SparkContext()
    source = [1, 2, 3]

    rdd = sc.parallelize(source)
    source.append(4)

    assert rdd.collect() == [1, 2, 3]


def test_collect_returns_a_copy():
    sc = SparkContext()
    rdd = sc.parallelize([1, 2, 3])

    collected = rdd.collect()
    collected.append(4)

    assert rdd.collect() == [1, 2, 3]
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
python -m pytest tests/test_stage_1_rdd_basics.py -q
```

Expected:

```text
ModuleNotFoundError: No module named 'mini_spark'
```

- [ ] **Step 3: Do not implement in this task**

This task only establishes the desired public behavior.

---

### Task 2: Implement Minimal Package And Public API

**Files:**
- Create: `mini_spark/__init__.py`
- Create: `mini_spark/context.py`
- Create: `mini_spark/rdd.py`
- Test: `tests/test_stage_1_rdd_basics.py`

- [ ] **Step 1: Create package export**

Create `mini_spark/__init__.py` with:

```python
from mini_spark.context import SparkContext
from mini_spark.rdd import RDD

__all__ = ["RDD", "SparkContext"]
```

- [ ] **Step 2: Create `SparkContext`**

Create `mini_spark/context.py` with:

```python
from collections.abc import Iterable
from typing import TypeVar

from mini_spark.rdd import RDD

T = TypeVar("T")


class SparkContext:
    """Entry point for creating Mini Spark RDDs."""

    def parallelize(self, data: Iterable[T]) -> RDD[T]:
        return RDD(data)
```

- [ ] **Step 3: Create `RDD`**

Create `mini_spark/rdd.py` with:

```python
from collections.abc import Iterable
from typing import Generic, TypeVar

T = TypeVar("T")


class RDD(Generic[T]):
    """A tiny immutable local RDD for learning Spark's execution model."""

    def __init__(self, data: Iterable[T]) -> None:
        self._data = tuple(data)

    def collect(self) -> list[T]:
        return list(self._data)
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```powershell
python -m pytest tests/test_stage_1_rdd_basics.py -q
```

Expected:

```text
4 passed
```

- [ ] **Step 5: Commit when git is available**

The current directory is not a git repository. Do not run `git commit` now. If the project is later initialized as a git repo, run:

```powershell
git add mini_spark tests/test_stage_1_rdd_basics.py
git commit -m "feat: add stage 1 rdd basics"
```

---

### Task 3: Add Stage 1 Learning Note

**Files:**
- Create: `docs/01-rdd-basics.md`

- [ ] **Step 1: Create the learning note**

Create `docs/01-rdd-basics.md` with:

```markdown
# 01 - RDD 基础

## 本次目标

本阶段实现 Mini Spark 的最小可运行版本：

- `SparkContext`
- `RDD`
- `parallelize`
- `collect`

目标不是实现分布式计算，而是先建立 Spark 最核心的调用感觉：

```python
from mini_spark import SparkContext

sc = SparkContext()
rdd = sc.parallelize([1, 2, 3])
print(rdd.collect())
```

输出：

```text
[1, 2, 3]
```

## 新增类

### SparkContext

`SparkContext` 是用户进入 Mini Spark 的入口。

当前只提供一个方法：

- `parallelize(data)`：把本地 Python 可迭代对象包装成一个 `RDD`。

在真实 Spark 中，`SparkContext` 负责连接集群、申请资源、创建 RDD、提交 Job 等。当前版本只保留“创建 RDD”这个最小能力。

### RDD

`RDD` 是弹性分布式数据集的抽象。

当前版本的 `RDD` 只保存一份本地不可变数据：

- 构造时把输入数据转成 `tuple`，避免外部列表变化影响 RDD。
- `collect()` 返回新的 `list`，避免调用方修改返回值后影响 RDD 内部数据。

真实 Spark 中，RDD 不直接保存所有数据，而是保存分区、依赖关系和计算函数。数据通常分布在多个 Executor 上。

## 调用流程

```text
用户代码
  ↓
SparkContext.parallelize([1, 2, 3])
  ↓
创建 RDD
  ↓
RDD.collect()
  ↓
把 RDD 中的数据返回到 Driver
```

当前版本没有 Scheduler、Task、Executor。它是单进程本地模拟。

## 对应真实 Spark 概念

| Mini Spark | 真实 Spark |
| --- | --- |
| `SparkContext` | `SparkContext` |
| `parallelize` | `SparkContext.parallelize` |
| `RDD` | `RDD` |
| `collect` | `RDD.collect` |
| 本地 `tuple` | 分布式 Partition |

## 为什么 collect 是 Action

`collect()` 会把 RDD 的数据真正取出来并返回给 Driver。

在真实 Spark 中，Transformation 只是描述计算，Action 才会触发 Job 执行。虽然当前版本还没有 Lazy Evaluation，但先把 `collect` 定义成 Action，有助于后续引入 `map`、`filter` 和 Lineage。

## 当前版本的不足

- 没有 Lazy Evaluation。
- 没有 Transformation。
- 没有 Partition。
- 没有 Scheduler。
- 没有 Executor。
- 没有分布式执行。

这些不足不是问题，而是后续阶段要逐步补上的学习点。

## PySpark 实战提醒

真实 PySpark 中，`collect()` 会把所有分区的数据拉回 Driver。

如果数据量很大，`collect()` 可能导致：

- Driver 内存溢出。
- 网络传输压力过大。
- 作业运行很慢。

实战中更常用：

- `take(n)` 查看少量样本。
- `show(n)` 查看 DataFrame 样本。
- 写入外部存储，而不是全部拉回本地。

## 思考题

1. 为什么 `SparkContext` 适合作为创建 RDD 的入口？
2. 为什么当前实现要把输入数据转成 `tuple`？
3. 为什么 `collect()` 返回的是新的 `list`，而不是直接返回内部数据？
4. 真实 Spark 中，`collect()` 为什么可能很危险？
5. 当前 Mini Spark 的 `RDD` 和真实 Spark 的 `RDD` 最大区别是什么？
```

- [ ] **Step 2: Read the note for consistency**

Run:

```powershell
Get-Content -Path docs/01-rdd-basics.md -Encoding UTF8
```

Expected:

```text
The file renders as readable Chinese Markdown and includes 本次目标, 新增类, 调用流程, 对应真实 Spark 概念, 当前版本的不足, PySpark 实战提醒, 思考题.
```

- [ ] **Step 3: Commit when git is available**

The current directory is not a git repository. Do not run `git commit` now. If the project is later initialized as a git repo, run:

```powershell
git add docs/01-rdd-basics.md
git commit -m "docs: explain stage 1 rdd basics"
```

---

### Task 4: Verify First Stage End To End

**Files:**
- Read: `mini_spark/__init__.py`
- Read: `mini_spark/context.py`
- Read: `mini_spark/rdd.py`
- Read: `tests/test_stage_1_rdd_basics.py`
- Read: `docs/01-rdd-basics.md`

- [ ] **Step 1: Run the unit tests**

Run:

```powershell
python -m pytest tests/test_stage_1_rdd_basics.py -q
```

Expected:

```text
4 passed
```

- [ ] **Step 2: Run a manual example**

Run:

```powershell
python -c "from mini_spark import SparkContext; sc = SparkContext(); rdd = sc.parallelize([1, 2, 3]); print(rdd.collect())"
```

Expected:

```text
[1, 2, 3]
```

- [ ] **Step 3: Confirm stage completion criteria**

Confirm:

```text
Code runs: yes
Tests pass: yes
Learning note exists: yes
Real Spark comparison exists: yes
Thinking questions exist: yes
```

- [ ] **Step 4: Commit when git is available**

The current directory is not a git repository. Do not run `git commit` now. If the project is later initialized as a git repo, run:

```powershell
git add mini_spark tests docs/01-rdd-basics.md
git commit -m "feat: complete stage 1 rdd basics"
```
