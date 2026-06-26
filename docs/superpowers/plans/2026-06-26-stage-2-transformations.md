# Stage 2 Transformations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add lazy `map`, `filter`, and `flat_map` transformations to Mini Spark RDDs.

**Architecture:** `RDD` will support two internal forms: a root RDD that stores local source data, and a derived RDD that stores a parent RDD plus a transformation function. Transformations return new RDD instances without executing; `collect()` recursively computes the parent and then applies the current transformation.

**Tech Stack:** Python 3.10, pytest, Markdown docs.

---

## File Structure

- Modify: `mini_spark/rdd.py`
  - Add lazy derived RDD support.
  - Add `map`, `filter`, and `flat_map`.
  - Keep `collect()` as the first action and execution trigger.
- Create: `tests/test_stage_2_transformations.py`
  - Verify transformation results.
  - Verify transformations are lazy.
  - Verify transformations return new RDDs without mutating the parent.
- Create: `docs/02-transformations.md`
  - Explain Lazy Evaluation, Transformation, why a new RDD is returned, and PySpark implications.

This stage does not add partitions, scheduler, lineage visualization, or web UI. It introduces the conceptual foundation needed for lineage in Stage 4.

---

### Task 1: Create Failing Transformation Tests

**Files:**
- Create: `tests/test_stage_2_transformations.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_stage_2_transformations.py` with:

```python
from mini_spark import RDD, SparkContext


def test_map_transforms_values_when_collected():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3]).map(lambda value: value * 10)

    assert rdd.collect() == [10, 20, 30]


def test_filter_keeps_matching_values_when_collected():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3, 4]).filter(lambda value: value % 2 == 0)

    assert rdd.collect() == [2, 4]


def test_flat_map_expands_values_when_collected():
    sc = SparkContext()

    rdd = sc.parallelize(["ab", "cd"]).flat_map(lambda text: list(text))

    assert rdd.collect() == ["a", "b", "c", "d"]


def test_transformations_can_be_chained():
    sc = SparkContext()

    rdd = (
        sc.parallelize([1, 2, 3, 4])
        .map(lambda value: value * 2)
        .filter(lambda value: value > 4)
        .flat_map(lambda value: [value, value + 1])
    )

    assert rdd.collect() == [6, 7, 8, 9]


def test_transformations_return_new_rdds_without_changing_parent():
    sc = SparkContext()
    source = sc.parallelize([1, 2, 3])

    mapped = source.map(lambda value: value + 1)

    assert isinstance(mapped, RDD)
    assert mapped is not source
    assert source.collect() == [1, 2, 3]
    assert mapped.collect() == [2, 3, 4]


def test_map_is_lazy_until_action_runs():
    sc = SparkContext()
    calls = []

    rdd = sc.parallelize([1, 2, 3]).map(
        lambda value: calls.append(value) or value * 10
    )

    assert calls == []
    assert rdd.collect() == [10, 20, 30]
    assert calls == [1, 2, 3]
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
& '.\.venv\Scripts\python.exe' -m pytest tests/test_stage_2_transformations.py -q
```

Expected:

```text
AttributeError: 'RDD' object has no attribute 'map'
```

---

### Task 2: Implement Lazy Transformations

**Files:**
- Modify: `mini_spark/rdd.py`
- Test: `tests/test_stage_2_transformations.py`
- Test: `tests/test_stage_1_rdd_basics.py`

- [ ] **Step 1: Replace `mini_spark/rdd.py` with lazy RDD support**

Replace `mini_spark/rdd.py` with:

```python
from collections.abc import Callable, Iterable
from typing import Generic, TypeVar

T = TypeVar("T")
U = TypeVar("U")


class RDD(Generic[T]):
    """A tiny immutable local RDD for learning Spark's execution model."""

    def __init__(
        self,
        data: Iterable[T] | None = None,
        parent: "RDD[object] | None" = None,
        transform: Callable[[Iterable[object]], Iterable[T]] | None = None,
    ) -> None:
        if data is None and parent is None:
            raise ValueError("RDD needs either source data or a parent RDD")
        if data is not None and parent is not None:
            raise ValueError("RDD cannot have both source data and a parent RDD")
        if parent is None and transform is not None:
            raise ValueError("Root RDD cannot have a transform")
        if parent is not None and transform is None:
            raise ValueError("Derived RDD needs a transform")

        self._data = tuple(data) if data is not None else None
        self._parent = parent
        self._transform = transform

    def map(self, function: Callable[[T], U]) -> "RDD[U]":
        return RDD(
            parent=self,
            transform=lambda values: (function(value) for value in values),
        )

    def filter(self, function: Callable[[T], bool]) -> "RDD[T]":
        return RDD(
            parent=self,
            transform=lambda values: (value for value in values if function(value)),
        )

    def flat_map(self, function: Callable[[T], Iterable[U]]) -> "RDD[U]":
        return RDD(
            parent=self,
            transform=lambda values: (
                item for value in values for item in function(value)
            ),
        )

    def collect(self) -> list[T]:
        return list(self._compute())

    def _compute(self) -> Iterable[T]:
        if self._parent is None:
            if self._data is None:
                raise RuntimeError("Root RDD has no source data")
            return self._data

        if self._transform is None:
            raise RuntimeError("Derived RDD has no transform")

        return self._transform(self._parent._compute())
```

- [ ] **Step 2: Run Stage 2 tests**

Run:

```powershell
& '.\.venv\Scripts\python.exe' -m pytest tests/test_stage_2_transformations.py -q
```

Expected:

```text
6 passed
```

- [ ] **Step 3: Run Stage 1 regression tests**

Run:

```powershell
& '.\.venv\Scripts\python.exe' -m pytest tests/test_stage_1_rdd_basics.py -q
```

Expected:

```text
4 passed
```

---

### Task 3: Add Stage 2 Learning Note

**Files:**
- Create: `docs/02-transformations.md`

- [ ] **Step 1: Create the learning note**

Create `docs/02-transformations.md` with:

```markdown
# 02 - Transformation 与 Lazy Evaluation

## 本次目标

本阶段新增三个 Transformation：

- `map`
- `filter`
- `flat_map`

它们都不会立即执行计算，而是返回一个新的 `RDD`，把“未来要怎么计算”记录下来。真正执行发生在 Action，比如 `collect()`。

## 新增能力

### map

`map(function)` 对 RDD 中的每个元素应用函数，并返回新的 RDD。

```python
rdd = sc.parallelize([1, 2, 3])
mapped = rdd.map(lambda value: value * 10)
print(mapped.collect())
```

输出：

```text
[10, 20, 30]
```

### filter

`filter(function)` 保留满足条件的元素，并返回新的 RDD。

```python
rdd = sc.parallelize([1, 2, 3, 4])
even = rdd.filter(lambda value: value % 2 == 0)
print(even.collect())
```

输出：

```text
[2, 4]
```

### flat_map

`flat_map(function)` 先把一个元素变成多个元素，再把结果拍平。

```python
rdd = sc.parallelize(["ab", "cd"])
chars = rdd.flat_map(lambda text: list(text))
print(chars.collect())
```

输出：

```text
['a', 'b', 'c', 'd']
```

## 调用流程

```text
用户代码
  ↓
SparkContext.parallelize([1, 2, 3])
  ↓
Root RDD
  ↓
.map(lambda value: value * 10)
  ↓
Derived RDD，只记录 parent + transform，不执行
  ↓
.collect()
  ↓
递归计算 parent
  ↓
执行 transform
  ↓
结果返回 Driver
```

## 为什么 Transformation 不立即执行

如果 `map`、`filter` 一调用就执行，Spark 就无法看到完整计算链路。

Lazy Evaluation 的好处是：

- 可以把多个步骤合成一个执行计划。
- 可以等到 Action 出现时再决定如何调度。
- 可以避免不必要的计算。
- 可以基于依赖关系构建 Lineage。

当前 Mini Spark 还没有调度优化，但已经保留了这个核心思想：Transformation 只描述计算，Action 才触发计算。

## 为什么返回新的 RDD

RDD 是不可变的。

`map` 不会修改原来的 RDD，而是返回新的 RDD：

```python
source = sc.parallelize([1, 2, 3])
mapped = source.map(lambda value: value + 1)

print(source.collect())  # [1, 2, 3]
print(mapped.collect())  # [2, 3, 4]
```

这样做的好处是：

- 原始数据不会被意外修改。
- Lineage 可以清楚表达父子关系。
- 后续容错时可以从父 RDD 重新计算。

## 对应真实 Spark 概念

| Mini Spark | 真实 Spark |
| --- | --- |
| `RDD.map` | `RDD.map` |
| `RDD.filter` | `RDD.filter` |
| `RDD.flat_map` | PySpark `RDD.flatMap` |
| parent RDD | RDD dependency |
| transform 函数 | 每个分区上的计算函数 |
| `collect()` 触发 `_compute()` | Action 触发 Job |

## 当前版本的不足

- 还没有 Partition。
- 还没有每个分区独立执行。
- 还没有 Scheduler。
- 还没有 DAG 和 Stage。
- 每次 `collect()` 都会重新计算，没有 Cache。

这些不足会在后续阶段逐步补上。

## PySpark 实战提醒

PySpark 中 Transformation 也是 Lazy 的。下面这段代码不会立刻执行：

```python
rdd2 = rdd.map(lambda value: value * 10)
```

只有遇到 Action 才会触发执行：

```python
rdd2.collect()
```

这解释了一个常见现象：你写了很多 `map`、`filter`，Spark UI 里却暂时看不到 Job。因为 Job 还没有被 Action 触发。

## 思考题

1. 为什么 `map` 不应该直接修改原来的 RDD？
2. 为什么 Transformation 不立即执行有利于 Spark 优化？
3. `map` 和 `flat_map` 的核心区别是什么？
4. 如果同一个 RDD 调用两次 `collect()`，当前 Mini Spark 会发生什么？
5. Lazy Evaluation 和 Lineage 之间有什么关系？
```

- [ ] **Step 2: Read the note for consistency**

Run:

```powershell
Get-Content -Path docs/02-transformations.md -Encoding UTF8
```

Expected:

```text
The file renders as readable Chinese Markdown and includes 本次目标, 新增能力, 调用流程, Lazy Evaluation, Spark 对照, PySpark 实战提醒, 思考题.
```

---

### Task 4: Verify Stage 2 End To End

**Files:**
- Read: `mini_spark/rdd.py`
- Read: `tests/test_stage_1_rdd_basics.py`
- Read: `tests/test_stage_2_transformations.py`
- Read: `docs/02-transformations.md`

- [ ] **Step 1: Run all tests**

Run:

```powershell
& '.\.venv\Scripts\python.exe' -m pytest tests -q
```

Expected:

```text
10 passed
```

- [ ] **Step 2: Run a manual chained example**

Run:

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc = SparkContext(); rdd = sc.parallelize([1, 2, 3, 4]).map(lambda x: x * 2).filter(lambda x: x > 4).flat_map(lambda x: [x, x + 1]); print(rdd.collect())"
```

Expected:

```text
[6, 7, 8, 9]
```

- [ ] **Step 3: Confirm completion criteria**

Confirm:

```text
Code runs: yes
Tests pass: yes
Stage 1 regression passes: yes
Learning note exists: yes
Real Spark comparison exists: yes
Thinking questions exist: yes
```

---

### Task 5: Commit Stage 2

**Files:**
- Modify: `mini_spark/rdd.py`
- Create: `tests/test_stage_2_transformations.py`
- Create: `docs/02-transformations.md`
- Create: `docs/superpowers/plans/2026-06-26-stage-2-transformations.md`

- [ ] **Step 1: Inspect git status**

Run:

```powershell
git status --short
```

Expected:

```text
 M mini_spark/rdd.py
?? docs/02-transformations.md
?? docs/superpowers/plans/2026-06-26-stage-2-transformations.md
?? tests/test_stage_2_transformations.py
```

- [ ] **Step 2: Commit changes**

Run:

```powershell
git add mini_spark/rdd.py tests/test_stage_2_transformations.py docs/02-transformations.md docs/superpowers/plans/2026-06-26-stage-2-transformations.md
git commit -m "feat: add lazy rdd transformations"
```

Expected:

```text
[main <hash>] feat: add lazy rdd transformations
```

- [ ] **Step 3: Push changes**

Run:

```powershell
git push
```

Expected:

```text
main -> main
```
