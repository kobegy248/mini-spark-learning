# 第三阶段 Action 实现计划

> **给执行代理的要求：** 实施本计划时，应使用 `superpowers:subagent-driven-development`（推荐）或 `superpowers:executing-plans`，按任务逐项执行。步骤使用复选框（`- [ ]`）追踪。

**目标：** 为 Mini Spark 的 `RDD` 新增 `count`、`first`、`take`、`reduce` 四个 Action。

**架构：** Action 只作为现有惰性 `_compute()` 执行链路的薄封装。Transformation 仍然只负责构建 RDD 链；每个 Action 都会触发计算，并把一个具体结果返回到 Driver。

**技术栈：** Python 3.10、pytest、中文 Markdown 文档、纯 HTML 可视化。

---

## 文件结构

- 修改：`mini_spark/rdd.py`
  - 新增 `count()`。
  - 新增 `first()`。
  - 新增 `take(n)`。
  - 新增 `reduce(function)`。
- 新建：`tests/test_stage_3_actions.py`
  - 验证 Action 返回结果。
  - 验证 Action 会触发惰性 Transformation。
  - 验证 `first` 和 `reduce` 面对空 RDD 时的行为。
- 新建：`docs/03-actions.md`
  - 面向小白的中文教程，解释 Action 为什么是执行触发器。
- 新建：`docs/visualizations/03-actions.html`
  - 用“点击下一步”的方式可视化 Action 如何触发 RDD 链执行。

本阶段不引入 Scheduler、Task、Partition、DAG、Stage 或 Shuffle。它只扩展当前本地惰性 RDD 模型上的 Action 能力。

---

### 任务 1：先写失败测试

**文件：**
- 新建：`tests/test_stage_3_actions.py`

- [ ] **步骤 1：写测试**

创建 `tests/test_stage_3_actions.py`：

```python
import pytest

from mini_spark import SparkContext


def test_count_returns_number_of_elements():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3]).map(lambda value: value * 10)

    assert rdd.count() == 3


def test_first_returns_first_element():
    sc = SparkContext()

    rdd = sc.parallelize([10, 20, 30]).filter(lambda value: value > 15)

    assert rdd.first() == 20


def test_first_raises_for_empty_rdd():
    sc = SparkContext()

    rdd = sc.parallelize([])

    with pytest.raises(ValueError, match="first called on empty RDD"):
        rdd.first()


def test_take_returns_at_most_requested_number_of_elements():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3, 4]).map(lambda value: value * 2)

    assert rdd.take(2) == [2, 4]
    assert rdd.take(10) == [2, 4, 6, 8]


def test_take_zero_returns_empty_list():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3])

    assert rdd.take(0) == []


def test_take_rejects_negative_number():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3])

    with pytest.raises(ValueError, match="take count must be non-negative"):
        rdd.take(-1)


def test_reduce_combines_values():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3, 4]).map(lambda value: value * 10)

    assert rdd.reduce(lambda left, right: left + right) == 100


def test_reduce_raises_for_empty_rdd():
    sc = SparkContext()

    rdd = sc.parallelize([])

    with pytest.raises(ValueError, match="reduce called on empty RDD"):
        rdd.reduce(lambda left, right: left + right)


def test_actions_trigger_lazy_transformations_each_time():
    sc = SparkContext()
    calls = []
    rdd = sc.parallelize([1, 2, 3]).map(
        lambda value: calls.append(value) or value * 10
    )

    assert calls == []
    assert rdd.count() == 3
    assert calls == [1, 2, 3]

    assert rdd.take(2) == [10, 20]
    assert calls == [1, 2, 3, 1, 2]
```

- [ ] **步骤 2：运行测试，确认失败**

运行：

```powershell
& '.\.venv\Scripts\python.exe' -m pytest tests/test_stage_3_actions.py -q
```

预期：

```text
AttributeError: 'RDD' object has no attribute 'count'
```

---

### 任务 2：实现 Action 方法

**文件：**
- 修改：`mini_spark/rdd.py`
- 测试：`tests/test_stage_3_actions.py`
- 回归测试：`tests/test_stage_1_rdd_basics.py`
- 回归测试：`tests/test_stage_2_transformations.py`

- [ ] **步骤 1：添加导入**

把 `mini_spark/rdd.py` 中的导入：

```python
from collections.abc import Callable, Iterable
```

改成：

```python
from collections.abc import Callable, Iterable
from itertools import islice
```

- [ ] **步骤 2：在 `collect()` 后添加 Action 方法**

在 `mini_spark/rdd.py` 的 `collect()` 后添加：

```python
    def count(self) -> int:
        return sum(1 for _ in self._compute())

    def first(self) -> T:
        for value in self._compute():
            return value
        raise ValueError("first called on empty RDD")

    def take(self, count: int) -> list[T]:
        if count < 0:
            raise ValueError("take count must be non-negative")
        return list(islice(self._compute(), count))

    def reduce(self, function: Callable[[T, T], T]) -> T:
        iterator = iter(self._compute())
        try:
            result = next(iterator)
        except StopIteration as exc:
            raise ValueError("reduce called on empty RDD") from exc

        for value in iterator:
            result = function(result, value)
        return result
```

- [ ] **步骤 3：运行第三阶段测试**

运行：

```powershell
& '.\.venv\Scripts\python.exe' -m pytest tests/test_stage_3_actions.py -q
```

预期：

```text
9 passed
```

- [ ] **步骤 4：运行第一、二阶段回归测试**

运行：

```powershell
& '.\.venv\Scripts\python.exe' -m pytest tests/test_stage_1_rdd_basics.py tests/test_stage_2_transformations.py -q
```

预期：

```text
10 passed
```

---

### 任务 3：新增第三章中文学习文档

**文件：**
- 新建：`docs/03-actions.md`

- [ ] **步骤 1：创建面向小白的学习文档**

创建 `docs/03-actions.md`。文档必须使用中文，并遵循 `CLAUDE.md` 中的“小白跟学教程”准则。

内容必须覆盖：

- 这一章学什么、不学什么。
- 用人话解释 Action。
- `count`、`first`、`take`、`reduce` 的最小示例。
- 逐行解释：Transformation 如何先记录计划，Action 如何触发 `_compute()`。
- Mini Spark 内部实现：每个 Action 如何调用 `_compute()`。
- 真实 Spark 对照：Action 会触发 Job，后续会产生 Stage 和 Task。
- 亲手实验：证明 `count()`、`take()` 会触发惰性 Transformation。
- 常见误解：不是所有操作都会触发计算，`count()` 也通常需要执行。
- 本章掌握标准。
- 3 到 5 个思考题。

- [ ] **步骤 2：读取文档确认中文可读**

运行：

```powershell
Get-Content -Path docs/03-actions.md -Encoding UTF8
```

预期：

```text
文档是可读中文，并包含：先学什么/不学什么、Action 解释、代码逐行讲解、Mini Spark 内部实现、真实 Spark 对照、亲手实验、常见误解、本章掌握标准、思考题。
```

---

### 任务 4：新增 Action HTML 可视化

**文件：**
- 新建：`docs/visualizations/03-actions.html`
- 修改：`docs/03-actions.md`

- [ ] **步骤 1：创建 HTML 可视化页面**

创建 `docs/visualizations/03-actions.html`。

页面要求：

- 纯 HTML/CSS/JavaScript，不依赖外部网络。
- 页面文案全部使用中文。
- 采用“点击下一步”的交互方式。
- 展示一条 RDD 链：

```text
Root RDD
  ↓ map，不执行
Derived RDD
  ↓ count / take / reduce，触发执行
```

- 必须突出：
  - Transformation 只是记录计划。
  - Action 会触发 `_compute()`。
  - `count()` 返回数量。
  - `take(2)` 返回前两个结果。
  - `reduce()` 把多个值合并成一个值。

- [ ] **步骤 2：在 `docs/03-actions.md` 中添加链接**

在第三章开头附近添加：

```markdown
如果你觉得 Action 触发执行这个过程抽象，可以先打开这个可视化页面：

[Action 触发执行可视化](visualizations/03-actions.html)
```

- [ ] **步骤 3：确认链接和 HTML 标题存在**

运行：

```powershell
Select-String -Path docs/03-actions.md -Pattern "Action 触发执行可视化"
Select-String -Path docs/visualizations/03-actions.html -Pattern "<title>|Action"
```

预期：

```text
能找到文档链接和 HTML 标题。
```

---

### 任务 5：端到端验证

**文件：**
- 读取：`mini_spark/rdd.py`
- 读取：`tests/test_stage_3_actions.py`
- 读取：`docs/03-actions.md`
- 读取：`docs/visualizations/03-actions.html`

- [ ] **步骤 1：运行全部测试**

运行：

```powershell
& '.\.venv\Scripts\python.exe' -m pytest tests -q
```

预期：

```text
19 passed
```

- [ ] **步骤 2：运行手动 Action 示例**

运行：

```powershell
& '.\.venv\Scripts\python.exe' -c "from mini_spark import SparkContext; sc = SparkContext(); rdd = sc.parallelize([1, 2, 3, 4]).map(lambda x: x * 10); print(rdd.count()); print(rdd.first()); print(rdd.take(2)); print(rdd.reduce(lambda a, b: a + b))"
```

预期：

```text
4
10
[10, 20]
100
```

- [ ] **步骤 3：确认完成标准**

确认：

```text
代码可运行：是
测试通过：是
第一、二阶段回归通过：是
中文学习文档存在：是
HTML 可视化存在：是
真实 Spark 对照存在：是
思考题存在：是
```

---

### 任务 6：提交并推送第三阶段

**文件：**
- 修改：`mini_spark/rdd.py`
- 新建：`tests/test_stage_3_actions.py`
- 新建：`docs/03-actions.md`
- 新建：`docs/visualizations/03-actions.html`
- 新建：`docs/superpowers/plans/2026-06-27-stage-3-actions.md`
- 修改：`CLAUDE.md`

- [ ] **步骤 1：查看 git 状态**

运行：

```powershell
git status --short
```

预期：

```text
 M CLAUDE.md
 M mini_spark/rdd.py
?? docs/03-actions.md
?? docs/visualizations/03-actions.html
?? docs/superpowers/plans/2026-06-27-stage-3-actions.md
?? tests/test_stage_3_actions.py
```

本地 `.claude/` 可能仍然显示为未跟踪目录。不要提交它。

- [ ] **步骤 2：提交变更**

运行：

```powershell
git add CLAUDE.md mini_spark/rdd.py tests/test_stage_3_actions.py docs/03-actions.md docs/visualizations/03-actions.html docs/superpowers/plans/2026-06-27-stage-3-actions.md
git commit -m "feat: add rdd actions"
```

预期：

```text
[main <hash>] feat: add rdd actions
```

- [ ] **步骤 3：推送变更**

运行：

```powershell
git push
```

预期：

```text
main -> main
```
