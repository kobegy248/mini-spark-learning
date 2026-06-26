# 第八阶段 Shuffle 实现计划

目标：新增 Key-Value RDD 的 `group_by_key` 和 `reduce_by_key`，引入宽依赖和简化版 Shuffle。

实施内容：

- RDD 支持 `wide_transform`。
- 新增 `group_by_key(num_partitions=None)`。
- 新增 `reduce_by_key(function, num_partitions=None)`。
- 宽依赖 RDD 的 `dependency_kind()` 返回 `wide`。
- DAG 可基于 `wide` 依赖切分 Stage。
- 新增测试 `tests/test_stage_8_shuffle.py`。
- 新增中文教程 `docs/08-shuffle.md`。
- 新增 HTML 可视化 `docs/visualizations/08-shuffle.html`。

验证：

```powershell
& '.\.venv\Scripts\python.exe' -m pytest tests -q
```

预期：

```text
42 passed
```
