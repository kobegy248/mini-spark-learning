# 第五阶段 Partition 实现计划

目标：让 RDD 支持多个 Partition，并让窄依赖 Transformation 在每个 Partition 内部执行。

实施内容：

- `SparkContext.parallelize(data, num_slices=1)` 支持指定分区数量。
- 新增 `Partition(index, data)`。
- Root RDD 保存多个 Partition。
- Derived RDD 对父 RDD 的每个 Partition 分别应用 transform。
- 新增 `partitions()`、`num_partitions()`、`collect_partitions()`。
- 新增中文教程 `docs/05-partitions.md`。
- 新增 HTML 可视化 `docs/visualizations/05-partitions.html`。

验证：

```powershell
& '.\.venv\Scripts\python.exe' -m pytest tests -q
```

预期：

```text
30 passed
```
