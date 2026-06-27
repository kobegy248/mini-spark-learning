# 第十阶段 Fault Tolerance 实现计划

目标：模拟缓存 Partition 丢失，并通过 RDD Lineage 重新计算丢失的 Partition。

实施内容：

- 新增 `simulate_partition_loss(index)`。
- 新增 `lost_partitions()`。
- 新增 `recover_lost_partitions()`。
- 缓存命中时如果发现丢失 Partition，自动重算并修复缓存。
- 新增测试 `tests/test_stage_10_fault_tolerance.py`。
- 新增中文教程 `docs/10-fault-tolerance.md`。
- 新增 HTML 可视化 `docs/visualizations/10-fault-tolerance.html`。

验证：

```powershell
& '.\.venv\Scripts\python.exe' -m pytest tests -q
```

预期：

```text
49 passed
```
