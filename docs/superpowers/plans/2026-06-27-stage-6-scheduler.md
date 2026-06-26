# 第六阶段 Scheduler 实现计划

目标：新增本地 Scheduler、Task 和 Executor，让 Action 能记录“每个 Partition 对应一个 Task”的执行过程。

实施内容：

- 新建 `mini_spark/scheduler.py`
  - `Task`
  - `TaskResult`
  - `LocalExecutor`
  - `LocalScheduler`
- `RDD.collect()` 和 `RDD.count()` 通过 `LocalScheduler.run_job()` 执行。
- 新增 `last_job_tasks()`，便于学习和测试观察。
- 新增测试 `tests/test_stage_6_scheduler.py`。
- 新增中文教程 `docs/06-scheduler.md`。
- 新增 HTML 可视化 `docs/visualizations/06-scheduler.html`。

验证：

```powershell
& '.\.venv\Scripts\python.exe' -m pytest tests -q
```

预期：

```text
34 passed
```
