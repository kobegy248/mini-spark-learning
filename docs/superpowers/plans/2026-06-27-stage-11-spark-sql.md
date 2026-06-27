# 第十一阶段 Spark SQL 实现计划

目标：实现极简 Spark SQL 链路：SQL 字符串 -> Logical Plan -> Physical Plan -> RDD 执行。

实施内容：

- 新建 `mini_spark/sql.py`
  - `MiniSparkSession`
  - `TableScan`
  - `Filter`
  - `Project`
  - `PhysicalPlan`
  - `QueryResult`
- 支持 `select ... from ... where ...` 的最小 SQL 子集。
- `QueryResult.collect()` 映射到 RDD 执行。
- `QueryResult.explain()` 输出 Logical Plan 和 Physical Plan。
- 新增测试 `tests/test_stage_11_sql.py`。
- 新增中文教程 `docs/11-spark-sql.md`。
- 新增 HTML 可视化 `docs/visualizations/11-spark-sql.html`。

验证：

```powershell
& '.\.venv\Scripts\python.exe' -m pytest tests -q
```

预期：

```text
54 passed
```
