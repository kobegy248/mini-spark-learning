# 第四阶段 Lineage 实现计划

目标：为 RDD 增加父子依赖记录、窄依赖对象、`lineage()` 和 `to_debug_string()`，让学习者能看到 RDD 从哪里来。

实施内容：

- `mini_spark/rdd.py`
  - 新增 `Dependency`。
  - RDD 记录 `_operation` 和 `_dependency_kind`。
  - Transformation 创建派生 RDD 时记录操作名。
  - 新增 `dependencies()`。
  - 新增 `lineage()`。
  - 新增 `to_debug_string()`。
- `tests/test_stage_4_lineage.py`
  - 验证 Root RDD 的 lineage。
  - 验证 Transformation 的父子链。
  - 验证窄依赖。
  - 验证 debug string。
- `docs/04-lineage.md`
  - 中文小白教程，解释 Lineage 是 RDD 的来历记录。
- `docs/visualizations/04-lineage.html`
  - 点击下一步展示 Root RDD、map RDD、filter RDD 的 parent 链。

验证：

```powershell
& '.\.venv\Scripts\python.exe' -m pytest tests -q
```

预期：

```text
24 passed
```
