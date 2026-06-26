# 第九阶段 Cache 实现计划

目标：新增 `cache` / `persist`，让 RDD 可以复用第一次 Action 计算出的 Partition 结果。

实施内容：

- RDD 新增缓存开关、缓存数据、命中/未命中计数。
- 新增 `cache()`、`persist()`、`is_cached()`、`cache_info()`。
- `_compute_partitions()` 在缓存开启时支持 miss 后写入、hit 后复用。
- 新增测试 `tests/test_stage_9_cache.py`。
- 新增中文教程 `docs/09-cache.md`。
- 新增 HTML 可视化 `docs/visualizations/09-cache.html`。

验证：

```powershell
& '.\.venv\Scripts\python.exe' -m pytest tests -q
```

预期：

```text
46 passed
```
