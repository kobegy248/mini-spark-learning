from mini_spark import SparkContext


def test_cache_reuses_computed_partitions():
    sc = SparkContext()
    calls = []
    rdd = sc.parallelize([1, 2, 3], num_slices=2).map(
        lambda value: calls.append(value) or value * 10
    )

    rdd.cache()

    assert rdd.collect() == [10, 20, 30]
    assert calls == [1, 2, 3]

    assert rdd.collect() == [10, 20, 30]
    assert calls == [1, 2, 3]


def test_uncached_rdd_recomputes_each_action():
    sc = SparkContext()
    calls = []
    rdd = sc.parallelize([1, 2, 3]).map(
        lambda value: calls.append(value) or value * 10
    )

    assert rdd.collect() == [10, 20, 30]
    assert rdd.collect() == [10, 20, 30]
    assert calls == [1, 2, 3, 1, 2, 3]


def test_persist_is_alias_for_cache():
    sc = SparkContext()
    rdd = sc.parallelize([1, 2, 3])

    assert rdd.persist() is rdd
    assert rdd.is_cached()


def test_cache_info_reports_hits_and_misses():
    sc = SparkContext()
    rdd = sc.parallelize([1, 2, 3]).map(lambda value: value * 10).cache()

    assert rdd.cache_info() == {"enabled": True, "hits": 0, "misses": 0}

    rdd.collect()
    assert rdd.cache_info() == {"enabled": True, "hits": 0, "misses": 1}

    rdd.collect()
    assert rdd.cache_info() == {"enabled": True, "hits": 1, "misses": 1}
