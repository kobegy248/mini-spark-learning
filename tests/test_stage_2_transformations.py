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
