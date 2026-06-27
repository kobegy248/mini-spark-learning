from mini_spark import SparkContext


def test_cached_lost_partition_is_recomputed_from_lineage():
    sc = SparkContext()
    calls = []
    rdd = sc.parallelize([1, 2, 3, 4], num_slices=2).map(
        lambda value: calls.append(value) or value * 10
    )
    rdd.cache()

    assert rdd.collect() == [10, 20, 30, 40]
    assert calls == [1, 2, 3, 4]

    rdd.simulate_partition_loss(1)
    assert rdd.lost_partitions() == [1]

    assert rdd.collect() == [10, 20, 30, 40]
    assert calls == [1, 2, 3, 4, 3, 4]
    assert rdd.lost_partitions() == []


def test_recover_lost_partitions_can_be_called_explicitly():
    sc = SparkContext()
    calls = []
    rdd = sc.parallelize([1, 2, 3, 4], num_slices=2).map(
        lambda value: calls.append(value) or value * 10
    ).cache()

    rdd.collect()
    rdd.simulate_partition_loss(0)
    rdd.recover_lost_partitions()

    assert calls == [1, 2, 3, 4, 1, 2]
    assert rdd.lost_partitions() == []
    assert rdd.collect() == [10, 20, 30, 40]
    assert calls == [1, 2, 3, 4, 1, 2]


def test_simulate_partition_loss_rejects_invalid_partition_index():
    sc = SparkContext()
    rdd = sc.parallelize([1, 2, 3], num_slices=2).cache()
    rdd.collect()

    try:
        rdd.simulate_partition_loss(2)
    except IndexError as exc:
        assert str(exc) == "partition index out of range"
    else:
        raise AssertionError("expected IndexError")
