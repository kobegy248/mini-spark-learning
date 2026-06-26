import pytest

from mini_spark import SparkContext
from mini_spark.rdd import Partition


def test_parallelize_creates_requested_partitions():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3, 4, 5], num_slices=2)

    assert rdd.partitions() == [
        Partition(index=0, data=(1, 2, 3)),
        Partition(index=1, data=(4, 5)),
    ]


def test_collect_preserves_partition_order():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3, 4, 5], num_slices=2)

    assert rdd.collect() == [1, 2, 3, 4, 5]


def test_transformations_run_inside_each_partition():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3, 4], num_slices=2).map(lambda value: value * 10)

    assert rdd.collect_partitions() == [[10, 20], [30, 40]]


def test_filter_can_create_empty_partitions():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3, 4], num_slices=2).filter(lambda value: value > 2)

    assert rdd.collect_partitions() == [[], [3, 4]]
    assert rdd.collect() == [3, 4]


def test_num_partitions_is_preserved_across_narrow_transformations():
    sc = SparkContext()

    rdd = (
        sc.parallelize([1, 2, 3, 4], num_slices=2)
        .map(lambda value: value * 2)
        .filter(lambda value: value > 2)
    )

    assert rdd.num_partitions() == 2


def test_parallelize_rejects_non_positive_num_slices():
    sc = SparkContext()

    with pytest.raises(ValueError, match="num_slices must be positive"):
        sc.parallelize([1, 2, 3], num_slices=0)
