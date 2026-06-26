from mini_spark import SparkContext


def test_group_by_key_groups_values_across_partitions():
    sc = SparkContext()
    rdd = sc.parallelize(
        [("a", 1), ("b", 2), ("a", 3), ("b", 4), ("c", 5)],
        num_slices=2,
    )

    grouped = rdd.group_by_key(num_partitions=2)

    assert grouped.dependency_kind() == "wide"
    assert sorted(grouped.collect()) == [
        ("a", [1, 3]),
        ("b", [2, 4]),
        ("c", [5]),
    ]


def test_reduce_by_key_combines_values_across_partitions():
    sc = SparkContext()
    rdd = sc.parallelize(
        [("a", 1), ("b", 2), ("a", 3), ("b", 4), ("c", 5)],
        num_slices=2,
    )

    reduced = rdd.reduce_by_key(lambda left, right: left + right, num_partitions=2)

    assert reduced.dependency_kind() == "wide"
    assert sorted(reduced.collect()) == [("a", 4), ("b", 6), ("c", 5)]


def test_shuffle_preserves_target_partition_count():
    sc = SparkContext()
    rdd = sc.parallelize([(0, "a"), (1, "b"), (2, "c"), (3, "d")], num_slices=2)

    grouped = rdd.group_by_key(num_partitions=3)

    assert grouped.num_partitions() == 3
    assert len(grouped.collect_partitions()) == 3


def test_reduce_by_key_records_wide_dependency_in_dag():
    sc = SparkContext()
    rdd = sc.parallelize([(1, 1), (1, 2)], num_slices=2).reduce_by_key(
        lambda left, right: left + right,
        num_partitions=2,
    )

    assert rdd.lineage() == ["parallelize", "reduce_by_key"]
    assert rdd.dependencies()[0].kind == "wide"
