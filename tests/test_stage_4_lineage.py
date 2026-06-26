from mini_spark import SparkContext
from mini_spark.rdd import Dependency


def test_root_rdd_lineage_contains_parallelize():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3])

    assert rdd.lineage() == ["parallelize"]


def test_transformations_record_parent_lineage():
    sc = SparkContext()

    rdd = (
        sc.parallelize([1, 2, 3, 4])
        .map(lambda value: value * 2)
        .filter(lambda value: value > 4)
        .flat_map(lambda value: [value, value + 1])
    )

    assert rdd.lineage() == ["parallelize", "map", "filter", "flat_map"]
    assert rdd.collect() == [6, 7, 8, 9]


def test_derived_rdd_exposes_narrow_dependency():
    sc = SparkContext()
    source = sc.parallelize([1, 2, 3])

    mapped = source.map(lambda value: value * 10)

    assert mapped.dependencies() == [Dependency(parent=source, kind="narrow")]


def test_debug_string_draws_lineage_tree():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3]).map(lambda value: value * 10).filter(
        lambda value: value > 10
    )

    assert rdd.to_debug_string() == (
        "filter\n"
        "  map\n"
        "    parallelize"
    )


def test_lineage_is_immutable_snapshot():
    sc = SparkContext()
    source = sc.parallelize([1, 2, 3])

    mapped = source.map(lambda value: value + 1)

    assert source.lineage() == ["parallelize"]
    assert mapped.lineage() == ["parallelize", "map"]
