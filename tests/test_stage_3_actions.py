import pytest

from mini_spark import SparkContext


def test_count_returns_number_of_elements():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3]).map(lambda value: value * 10)

    assert rdd.count() == 3


def test_first_returns_first_element():
    sc = SparkContext()

    rdd = sc.parallelize([10, 20, 30]).filter(lambda value: value > 15)

    assert rdd.first() == 20


def test_first_raises_for_empty_rdd():
    sc = SparkContext()

    rdd = sc.parallelize([])

    with pytest.raises(ValueError, match="first called on empty RDD"):
        rdd.first()


def test_take_returns_at_most_requested_number_of_elements():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3, 4]).map(lambda value: value * 2)

    assert rdd.take(2) == [2, 4]
    assert rdd.take(10) == [2, 4, 6, 8]


def test_take_zero_returns_empty_list():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3])

    assert rdd.take(0) == []


def test_take_rejects_negative_number():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3])

    with pytest.raises(ValueError, match="take count must be non-negative"):
        rdd.take(-1)


def test_reduce_combines_values():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3, 4]).map(lambda value: value * 10)

    assert rdd.reduce(lambda left, right: left + right) == 100


def test_reduce_raises_for_empty_rdd():
    sc = SparkContext()

    rdd = sc.parallelize([])

    with pytest.raises(ValueError, match="reduce called on empty RDD"):
        rdd.reduce(lambda left, right: left + right)


def test_actions_trigger_lazy_transformations_each_time():
    sc = SparkContext()
    calls = []
    rdd = sc.parallelize([1, 2, 3]).map(
        lambda value: calls.append(value) or value * 10
    )

    assert calls == []
    assert rdd.count() == 3
    assert calls == [1, 2, 3]

    assert rdd.take(2) == [10, 20]
    assert calls == [1, 2, 3, 1, 2]
