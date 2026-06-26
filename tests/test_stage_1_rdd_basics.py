from mini_spark import RDD, SparkContext


def test_parallelize_returns_rdd():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3])

    assert isinstance(rdd, RDD)


def test_collect_returns_original_values():
    sc = SparkContext()

    rdd = sc.parallelize([1, 2, 3])

    assert rdd.collect() == [1, 2, 3]


def test_rdd_is_immutable_from_input_list_changes():
    sc = SparkContext()
    source = [1, 2, 3]

    rdd = sc.parallelize(source)
    source.append(4)

    assert rdd.collect() == [1, 2, 3]


def test_collect_returns_a_copy():
    sc = SparkContext()
    rdd = sc.parallelize([1, 2, 3])

    collected = rdd.collect()
    collected.append(4)

    assert rdd.collect() == [1, 2, 3]
